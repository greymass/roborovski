package internal

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type StreamedAction struct {
	GlobalSeq  uint64
	BlockNum   uint32
	BlockTime  uint32
	Contract   uint64
	Action     uint64
	Receiver   uint64
	ActionData []byte
}

type ActionBroadcaster struct {
	subs   map[uint64]*Subscription
	mu     sync.RWMutex
	nextID atomic.Uint64

	liveMode atomic.Bool
	headSeq  atomic.Uint64
	libSeq   atomic.Uint64

	closeChan chan struct{}
	closed    atomic.Bool
}

func NewActionBroadcaster() *ActionBroadcaster {
	return &ActionBroadcaster{
		subs:      make(map[uint64]*Subscription),
		closeChan: make(chan struct{}),
	}
}

func (b *ActionBroadcaster) Subscribe(filter ActionFilter) *Subscription {
	sub := &Subscription{
		id:        b.nextID.Add(1),
		filter:    filter,
		sendCh:    make(chan StreamedAction, 1000),
		errorCh:   make(chan StreamError, 10),
		createdAt: time.Now(),
	}

	b.mu.Lock()
	b.subs[sub.id] = sub
	subCount := len(b.subs)
	b.mu.Unlock()

	logger.Printf("stream", "Subscription %d created (%d total)", sub.id, subCount)
	return sub
}

func (b *ActionBroadcaster) Unsubscribe(id uint64) {
	b.mu.Lock()
	sub, ok := b.subs[id]
	if ok {
		delete(b.subs, id)
		close(sub.sendCh)
		close(sub.errorCh)
	}
	subCount := len(b.subs)
	b.mu.Unlock()

	if ok {
		logger.Printf("stream", "Subscription %d removed (%d remaining)", id, subCount)
	}
}

func (b *ActionBroadcaster) Broadcast(action StreamedAction) bool {
	if b.closed.Load() || !b.liveMode.Load() {
		return false
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	var delivered bool
	for _, sub := range b.subs {
		if sub.IsCatchingUp() {
			continue
		}
		if !sub.filter.Matches(action) {
			logger.Printf("debug-stream", "Subscription %d filter rejected: contract=%x receiver=%x action=%x (filter: contracts=%d receivers=%d actions=%d)",
				sub.id, action.Contract, action.Receiver, action.Action,
				len(sub.filter.Contracts), len(sub.filter.Receivers), len(sub.filter.Actions))
			continue
		}
		if !sub.canSend() {
			blocked := sub.blockedCount.Add(1)
			now := time.Now().Unix()
			lastLog := sub.lastBlockedLog.Load()
			if blocked == 1 || (now-lastLog >= 5 && sub.lastBlockedLog.CompareAndSwap(lastLog, now)) {
				logger.Printf("stream", "Subscription %d backpressure: blocked %d actions (sent=%d, acked=%d, seq=%d)",
					sub.id, blocked, sub.sendCount.Load(), sub.ackCount.Load(), action.GlobalSeq)
			}
			continue
		}
		select {
		case sub.sendCh <- action:
			sub.sendCount.Add(1)
			sub.lastSentSeq.Store(action.GlobalSeq)
			delivered = true
		default:
			logger.Printf("stream", "Subscription %d buffer full, dropping action %d", sub.id, action.GlobalSeq)
		}
	}
	return delivered
}

func (b *ActionBroadcaster) BroadcastError(code uint16, message string) {
	if b.closed.Load() {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	streamErr := StreamError{Code: code, Message: message}
	for _, sub := range b.subs {
		select {
		case sub.errorCh <- streamErr:
		default:
			logger.Printf("stream", "Subscription %d error channel full", sub.id)
		}
	}

	logger.Printf("stream", "Broadcast error to %d subscribers: [%d] %s", len(b.subs), code, message)
}

func (b *ActionBroadcaster) SetLiveMode(live bool) {
	wasLive := b.liveMode.Swap(live)
	if live && !wasLive {
		logger.Printf("stream", "Live streaming enabled")
	} else if !live && wasLive {
		logger.Printf("stream", "Live streaming disabled (syncing)")
	}
}

func (b *ActionBroadcaster) IsLiveMode() bool {
	return b.liveMode.Load()
}

func (b *ActionBroadcaster) SetState(headSeq, libSeq uint64) {
	b.headSeq.Store(headSeq)
	b.libSeq.Store(libSeq)
}

func (b *ActionBroadcaster) GetState() (headSeq, libSeq uint64) {
	return b.headSeq.Load(), b.libSeq.Load()
}

func (b *ActionBroadcaster) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

func (b *ActionBroadcaster) CouldMatch(contract, action, receiver uint64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sub := range b.subs {
		f := &sub.filter

		if len(f.Contracts) == 0 && len(f.Receivers) == 0 {
			continue
		}

		contractMatch := len(f.Contracts) == 0
		if !contractMatch {
			_, contractMatch = f.Contracts[contract]
		}

		actionMatch := len(f.Actions) == 0
		if !actionMatch {
			_, actionMatch = f.Actions[action]
		}

		if !actionMatch {
			continue
		}

		if len(f.Receivers) == 0 {
			if contractMatch && receiver == contract {
				return true
			}
			continue
		}

		_, receiverMatch := f.Receivers[receiver]
		if len(f.Contracts) == 0 {
			if receiverMatch {
				return true
			}
			continue
		}

		if contractMatch && receiverMatch {
			return true
		}
	}
	return false
}

func (b *ActionBroadcaster) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	close(b.closeChan)

	b.mu.Lock()
	for _, sub := range b.subs {
		close(sub.sendCh)
		close(sub.errorCh)
	}
	b.subs = make(map[uint64]*Subscription)
	b.mu.Unlock()

	logger.Printf("stream", "Action broadcaster closed")
}
