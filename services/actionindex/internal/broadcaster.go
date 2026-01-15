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
	}
	subCount := len(b.subs)
	b.mu.Unlock()

	if ok {
		logger.Printf("stream", "Subscription %d removed (%d remaining)", id, subCount)
	}
}

func (b *ActionBroadcaster) Broadcast(action StreamedAction) {
	if b.closed.Load() || !b.liveMode.Load() {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sub := range b.subs {
		if sub.filter.Matches(action) && sub.canSend() {
			select {
			case sub.sendCh <- action:
				sub.lastSent.Store(action.GlobalSeq)
			default:
				logger.Printf("stream", "Subscription %d buffer full, dropping action %d", sub.id, action.GlobalSeq)
			}
		}
	}
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

func (b *ActionBroadcaster) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	close(b.closeChan)

	b.mu.Lock()
	for _, sub := range b.subs {
		close(sub.sendCh)
	}
	b.subs = make(map[uint64]*Subscription)
	b.mu.Unlock()

	logger.Printf("stream", "Action broadcaster closed")
}
