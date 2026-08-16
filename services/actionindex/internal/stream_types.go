package internal

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type ActionFilter struct {
	Contracts map[uint64]struct{}
	Receivers map[uint64]struct{}
	Actions   map[uint64]struct{}
}

func (f *ActionFilter) Matches(action StreamedAction, matchedVia []uint64) bool {
	if len(f.Contracts) == 0 && len(f.Receivers) == 0 {
		return false
	}

	contractMatch := len(f.Contracts) == 0
	if !contractMatch {
		_, contractMatch = f.Contracts[action.Contract]
	}

	actionMatch := len(f.Actions) == 0
	if !actionMatch {
		_, actionMatch = f.Actions[action.Action]
	}

	if !actionMatch {
		return false
	}

	if len(f.Receivers) == 0 {
		return contractMatch
	}

	receiverMatch := anyInSet(matchedVia, f.Receivers)

	if len(f.Contracts) == 0 {
		return receiverMatch
	}

	return contractMatch && receiverMatch
}

// anyInSet reports whether any element of keys is a key of m.
func anyInSet(keys []uint64, m map[uint64]struct{}) bool {
	for _, k := range keys {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

type StreamError struct {
	Code    uint16
	Message string
}

// maxAhead bounds unacked in-flight actions per subscription.
const maxAhead = 10000

type Subscription struct {
	id        uint64
	filter    ActionFilter
	sendCh    chan StreamedAction
	errorCh   chan StreamError
	createdAt time.Time

	// ascending globalSeqs sent but not yet acked; len == exact in-flight count
	ackMu       sync.Mutex
	pendingSeqs []uint64

	resyncSignalled atomic.Bool
}

func (s *Subscription) recordSent(globalSeq uint64) {
	s.ackMu.Lock()
	s.pendingSeqs = append(s.pendingSeqs, globalSeq)
	s.ackMu.Unlock()
}

// inFlight returns the number of actions sent but not yet acked.
func (s *Subscription) inFlight() int {
	s.ackMu.Lock()
	n := len(s.pendingSeqs)
	s.ackMu.Unlock()
	return n
}

func (s *Subscription) canSend() bool {
	return s.inFlight() < maxAhead
}

func (s *Subscription) ResetCounters() {
	s.ackMu.Lock()
	s.pendingSeqs = s.pendingSeqs[:0]
	s.ackMu.Unlock()
}

// Ack releases backpressure for every in-flight action at or below globalSeq.
func (s *Subscription) Ack(globalSeq uint64) {
	s.ackMu.Lock()
	i := 0
	for i < len(s.pendingSeqs) && s.pendingSeqs[i] <= globalSeq {
		i++
	}
	if i > 0 {
		remaining := copy(s.pendingSeqs, s.pendingSeqs[i:])
		s.pendingSeqs = s.pendingSeqs[:remaining]
	}
	s.ackMu.Unlock()
}

// tryDeliver owns the delivery policy: nothing sends past a latched resync, and a blocked window or full buffer signals resync instead of dropping silently.
func (s *Subscription) tryDeliver(action StreamedAction, matchedVia []uint64) bool {
	if s.resyncSignalled.Load() {
		return false
	}
	if !s.filter.Matches(action, matchedVia) {
		logger.Printf("debug-stream", "Subscription %d filter rejected: contract=%x receiver=%x action=%x matchedVia=%v (filter: contracts=%d receivers=%d actions=%d)",
			s.id, action.Contract, action.Receiver, action.Action, matchedVia,
			len(s.filter.Contracts), len(s.filter.Receivers), len(s.filter.Actions))
		return false
	}
	if !s.canSend() {
		s.signalResync("ack window exceeded")
		return false
	}
	select {
	case s.sendCh <- action:
		s.recordSent(action.GlobalSeq)
		return true
	default:
		s.signalResync("send buffer full")
		return false
	}
}

// signalResync tells the client to reconnect and resume; catch-up re-serves from the index.
func (s *Subscription) signalResync(reason string) {
	if s.resyncSignalled.Load() {
		return
	}
	select {
	case s.errorCh <- StreamError{Code: ActionErrorResyncRequired, Message: "resync required: " + reason}:
		s.resyncSignalled.Store(true)
		logger.Printf("stream", "Subscription %d resync required: %s", s.id, reason)
	default:
	}
}

const (
	MsgTypeActionSubscribe uint8 = 0x30
	MsgTypeActionAck       uint8 = 0x31
	MsgTypeActionBatch     uint8 = 0x32
	MsgTypeActionHeartbeat uint8 = 0x33
	MsgTypeActionError     uint8 = 0x34
	MsgTypeActionDecoded   uint8 = 0x35
	MsgTypeCatchupComplete uint8 = 0x36

	MaxStreamMessageSize = 10 * 1024 * 1024

	ActionErrorInvalidRequest   uint16 = 1
	ActionErrorServerSyncing    uint16 = 2
	ActionErrorMaxClients       uint16 = 3
	ActionErrorNoActions        uint16 = 4
	ActionErrorDataInconsistent uint16 = 5
	ActionErrorResyncRequired   uint16 = 6
)
