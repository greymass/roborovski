package internal

import (
	"sync/atomic"
	"time"
)

type ActionFilter struct {
	Contracts map[uint64]struct{}
	Receivers map[uint64]struct{}
	Actions   map[uint64]struct{}
}

func (f *ActionFilter) Matches(action StreamedAction) bool {
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

	_, receiverMatch := f.Receivers[action.Receiver]
	if len(f.Contracts) == 0 {
		return receiverMatch
	}

	return contractMatch && receiverMatch
}

type StreamError struct {
	Code    uint16
	Message string
}

type Subscription struct {
	id        uint64
	filter    ActionFilter
	sendCh    chan StreamedAction
	errorCh   chan StreamError
	createdAt time.Time

	// Backpressure tracking using simple counters (not globalSeq)
	sendCount atomic.Uint64
	ackCount  atomic.Uint64

	// Catchup mode - when true, broadcasts are skipped (gap filled from index after)
	catchingUp atomic.Bool

	// For debugging/logging
	lastSentSeq    atomic.Uint64
	blockedCount   atomic.Uint64
	lastBlockedLog atomic.Int64
}

func (s *Subscription) canSend() bool {
	const maxAhead = 10000
	sent := s.sendCount.Load()
	acked := s.ackCount.Load()
	if acked >= sent {
		return true
	}
	return sent-acked < maxAhead
}

func (s *Subscription) ResetCounters() {
	s.sendCount.Store(0)
	s.ackCount.Store(0)
}

func (s *Subscription) SetCatchingUp(catching bool) {
	s.catchingUp.Store(catching)
}

func (s *Subscription) IsCatchingUp() bool {
	return s.catchingUp.Load()
}

func (s *Subscription) Ack(globalSeq uint64) {
	s.ackCount.Add(1)
	_ = globalSeq // globalSeq no longer used for backpressure, just increment count
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

	ActionErrorInvalidRequest  uint16 = 1
	ActionErrorServerSyncing   uint16 = 2
	ActionErrorMaxClients      uint16 = 3
	ActionErrorNoActions       uint16 = 4
	ActionErrorDataInconsistent uint16 = 5
)
