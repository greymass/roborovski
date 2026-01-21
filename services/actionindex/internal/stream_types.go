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
		return contractMatch && action.Receiver == action.Contract
	}

	_, receiverMatch := f.Receivers[action.Receiver]
	if len(f.Contracts) == 0 {
		return receiverMatch
	}

	return contractMatch && receiverMatch
}

type Subscription struct {
	id        uint64
	filter    ActionFilter
	sendCh    chan StreamedAction
	lastSent  atomic.Uint64
	lastAck   atomic.Uint64
	createdAt time.Time
}

func (s *Subscription) canSend() bool {
	const maxAhead = 10000
	return s.lastSent.Load()-s.lastAck.Load() < maxAhead
}

func (s *Subscription) Ack(globalSeq uint64) {
	for {
		current := s.lastAck.Load()
		if globalSeq <= current {
			return
		}
		if s.lastAck.CompareAndSwap(current, globalSeq) {
			return
		}
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

	ActionErrorInvalidRequest uint16 = 1
	ActionErrorServerSyncing  uint16 = 2
	ActionErrorMaxClients     uint16 = 3
	ActionErrorNoActions      uint16 = 4
)
