package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

func tokenSub(b *ActionBroadcaster) *Subscription {
	return b.Subscribe(ActionFilter{
		Contracts: map[uint64]struct{}{chain.StringToName("eosio.token"): {}},
	})
}

func expectResync(t *testing.T, sub *Subscription) {
	t.Helper()
	select {
	case e := <-sub.errorCh:
		if e.Code != ActionErrorResyncRequired {
			t.Fatalf("error code = %d, want %d (resync required)", e.Code, ActionErrorResyncRequired)
		}
	default:
		t.Fatal("expected a resync error on errorCh")
	}
}

func expectNoError(t *testing.T, sub *Subscription) {
	t.Helper()
	select {
	case e := <-sub.errorCh:
		t.Fatalf("unexpected error: %+v", e)
	default:
	}
}

// subFilledToResync returns a live broadcaster and a subscription broadcast past its send buffer into a resync signal.
func subFilledToResync(t *testing.T) (*ActionBroadcaster, *Subscription) {
	t.Helper()
	b := NewActionBroadcaster()
	b.SetLiveMode(true)
	sub := tokenSub(b)
	for seq := uint64(1); seq <= 1001; seq++ {
		act, via := tokenAction(seq)
		b.Broadcast(act, via)
	}
	expectResync(t, sub)
	return b, sub
}

func TestBroadcast_BufferFull_SignalsResyncOnce(t *testing.T) {
	b, sub := subFilledToResync(t)

	act, via := tokenAction(2000)
	b.Broadcast(act, via)
	expectNoError(t, sub)
}

func TestBroadcast_AfterResyncSignalled_StopsDelivering(t *testing.T) {
	b, sub := subFilledToResync(t)

	<-sub.sendCh

	act, via := tokenAction(2000)
	if b.Broadcast(act, via) {
		t.Fatal("Broadcast reported delivery after resync was signalled")
	}

	for {
		select {
		case a := <-sub.sendCh:
			if a.GlobalSeq == 2000 {
				t.Fatal("action 2000 delivered after resync was signalled")
			}
		default:
			return
		}
	}
}

func TestBroadcast_AckWindowClosed_SignalsResync(t *testing.T) {
	b := NewActionBroadcaster()
	b.SetLiveMode(true)
	sub := tokenSub(b)

	for seq := uint64(1); seq <= maxAhead; seq++ {
		sub.recordSent(seq)
	}
	act, via := tokenAction(20000)
	b.Broadcast(act, via)

	expectResync(t, sub)
	assertNoMore(t, sub, 0)
}

func TestSetLiveMode_ResumeSignalsResync(t *testing.T) {
	b := NewActionBroadcaster()
	b.SetLiveMode(true)
	sub := tokenSub(b)
	expectNoError(t, sub)

	b.SetLiveMode(false)
	b.SetLiveMode(true)
	expectResync(t, sub)
}

func TestSetLiveMode_InitialTransition_NoSubscribers_NoPanic(t *testing.T) {
	b := NewActionBroadcaster()
	b.SetLiveMode(true)
}
