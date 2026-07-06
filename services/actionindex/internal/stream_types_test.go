package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

func TestSubscription_BackpressureWindow(t *testing.T) {
	s := &Subscription{}

	for i := 1; i <= maxAhead; i++ {
		if !s.canSend() {
			t.Fatalf("canSend() = false at %d in-flight, want true below %d", i-1, maxAhead)
		}
		s.recordSent(uint64(i))
	}
	if s.canSend() {
		t.Fatalf("canSend() = true at %d in-flight, want false", s.inFlight())
	}
	if got := s.inFlight(); got != maxAhead {
		t.Fatalf("inFlight() = %d, want %d", got, maxAhead)
	}

	s.Ack(uint64(maxAhead / 2))
	if got, want := s.inFlight(), maxAhead-maxAhead/2; got != want {
		t.Fatalf("inFlight() after ack = %d, want %d", got, want)
	}
	if !s.canSend() {
		t.Fatal("canSend() = false after ack released half the window, want true")
	}
}

func TestSubscription_AckSparseAndIdempotent(t *testing.T) {
	s := &Subscription{}

	seqs := []uint64{100, 5000, 900000, 900001}
	for _, q := range seqs {
		s.recordSent(q)
	}
	if got := s.inFlight(); got != len(seqs) {
		t.Fatalf("inFlight() = %d, want %d", got, len(seqs))
	}

	s.Ack(5000)
	if got := s.inFlight(); got != 2 {
		t.Fatalf("inFlight() after Ack(5000) = %d, want 2", got)
	}
	s.Ack(5000) // duplicate ack is a no-op
	if got := s.inFlight(); got != 2 {
		t.Fatalf("inFlight() after duplicate ack = %d, want 2", got)
	}

	s.Ack(1_000_000) // ack past everything drains the window
	if got := s.inFlight(); got != 0 {
		t.Fatalf("inFlight() after draining ack = %d, want 0", got)
	}
	if !s.canSend() {
		t.Fatal("canSend() = false with empty window")
	}
}

func TestSubscription_ResetCounters(t *testing.T) {
	s := &Subscription{}
	for i := 1; i <= 100; i++ {
		s.recordSent(uint64(i))
	}

	s.ResetCounters()

	if got := s.inFlight(); got != 0 {
		t.Fatalf("inFlight() after reset = %d, want 0", got)
	}
	if s.sendCount.Load() != 0 {
		t.Fatalf("sendCount not reset: %d", s.sendCount.Load())
	}
}

func TestActionFilter_Matches_ContractOnly(t *testing.T) {
	contract := uint64(5000)
	action := uint64(6000)
	receiver := uint64(1000)

	filter := ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "receiver equals contract",
			action: StreamedAction{
				Contract: contract,
				Receiver: contract,
				Action:   action,
			},
			want: true,
		},
		{
			name: "receiver differs from contract",
			action: StreamedAction{
				Contract: contract,
				Receiver: receiver,
				Action:   action,
			},
			want: true,
		},
		{
			name: "wrong contract",
			action: StreamedAction{
				Contract: 9999,
				Receiver: receiver,
				Action:   action,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action, []uint64{tt.action.Receiver})
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_ContractAndReceiver(t *testing.T) {
	contract := uint64(5000)
	receiver := uint64(1000)
	action := uint64(6000)

	filter := ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
		Receivers: map[uint64]struct{}{receiver: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "matches both contract and receiver",
			action: StreamedAction{
				Contract: contract,
				Receiver: receiver,
				Action:   action,
			},
			want: true,
		},
		{
			name: "matches contract but not receiver",
			action: StreamedAction{
				Contract: contract,
				Receiver: 9999,
				Action:   action,
			},
			want: false,
		},
		{
			name: "matches receiver but not contract",
			action: StreamedAction{
				Contract: 9999,
				Receiver: receiver,
				Action:   action,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action, []uint64{tt.action.Receiver})
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_ReceiverOnly(t *testing.T) {
	receiver := uint64(1000)
	action := uint64(6000)

	filter := ActionFilter{
		Receivers: map[uint64]struct{}{receiver: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "matches receiver",
			action: StreamedAction{
				Contract: 5000,
				Receiver: receiver,
				Action:   action,
			},
			want: true,
		},
		{
			name: "does not match receiver",
			action: StreamedAction{
				Contract: 5000,
				Receiver: 9999,
				Action:   action,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action, []uint64{tt.action.Receiver})
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_WithActions(t *testing.T) {
	contract := uint64(5000)
	action := uint64(6000)

	filter := ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
		Actions:   map[uint64]struct{}{action: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "matches contract and action",
			action: StreamedAction{
				Contract: contract,
				Receiver: 1000,
				Action:   action,
			},
			want: true,
		},
		{
			name: "matches contract but not action",
			action: StreamedAction{
				Contract: contract,
				Receiver: 1000,
				Action:   9999,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action, []uint64{tt.action.Receiver})
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_EmptyFilter(t *testing.T) {
	filter := ActionFilter{}

	action := StreamedAction{
		Contract: 5000,
		Receiver: 1000,
		Action:   6000,
	}

	if filter.Matches(action, []uint64{action.Receiver}) {
		t.Error("Empty filter should not match any action")
	}
}

func TestActionFilter_Matches_MatchedViaSingle(t *testing.T) {
	a := chain.StringToName("alice")
	f := ActionFilter{
		Receivers: map[uint64]struct{}{a: {}},
	}
	act := StreamedAction{
		Contract: chain.StringToName("eosio.token"),
		Action:   chain.StringToName("transfer"),
		Receiver: chain.StringToName("bob"), // chain truth != matched-via
	}
	if !f.Matches(act, []uint64{a}) {
		t.Errorf("expected match: filter Receivers={alice}, matchedVia=[alice]")
	}
}

func TestActionFilter_Matches_MatchedViaMulti(t *testing.T) {
	a := chain.StringToName("alice")
	b := chain.StringToName("bob")
	f := ActionFilter{
		Receivers: map[uint64]struct{}{a: {}},
	}
	act := StreamedAction{
		Contract: chain.StringToName("eosio.token"),
		Action:   chain.StringToName("transfer"),
		Receiver: chain.StringToName("carol"),
	}
	if !f.Matches(act, []uint64{b, a}) {
		t.Errorf("expected match: any element of matchedVia in Receivers should match")
	}
}

func TestActionFilter_Matches_MatchedViaNone(t *testing.T) {
	a := chain.StringToName("alice")
	b := chain.StringToName("bob")
	f := ActionFilter{
		Receivers: map[uint64]struct{}{a: {}},
	}
	act := StreamedAction{
		Contract: chain.StringToName("eosio.token"),
		Action:   chain.StringToName("transfer"),
		Receiver: a,
	}
	if f.Matches(act, []uint64{b}) {
		t.Errorf("expected no match: matchedVia=[bob] is not in filter Receivers={alice}; chain Receiver is no longer consulted")
	}
}

func TestActionFilter_Matches_ContractOnly_NilMatchedVia(t *testing.T) {
	f := ActionFilter{
		Contracts: map[uint64]struct{}{chain.StringToName("eosio.token"): {}},
	}
	act := StreamedAction{
		Contract: chain.StringToName("eosio.token"),
		Action:   chain.StringToName("transfer"),
		Receiver: chain.StringToName("alice"),
	}
	if !f.Matches(act, nil) {
		t.Errorf("expected match on contract-only filter regardless of matchedVia")
	}
}
