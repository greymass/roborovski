package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

type stubBaseReader struct {
	corereader.BaseReader
	traces   []chain.ActionTrace
	bySeq    map[uint64]chain.ActionTrace
}

func (s *stubBaseReader) GetActionsByGlobalSeqs(seqs []uint64) ([]chain.ActionTrace, *corereader.FetchTimings, error) {
	if s.bySeq != nil {
		out := make([]chain.ActionTrace, 0, len(seqs))
		for _, seq := range seqs {
			out = append(out, s.bySeq[seq])
		}
		return out, &corereader.FetchTimings{}, nil
	}
	return s.traces, &corereader.FetchTimings{}, nil
}

func TestProcessBatch_DeliversAuthorizerOnlyAction(t *testing.T) {
	tracked := chain.StringToName("shipload.gm")

	setabiTrace := chain.ActionTrace{
		Receiver: "eosio",
		Act: chain.Action{
			Account: "eosio",
			Name:    "setabi",
			Authorization: []chain.PermissionLevel{
				{Actor: "shipload.gm", Permission: "active"},
			},
		},
	}

	catchup := &StreamCatchup{
		reader: &stubBaseReader{traces: []chain.ActionTrace{setabiTrace}},
		filter: ActionFilter{
			Receivers: map[uint64]struct{}{tracked: {}},
		},
	}

	var delivered []StreamedAction
	sent, err := catchup.processBatch([]uint64{1000}, tracked, nil, func(a StreamedAction) error {
		delivered = append(delivered, a)
		return nil
	})
	if err != nil {
		t.Fatalf("processBatch error: %v", err)
	}
	if sent != 1 {
		t.Fatalf("expected 1 action delivered, got %d", sent)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected delivered len 1, got %d", len(delivered))
	}
	got := delivered[0]
	if got.Contract != chain.StringToName("eosio") {
		t.Errorf("Contract = %s, want eosio", chain.NameToString(got.Contract))
	}
	if got.Action != chain.StringToName("setabi") {
		t.Errorf("Action = %s, want setabi", chain.NameToString(got.Action))
	}
	if got.Receiver != tracked {
		t.Errorf("Receiver = %s, want shipload.gm (tracked account)", chain.NameToString(got.Receiver))
	}
}

func TestProcessBatch_DeliversReceiverPathAction(t *testing.T) {
	tracked := chain.StringToName("shipload.gm")

	transferTrace := chain.ActionTrace{
		Receiver: "shipload.gm",
		Act: chain.Action{
			Account: "eosio.token",
			Name:    "transfer",
		},
	}

	catchup := &StreamCatchup{
		reader: &stubBaseReader{traces: []chain.ActionTrace{transferTrace}},
		filter: ActionFilter{
			Receivers: map[uint64]struct{}{tracked: {}},
		},
	}

	var delivered []StreamedAction
	sent, err := catchup.processBatch([]uint64{2000}, tracked, nil, func(a StreamedAction) error {
		delivered = append(delivered, a)
		return nil
	})
	if err != nil {
		t.Fatalf("processBatch error: %v", err)
	}
	if sent != 1 || len(delivered) != 1 {
		t.Fatalf("expected 1 delivery, got sent=%d delivered=%d", sent, len(delivered))
	}
	got := delivered[0]
	if got.Contract != chain.StringToName("eosio.token") {
		t.Errorf("Contract = %s, want eosio.token", chain.NameToString(got.Contract))
	}
	if got.Action != chain.StringToName("transfer") {
		t.Errorf("Action = %s, want transfer", chain.NameToString(got.Action))
	}
	if got.Receiver != tracked {
		t.Errorf("Receiver = %s, want shipload.gm", chain.NameToString(got.Receiver))
	}
}

func TestProcessBatch_MultiAccount_AttributesPerSeq(t *testing.T) {
	shipload := chain.StringToName("shipload.gm")
	platform := chain.StringToName("platform.gm")

	reader := &stubBaseReader{
		bySeq: map[uint64]chain.ActionTrace{
			100: {
				Receiver: "eosio",
				Act:      chain.Action{Account: "eosio", Name: "setabi"},
			},
			200: {
				Receiver: "shipload.gm",
				Act:      chain.Action{Account: "eosio.token", Name: "transfer"},
			},
			300: {
				Receiver: "eosio",
				Act:      chain.Action{Account: "eosio", Name: "setcode"},
			},
		},
	}

	seqToAccount := map[uint64]uint64{
		100: platform,
		200: shipload,
		300: platform,
	}

	catchup := &StreamCatchup{
		reader: reader,
		filter: ActionFilter{
			Receivers: map[uint64]struct{}{shipload: {}, platform: {}},
		},
	}

	delivered := map[uint64]StreamedAction{}
	sent, err := catchup.processBatch([]uint64{100, 200, 300}, 0, seqToAccount, func(a StreamedAction) error {
		delivered[a.GlobalSeq] = a
		return nil
	})
	if err != nil {
		t.Fatalf("processBatch error: %v", err)
	}
	if sent != 3 || len(delivered) != 3 {
		t.Fatalf("expected 3 deliveries, got sent=%d delivered=%d", sent, len(delivered))
	}
	if got := delivered[100].Receiver; got != platform {
		t.Errorf("seq 100 Receiver = %s, want platform.gm", chain.NameToString(got))
	}
	if got := delivered[200].Receiver; got != shipload {
		t.Errorf("seq 200 Receiver = %s, want shipload.gm", chain.NameToString(got))
	}
	if got := delivered[300].Receiver; got != platform {
		t.Errorf("seq 300 Receiver = %s, want platform.gm", chain.NameToString(got))
	}
}

func TestProcessBatch_AppliesContractActionFilter(t *testing.T) {
	tracked := chain.StringToName("shipload.gm")

	transferTrace := chain.ActionTrace{
		Receiver: "shipload.gm",
		Act: chain.Action{
			Account: "eosio.token",
			Name:    "transfer",
		},
	}

	catchup := &StreamCatchup{
		reader: &stubBaseReader{traces: []chain.ActionTrace{transferTrace}},
		filter: ActionFilter{
			Contracts: map[uint64]struct{}{chain.StringToName("eosio"): {}},
			Actions:   map[uint64]struct{}{chain.StringToName("setabi"): {}},
			Receivers: map[uint64]struct{}{tracked: {}},
		},
	}

	sent, err := catchup.processBatch([]uint64{1000}, tracked, nil, func(a StreamedAction) error {
		t.Fatalf("did not expect delivery: %+v", a)
		return nil
	})
	if err != nil {
		t.Fatalf("processBatch error: %v", err)
	}
	if sent != 0 {
		t.Fatalf("expected 0 deliveries, got %d", sent)
	}
}
