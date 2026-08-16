package internal

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

const testTrxID = "5b273364b825dfd58e7ac36e4014a24f1547cb5b1786a586af31c5a83daaa03b"

func TestProcessBatch_CarriesTrxID(t *testing.T) {
	tracked := chain.StringToName("shipload.gm")

	trace := chain.ActionTrace{
		TrxID:    testTrxID,
		Receiver: "shipload.gm",
		Act: chain.Action{
			Account: "shipload.gm",
			Name:    "consume",
		},
	}

	catchup := &StreamCatchup{
		reader: &stubBaseReader{traces: []chain.ActionTrace{trace}},
		filter: ActionFilter{
			Receivers: map[uint64]struct{}{tracked: {}},
		},
	}

	var delivered []StreamedAction
	sent, err := catchup.processBatch([]uint64{900}, tracked, nil, func(a StreamedAction) error {
		delivered = append(delivered, a)
		return nil
	})
	if err != nil {
		t.Fatalf("processBatch error: %v", err)
	}
	if sent != 1 || len(delivered) != 1 {
		t.Fatalf("expected 1 delivery, got sent=%d delivered=%d", sent, len(delivered))
	}
	if got := delivered[0].TrxID; got != testTrxID {
		t.Errorf("TrxID = %q, want %q", got, testTrxID)
	}
}

func TestBroadcastActions_FetchedPath_CarriesTrxID(t *testing.T) {
	contract := chain.StringToName("shipload.gm")
	action := chain.StringToName("consume")
	const seq = uint64(700)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	sub := broadcaster.Subscribe(ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
	})

	reader := &stubReaderForBroadcast{
		bySeq: map[uint64]chain.ActionTrace{
			seq: {
				TrxID:    testTrxID,
				BlockNum: 1, BlockTime: "1970-01-01T00:16:40.000",
				Receiver: "shipload.gm",
				Act:      chain.Action{Account: "shipload.gm", Name: "consume"},
			},
		},
	}

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			{
				Account: contract, Contract: contract, Action: action,
				GlobalSeq: seq, Receiver: contract,
			},
		},
	}

	syncer := &Syncer{broadcaster: broadcaster, reader: reader, config: &Config{}}
	proc := NewAccountHistoryProcessor(syncer)
	if err := proc.broadcastActions(block); err != nil {
		t.Fatalf("broadcastActions error: %v", err)
	}

	delivered := drainExactly(t, sub, 1)
	if len(delivered) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(delivered))
	}
	if got := delivered[0].TrxID; got != testTrxID {
		t.Errorf("TrxID = %q, want %q", got, testTrxID)
	}
}

func TestBroadcastActions_InlinePath_CarriesTrxID(t *testing.T) {
	contract := chain.StringToName("shipload.gm")
	action := chain.StringToName("consume")
	const seq = uint64(343972368)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	sub := broadcaster.Subscribe(ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
	})

	var id [32]byte
	raw, err := hex.DecodeString(testTrxID)
	if err != nil {
		t.Fatal(err)
	}
	copy(id[:], raw)

	rawData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	rawBlock := &corereader.RawBlock{
		BlockNum:  1,
		BlockTime: 1000,
		Actions: []corereader.CanonicalAction{
			{GlobalSeqUint64: seq, DataIndex: 0},
		},
	}
	rawBlock.SetActionData(rawData, []uint32{0}, []uint32{uint32(len(rawData))})
	rawBlock.SetTransactionIDs([][32]byte{id})

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			{
				Account: contract, Contract: contract, Action: action,
				GlobalSeq: seq, Receiver: contract, TrxIndex: 0,
			},
		},
	}
	block.SetRawBlock(rawBlock)

	syncer := &Syncer{broadcaster: broadcaster, reader: &stubReaderForBroadcast{}, config: &Config{}}
	proc := NewAccountHistoryProcessor(syncer)
	if err := proc.broadcastActions(block); err != nil {
		t.Fatalf("broadcastActions error: %v", err)
	}

	delivered := drainExactly(t, sub, 1)
	if len(delivered) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(delivered))
	}
	if got := delivered[0].TrxID; got != testTrxID {
		t.Errorf("TrxID = %q, want %q", got, testTrxID)
	}
}

func TestBuildWsActionMessage_CarriesTrxID(t *testing.T) {
	msg := buildWsActionMessage(StreamedAction{GlobalSeq: 42, TrxID: testTrxID}, false, nil, 0)
	if msg.TrxID != testTrxID {
		t.Fatalf("TrxID = %q, want %q", msg.TrxID, testTrxID)
	}

	encoded, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if !strings.Contains(string(encoded), `"trx_id":"`+testTrxID+`"`) {
		t.Errorf("encoded message missing trx_id field: %s", encoded)
	}
}

func TestBuildWsActionMessage_TrxIDIsUnconditional(t *testing.T) {
	encoded, err := json.Marshal(buildWsActionMessage(StreamedAction{GlobalSeq: 42}, false, nil, 0))
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if !strings.Contains(string(encoded), `"trx_id"`) {
		t.Errorf("trx_id must always be serialized, got: %s", encoded)
	}
}

func TestBroadcastActions_InlinePath_MissingTrxID_DataInconsistent(t *testing.T) {
	contract := chain.StringToName("shipload.gm")
	action := chain.StringToName("consume")
	const seq = uint64(343972368)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	sub := broadcaster.Subscribe(ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
	})

	// rawData yields no trx-id table, so the id resolves to zero (corrupt input)
	rawData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	rawBlock := &corereader.RawBlock{
		BlockNum:  1,
		BlockTime: 1000,
		Actions: []corereader.CanonicalAction{
			{GlobalSeqUint64: seq, DataIndex: 0},
		},
	}
	rawBlock.SetActionData(rawData, []uint32{0}, []uint32{uint32(len(rawData))})

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			{
				Account: contract, Contract: contract, Action: action,
				GlobalSeq: seq, Receiver: contract, TrxIndex: 0,
			},
		},
	}
	block.SetRawBlock(rawBlock)

	syncer := &Syncer{broadcaster: broadcaster, reader: &stubReaderForBroadcast{}, config: &Config{}}
	proc := NewAccountHistoryProcessor(syncer)
	if err := proc.broadcastActions(block); err == nil {
		t.Fatal("broadcastActions should error when a transaction id cannot be resolved")
	}

	select {
	case streamErr := <-sub.errorCh:
		if streamErr.Code != ActionErrorDataInconsistent {
			t.Errorf("error code = %d, want %d (DataInconsistent)", streamErr.Code, ActionErrorDataInconsistent)
		}
	default:
		t.Error("expected a DataInconsistent error broadcast to subscribers")
	}

	if got := drainExactly(t, sub, 1); len(got) != 0 {
		t.Errorf("no action should be delivered for a block with unresolvable trx ids, got %d", len(got))
	}
}
