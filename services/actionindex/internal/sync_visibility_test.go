package internal

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

// Every sequence at or below the published head must be readable by a catch-up reader.
func TestProcessBlock_PublishesHeadOnlyAfterCommit(t *testing.T) {
	const seq = uint64(500)
	const blockNum = uint32(50)
	contract := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")

	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	// Live mode indexes through the WAL, where the commit is the visibility point.
	idx.SetBulkMode(false)
	defer idx.walCompactor.Stop()

	b := NewActionBroadcaster()
	b.SetState(100, 100)
	b.SetLiveMode(true)
	sub := tokenSub(b)

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	raw := &corereader.RawBlock{
		BlockNum:  blockNum,
		BlockTime: 1000,
		Actions: []corereader.CanonicalAction{
			{GlobalSeqUint64: seq, DataIndex: 0, CpuUsageUs: 42, NetUsageWords: 7},
		},
	}
	raw.SetActionData(payload, []uint32{0}, []uint32{uint32(len(payload))})
	raw.SetTransactionIDs([][32]byte{{0x01}})

	block := corereader.Block{
		BlockNum:  blockNum,
		BlockTime: 1000,
		MinSeq:    seq,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			{Account: contract, Contract: contract, Action: transfer, GlobalSeq: seq, Receiver: contract},
		},
	}
	block.SetRawBlock(raw)

	syncer := &Syncer{
		indexes:            idx,
		broadcaster:        b,
		reader:             &stubReaderForBroadcast{},
		config:             &Config{},
		bulkCommitInterval: 10000,
	}
	proc := NewAccountHistoryProcessor(syncer)

	if err := proc.ProcessBlock(block); err != nil {
		t.Fatalf("ProcessBlock error: %v", err)
	}

	if head, _ := b.GetState(); head >= seq {
		t.Errorf("head = %d after ProcessBlock alone, want < %d: head advertised before the action is readable", head, seq)
	}
	assertNoMore(t, sub, 50*time.Millisecond)

	if err := proc.Commit(blockNum, false); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	head, _ := b.GetState()
	if head != seq {
		t.Errorf("head = %d after Commit, want %d", head, seq)
	}

	delivered := drainExactly(t, sub, 1)
	if len(delivered) != 1 {
		t.Fatalf("expected 1 live delivery after Commit, got %d", len(delivered))
	}
	if delivered[0].GlobalSeq != seq {
		t.Errorf("delivered global_seq = %d, want %d", delivered[0].GlobalSeq, seq)
	}

	catchupReader := &stubBaseReader{bySeq: map[uint64]chain.ActionTrace{
		seq: {
			BlockNum: blockNum, BlockTime: "1970-01-01T00:16:40.000",
			Receiver: "eosio.token",
			Act:      chain.Action{Account: "eosio.token", Name: "transfer", Data: hex.EncodeToString(payload)},
		},
	}}
	catchup := NewStreamCatchup(idx, catchupReader, ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
	}, 1, head)

	var readable []uint64
	if err := catchup.Run(context.Background(), func(a StreamedAction) error {
		readable = append(readable, a.GlobalSeq)
		return nil
	}); err != nil {
		t.Fatalf("catchup error: %v", err)
	}
	if len(readable) != 1 || readable[0] != seq {
		t.Fatalf("catch-up over [1, %d] returned %v, want [%d]: head advanced past an unreadable sequence", head, readable, seq)
	}
}
