package internal

import (
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

func TestBroadcastActions_CarriesOrdinals(t *testing.T) {
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
				ActionOrdinal: 3, CreatorAO: 1, ClosestUAAO: 1,
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
	d := delivered[0]
	if d.ActionOrdinal != 3 || d.CreatorActionOrdinal != 1 || d.ClosestUnnotifiedAncestorActionOrdinal != 1 {
		t.Errorf("ordinals = (%d,%d,%d), want (3,1,1)",
			d.ActionOrdinal, d.CreatorActionOrdinal, d.ClosestUnnotifiedAncestorActionOrdinal)
	}
}

// stubReaderForBroadcast satisfies corereader.Reader minimally for broadcastActions tests.
// Only GetActionsByGlobalSeqs is called from the fetched-traces sub-path.
type stubReaderForBroadcast struct {
	corereader.Reader
	bySeq map[uint64]chain.ActionTrace
}

func (s *stubReaderForBroadcast) GetActionsByGlobalSeqs(seqs []uint64) ([]chain.ActionTrace, *corereader.FetchTimings, error) {
	out := make([]chain.ActionTrace, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, s.bySeq[seq])
	}
	return out, &corereader.FetchTimings{}, nil
}

// drainExactly drains exactly n actions from the subscription. Returns nil if
// n actions don't arrive within a short timeout. Use this for the happy path
// where you know the expected count.
func drainExactly(t *testing.T, sub *Subscription, n int) []StreamedAction {
	t.Helper()
	got := make([]StreamedAction, 0, n)
	deadline := time.After(200 * time.Millisecond)
	for len(got) < n {
		select {
		case a, ok := <-sub.sendCh:
			if !ok {
				return got
			}
			got = append(got, a)
		case <-deadline:
			return got
		}
	}
	return got
}

// assertNoMore verifies that no additional actions arrive in a short window.
// Use after drainExactly when you need to prove the count is tight (not just >=).
func assertNoMore(t *testing.T, sub *Subscription, window time.Duration) {
	t.Helper()
	select {
	case extra := <-sub.sendCh:
		t.Fatalf("unexpected extra delivery: %+v", extra)
	case <-time.After(window):
	}
}

func TestBroadcastActions_AuthorizerOnly_UsesChainReceiver(t *testing.T) {
	atomic := chain.StringToName("atomicassets")
	createschema := chain.StringToName("createschema")
	shipload := chain.StringToName("shipload.gm")
	const seq = uint64(343972368)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	sub := broadcaster.Subscribe(ActionFilter{
		Receivers: map[uint64]struct{}{shipload: {}},
	})

	reader := &stubReaderForBroadcast{
		bySeq: map[uint64]chain.ActionTrace{
			seq: {
				BlockNum: 1, BlockTime: "1970-01-01T00:16:40.000",
				Receiver: "atomicassets",
				Act: chain.Action{
					Account: "atomicassets", Name: "createschema",
					Authorization: []chain.PermissionLevel{
						{Actor: "shipload.gm", Permission: "active"},
					},
				},
			},
		},
	}

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			// Authorizer-only entry: shipload.gm indexed because it authorized
			{Account: shipload, Contract: atomic, Action: createschema, GlobalSeq: seq, Receiver: atomic, IsAuthorizer: true},
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
	if delivered[0].Receiver != atomic {
		t.Errorf("Receiver = %s, want atomicassets (chain receiver)",
			chain.NameToString(delivered[0].Receiver))
	}
	if delivered[0].GlobalSeq != seq {
		t.Errorf("GlobalSeq = %d, want %d", delivered[0].GlobalSeq, seq)
	}
	if delivered[0].Contract != atomic {
		t.Errorf("Contract = %s, want atomicassets", chain.NameToString(delivered[0].Contract))
	}
	if delivered[0].Action != createschema {
		t.Errorf("Action = %s, want createschema", chain.NameToString(delivered[0].Action))
	}
}

func TestBroadcastActions_DedupesByGlobalSeq(t *testing.T) {
	contract := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")
	alice := chain.StringToName("alice")
	bob := chain.StringToName("bob")
	const seq = uint64(5000)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	// Sub matches both alice (chain receiver) AND bob (authorizer) on the same seq.
	sub := broadcaster.Subscribe(ActionFilter{
		Receivers: map[uint64]struct{}{alice: {}, bob: {}},
	})

	reader := &stubReaderForBroadcast{
		bySeq: map[uint64]chain.ActionTrace{
			seq: {
				BlockNum: 1, BlockTime: "1970-01-01T00:16:40.000",
				Receiver: "alice",
				Act: chain.Action{
					Account: "eosio.token", Name: "transfer",
					Authorization: []chain.PermissionLevel{
						{Actor: "bob", Permission: "active"},
					},
				},
			},
		},
	}

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			// Two index entries for the same seq: receiver-path + authorizer-path
			{Account: alice, Contract: contract, Action: transfer, GlobalSeq: seq, Receiver: alice, IsAuthorizer: false},
			{Account: bob, Contract: contract, Action: transfer, GlobalSeq: seq, Receiver: alice, IsAuthorizer: true},
		},
	}

	syncer := &Syncer{broadcaster: broadcaster, reader: reader, config: &Config{}}
	proc := NewAccountHistoryProcessor(syncer)
	if err := proc.broadcastActions(block); err != nil {
		t.Fatalf("broadcastActions error: %v", err)
	}

	delivered := drainExactly(t, sub, 1)
	assertNoMore(t, sub, 50*time.Millisecond)
	if len(delivered) != 1 {
		t.Fatalf("expected exactly 1 delivery (dedup), got %d", len(delivered))
	}
	if delivered[0].Receiver != alice {
		t.Errorf("Receiver = %s, want alice (chain receiver)", chain.NameToString(delivered[0].Receiver))
	}
	if delivered[0].GlobalSeq != seq {
		t.Errorf("GlobalSeq = %d, want %d", delivered[0].GlobalSeq, seq)
	}
	if delivered[0].Contract != contract {
		t.Errorf("Contract = %s, want eosio.token", chain.NameToString(delivered[0].Contract))
	}
	if delivered[0].Action != transfer {
		t.Errorf("Action = %s, want transfer", chain.NameToString(delivered[0].Action))
	}
}

func TestBroadcastActions_InlinePath_UsesChainReceiver(t *testing.T) {
	// Exercises the inline-data branch (block.HasActionData() == true), which
	// is the production hot path for live sync. Mirrors AuthorizerOnly_UsesChainReceiver
	// but constructs a RawBlock with action data so the broadcaster uses the
	// inline branch instead of the fetched-traces fallback.
	atomic := chain.StringToName("atomicassets")
	createschema := chain.StringToName("createschema")
	shipload := chain.StringToName("shipload.gm")
	const seq = uint64(343972368)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	sub := broadcaster.Subscribe(ActionFilter{
		Receivers: map[uint64]struct{}{shipload: {}},
	})

	rawData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	raw := &corereader.RawBlock{
		BlockNum:  1,
		BlockTime: 1000,
		Actions: []corereader.CanonicalAction{
			{GlobalSeqUint64: seq, DataIndex: 0, CpuUsageUs: 42, NetUsageWords: 7},
		},
	}
	raw.SetActionData(rawData, []uint32{0}, []uint32{uint32(len(rawData))})

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			{Account: shipload, Contract: atomic, Action: createschema, GlobalSeq: seq, Receiver: atomic, IsAuthorizer: true},
		},
	}
	block.SetRawBlock(raw)

	// Reader stub isn't used in the inline path but Syncer requires a non-nil one.
	syncer := &Syncer{broadcaster: broadcaster, reader: &stubReaderForBroadcast{}, config: &Config{}}
	proc := NewAccountHistoryProcessor(syncer)
	if err := proc.broadcastActions(block); err != nil {
		t.Fatalf("broadcastActions error: %v", err)
	}

	delivered := drainExactly(t, sub, 1)
	if len(delivered) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(delivered))
	}
	got := delivered[0]
	if got.Receiver != atomic {
		t.Errorf("Receiver = %s, want atomicassets (chain receiver)", chain.NameToString(got.Receiver))
	}
	if got.GlobalSeq != seq {
		t.Errorf("GlobalSeq = %d, want %d", got.GlobalSeq, seq)
	}
	if got.Contract != atomic {
		t.Errorf("Contract = %s, want atomicassets", chain.NameToString(got.Contract))
	}
	if got.Action != createschema {
		t.Errorf("Action = %s, want createschema", chain.NameToString(got.Action))
	}
	if got.CpuUsageUs != 42 || got.NetUsageWords != 7 {
		t.Errorf("resource usage = cpu=%d net=%d, want cpu=42 net=7", got.CpuUsageUs, got.NetUsageWords)
	}
}

func TestBroadcastActions_ContractOnlyFilter_DeliversAuthorizerIndexed(t *testing.T) {
	atomic := chain.StringToName("atomicassets")
	createschema := chain.StringToName("createschema")
	shipload := chain.StringToName("shipload.gm")
	const seq = uint64(343972368)

	broadcaster := NewActionBroadcaster()
	broadcaster.SetLiveMode(true)
	sub := broadcaster.Subscribe(ActionFilter{
		Contracts: map[uint64]struct{}{atomic: {}},
	})

	reader := &stubReaderForBroadcast{
		bySeq: map[uint64]chain.ActionTrace{
			seq: {
				BlockNum: 1, BlockTime: "1970-01-01T00:16:40.000",
				Receiver: "atomicassets",
				Act: chain.Action{
					Account: "atomicassets", Name: "createschema",
					Authorization: []chain.PermissionLevel{
						{Actor: "shipload.gm", Permission: "active"},
					},
				},
			},
		},
	}

	block := corereader.Block{
		BlockNum:  1,
		BlockTime: 1000,
		MaxSeq:    seq,
		Actions: []corereader.Action{
			{Account: shipload, Contract: atomic, Action: createschema, GlobalSeq: seq, Receiver: atomic, IsAuthorizer: true},
		},
	}

	syncer := &Syncer{broadcaster: broadcaster, reader: reader, config: &Config{}}
	proc := NewAccountHistoryProcessor(syncer)
	if err := proc.broadcastActions(block); err != nil {
		t.Fatalf("broadcastActions error: %v", err)
	}

	delivered := drainExactly(t, sub, 1)
	if len(delivered) != 1 {
		t.Fatalf("expected 1 delivery to contract-only subscriber, got %d", len(delivered))
	}
	got := delivered[0]
	if got.Contract != atomic {
		t.Errorf("Contract = %s, want atomicassets", chain.NameToString(got.Contract))
	}
	if got.Receiver != atomic {
		t.Errorf("Receiver = %s, want atomicassets", chain.NameToString(got.Receiver))
	}
}
