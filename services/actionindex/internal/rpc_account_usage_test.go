package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

type stubUsageReader struct {
	corereader.Reader
	bySeq map[uint64]chain.ActionTrace
}

func (s *stubUsageReader) GetActionsByGlobalSeqs(seqs []uint64) ([]chain.ActionTrace, *corereader.FetchTimings, error) {
	out := make([]chain.ActionTrace, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, s.bySeq[seq])
	}
	return out, &corereader.FetchTimings{}, nil
}

func TestFetchUsageByGlobalSeqs_CarriesOrdinals(t *testing.T) {
	reader := &stubUsageReader{
		bySeq: map[uint64]chain.ActionTrace{
			100: {
				ActionOrdinal: 1, CreatorAO: 0, ClosestUAAO: 0,
				Receiver: "eon.shipload",
				Act:      chain.Action{Account: "eon.shipload", Name: "resolve"},
				TrxID:    "aa", BlockNum: 10, BlockTime: "2026-06-10T00:00:00.000",
				CpuUsageUs: 310, NetUsageWords: 15,
			},
			101: {
				ActionOrdinal: 2, CreatorAO: 1, ClosestUAAO: 1,
				Receiver: "eon.shipload",
				Act:      chain.Action{Account: "eon.shipload", Name: "notify"},
				TrxID:    "aa", BlockNum: 10, BlockTime: "2026-06-10T00:00:00.000",
				CpuUsageUs: 310, NetUsageWords: 15,
			},
		},
	}

	results, _, err := fetchUsageByGlobalSeqs(reader, []uint64{100, 101})
	if err != nil {
		t.Fatalf("fetchUsageByGlobalSeqs error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 grouped transaction, got %d", len(results))
	}
	tx := results[0]
	if tx.TrxID != "aa" || len(tx.Actions) != 2 {
		t.Fatalf("expected trx aa with 2 actions, got trx=%s actions=%d", tx.TrxID, len(tx.Actions))
	}

	top := tx.Actions[0]
	if top.GlobalActionSeq != 100 || top.Receiver != "eon.shipload" ||
		top.ActionOrdinal != 1 || top.CreatorAO != 0 || top.ClosestUAAO != 0 {
		t.Errorf("top-level action = seq %d recv %s (ao=%d,cao=%d,cuaao=%d), want seq 100 recv eon.shipload (1,0,0)",
			top.GlobalActionSeq, top.Receiver, top.ActionOrdinal, top.CreatorAO, top.ClosestUAAO)
	}

	inline := tx.Actions[1]
	if inline.GlobalActionSeq != 101 || inline.Receiver != "eon.shipload" ||
		inline.ActionOrdinal != 2 || inline.CreatorAO != 1 || inline.ClosestUAAO != 1 {
		t.Errorf("inline action = seq %d recv %s (ao=%d,cao=%d,cuaao=%d), want seq 101 recv eon.shipload (2,1,1)",
			inline.GlobalActionSeq, inline.Receiver, inline.ActionOrdinal, inline.CreatorAO, inline.ClosestUAAO)
	}
}

func TestFetchUsageByGlobalSeqs_CarriesAuthorization(t *testing.T) {
	reader := &stubUsageReader{
		bySeq: map[uint64]chain.ActionTrace{
			200: {
				ActionOrdinal: 1, CreatorAO: 0, ClosestUAAO: 0,
				Receiver: "eon.shipload",
				Act: chain.Action{
					Account: "eon.shipload", Name: "resolve",
					Authorization: []chain.PermissionLevel{
						{Actor: "player.gm", Permission: "active"},
						{Actor: "cosigner.gm", Permission: "active"},
					},
				},
				TrxID: "bb", BlockNum: 11, BlockTime: "2026-06-10T00:00:01.000",
			},
			201: {
				ActionOrdinal: 2, CreatorAO: 0, ClosestUAAO: 0,
				Receiver: "eon.shipload",
				Act:      chain.Action{Account: "eon.shipload", Name: "cfaction"},
				TrxID:    "bb", BlockNum: 11, BlockTime: "2026-06-10T00:00:01.000",
			},
		},
	}

	results, _, err := fetchUsageByGlobalSeqs(reader, []uint64{200, 201})
	if err != nil {
		t.Fatalf("fetchUsageByGlobalSeqs error: %v", err)
	}
	if len(results) != 1 || len(results[0].Actions) != 2 {
		t.Fatalf("expected 1 transaction with 2 actions, got %+v", results)
	}

	auth := results[0].Actions[0].Authorization
	if len(auth) != 2 ||
		auth[0].Actor != "player.gm" || auth[0].Permission != "active" ||
		auth[1].Actor != "cosigner.gm" || auth[1].Permission != "active" {
		t.Errorf("authorization = %+v, want [{player.gm active} {cosigner.gm active}] in order", auth)
	}

	if len(results[0].Actions[1].Authorization) != 0 {
		t.Errorf("empty-auth action carried authorization %+v, want empty", results[0].Actions[1].Authorization)
	}
}
