package internal

import (
	"errors"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

type failingSeqReader struct {
	corereader.BaseReader
	bySeq     map[uint64]chain.ActionTrace
	failSeq   uint64
	failsLeft int
	refreshes int
}

func (f *failingSeqReader) GetStateProps(bool) (uint32, uint32, error) {
	f.refreshes++
	return 0, 0, nil
}

func (f *failingSeqReader) GetActionsByGlobalSeqs(seqs []uint64) ([]chain.ActionTrace, *corereader.FetchTimings, error) {
	out := make([]chain.ActionTrace, 0, len(seqs))
	for _, seq := range seqs {
		if seq == f.failSeq && f.failsLeft != 0 {
			if f.failsLeft > 0 {
				f.failsLeft--
			}
			return nil, nil, errors.New("glob not found in slice (empty block index)")
		}
		out = append(out, f.bySeq[seq])
	}
	return out, &corereader.FetchTimings{}, nil
}

func gapFixture(failsLeft int) (*StreamCatchup, *failingSeqReader, uint64) {
	tracked := chain.StringToName("eosio.token")
	trace := func(n uint32) chain.ActionTrace {
		return chain.ActionTrace{BlockNum: n, Receiver: "eosio.token", Act: chain.Action{Account: "eosio.token", Name: "transfer"}}
	}
	r := &failingSeqReader{
		bySeq:     map[uint64]chain.ActionTrace{100: trace(1), 200: trace(2), 300: trace(3)},
		failSeq:   200,
		failsLeft: failsLeft,
	}
	c := &StreamCatchup{reader: r, filter: ActionFilter{Receivers: map[uint64]struct{}{tracked: {}}}}
	return c, r, tracked
}

func withoutRetryDelay(t *testing.T) {
	saved := catchupFetchRetryDelays
	catchupFetchRetryDelays = []time.Duration{0, 0, 0}
	t.Cleanup(func() { catchupFetchRetryDelays = saved })
}

func TestProcessBatch_UnfetchableSeqStopsTheRound(t *testing.T) {
	withoutRetryDelay(t)
	c, r, tracked := gapFixture(-1)

	var delivered []uint64
	sent, err := c.processBatch([]uint64{100, 200, 300}, tracked, nil, func(a StreamedAction) error {
		delivered = append(delivered, a.GlobalSeq)
		return nil
	})
	if err == nil {
		t.Fatalf("processBatch skipped seq 200: sent=%d delivered=%v", sent, delivered)
	}
	for _, seq := range delivered {
		if seq > 200 {
			t.Fatalf("delivered seq %d past the unfetchable seq 200: %v", seq, delivered)
		}
	}
	if r.refreshes == 0 {
		t.Fatalf("catch-up retried without refreshing the reader's view")
	}
}

func TestProcessBatch_SeqFetchedAfterRefresh(t *testing.T) {
	withoutRetryDelay(t)
	c, _, tracked := gapFixture(2)

	var delivered []uint64
	sent, err := c.processBatch([]uint64{100, 200, 300}, tracked, nil, func(a StreamedAction) error {
		delivered = append(delivered, a.GlobalSeq)
		return nil
	})
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if sent != 3 {
		t.Fatalf("sent %d, want 3 (delivered %v)", sent, delivered)
	}
}
