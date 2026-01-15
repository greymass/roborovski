package corereader

import (
	"testing"
)

func TestFilterRawBlock_Actions(t *testing.T) {
	namesInBlock := []uint64{1111, 2222, 5555, 8888}

	notif := RawBlock{
		BlockNum:     100,
		BlockTime:    1000000,
		NamesInBlock: namesInBlock,
		Notifications: map[uint64][]uint64{
			1111: {1, 2},
			2222: {3},
		},
		ActionMeta: []ActionMetadata{
			{GlobalSeq: 1, Contract: 5555, Action: 6666},
			{GlobalSeq: 2, Contract: 5555, Action: 7777},
			{GlobalSeq: 3, Contract: 8888, Action: 9999},
		},
		Actions: []CanonicalAction{
			{
				ActionOrdinal:      1,
				CreatorAO:          0,
				ReceiverUint64:     5555,
				DataIndex:          0,
				AuthAccountIndexes: []uint32{0},
				GlobalSeqUint64:    1,
				TrxIndex:           0,
				ContractUint64:     5555,
				ActionUint64:       6666,
			},
			{
				ActionOrdinal:      2,
				CreatorAO:          1,
				ReceiverUint64:     1111,
				DataIndex:          1,
				AuthAccountIndexes: []uint32{0},
				GlobalSeqUint64:    2,
				TrxIndex:           0,
				ContractUint64:     5555,
				ActionUint64:       7777,
			},
			{
				ActionOrdinal:      1,
				CreatorAO:          0,
				ReceiverUint64:     8888,
				DataIndex:          2,
				AuthAccountIndexes: []uint32{1},
				GlobalSeqUint64:    3,
				TrxIndex:           1,
				ContractUint64:     8888,
				ActionUint64:       9999,
			},
		},
	}

	filtered := FilterRawBlock(notif, nil)

	if filtered.BlockNum != 100 {
		t.Errorf("Expected BlockNum 100, got %d", filtered.BlockNum)
	}
	if filtered.BlockTime != 1000000 {
		t.Errorf("Expected BlockTime 1000000, got %d", filtered.BlockTime)
	}

	if len(filtered.Executions) != 2 {
		t.Errorf("Expected 2 executions (actions 1 and 3), got %d", len(filtered.Executions))
	}

	if len(filtered.Actions) == 0 {
		t.Error("Expected some filtered actions, got 0")
	}

	for _, action := range filtered.Actions {
		if action.Account == 0 {
			t.Error("Action has zero Account")
		}
		if action.Contract == 0 {
			t.Error("Action has zero Contract")
		}
		if action.GlobalSeq == 0 {
			t.Error("Action has zero GlobalSeq")
		}
	}
}

func TestFilterRawBlock_WithActionFilter(t *testing.T) {
	namesInBlock := []uint64{1111, 5555}

	notif := RawBlock{
		BlockNum:     100,
		BlockTime:    1000000,
		NamesInBlock: namesInBlock,
		Notifications: map[uint64][]uint64{
			5555: {1},
		},
		ActionMeta: []ActionMetadata{
			{GlobalSeq: 1, Contract: 5555, Action: 6666},
		},
		Actions: []CanonicalAction{
			{
				ActionOrdinal:      1,
				CreatorAO:          0,
				ReceiverUint64:     5555,
				DataIndex:          0,
				AuthAccountIndexes: []uint32{0},
				GlobalSeqUint64:    1,
				TrxIndex:           0,
				ContractUint64:     5555,
				ActionUint64:       6666,
			},
		},
	}

	actionFilter := func(contract, action uint64) bool {
		return contract == 5555
	}

	filtered := FilterRawBlock(notif, actionFilter)

	if len(filtered.Actions) != 0 {
		t.Errorf("Expected 0 actions (filtered), got %d", len(filtered.Actions))
	}
}

func TestFilterRawBlock_EmptyBlock(t *testing.T) {
	notif := RawBlock{
		BlockNum:      300,
		BlockTime:     3000000,
		Notifications: map[uint64][]uint64{},
	}

	filtered := FilterRawBlock(notif, nil)

	if len(filtered.Actions) != 0 {
		t.Errorf("Expected 0 actions for empty block, got %d", len(filtered.Actions))
	}
	if len(filtered.Executions) != 0 {
		t.Errorf("Expected 0 executions for empty block, got %d", len(filtered.Executions))
	}
}

func TestFilterRawBlock_Generated(t *testing.T) {
	for _, size := range []int{50, 330} {
		block := generateRealisticRawBlock(size)

		result := FilterRawBlock(block, nil)

		if result.BlockNum != block.BlockNum {
			t.Errorf("BlockNum mismatch: got %d, want %d", result.BlockNum, block.BlockNum)
		}
		if len(result.Actions) == 0 {
			t.Error("Expected some filtered actions")
		}
	}
}

func TestFilterRawBlock_BufferReuse(t *testing.T) {
	block := generateRealisticRawBlock(100)
	actionsBuf := make([]Action, 0, 200)
	execBuf := make([]ContractExecution, 0, 50)

	result1, actionsBuf, execBuf := FilterRawBlockInto(block, nil, actionsBuf, execBuf)
	result2, _, _ := FilterRawBlockInto(block, nil, actionsBuf, execBuf)

	if len(result1.Actions) != len(result2.Actions) {
		t.Errorf("Buffer reuse should produce same results: %d vs %d",
			len(result1.Actions), len(result2.Actions))
	}
}

func TestFilterRawBlock_TimedMatchesUntimed(t *testing.T) {
	block := generateRealisticRawBlock(100)

	actionsBuf1 := make([]Action, 0, 100)
	execBuf1 := make([]ContractExecution, 0, 25)
	untimed, _, _ := FilterRawBlockInto(block, nil, actionsBuf1, execBuf1)

	actionsBuf2 := make([]Action, 0, 100)
	execBuf2 := make([]ContractExecution, 0, 25)
	timing := &FilterTiming{}
	timed, _, _ := FilterRawBlockIntoTimed(block, nil, actionsBuf2, execBuf2, timing)

	if len(timed.Actions) != len(untimed.Actions) {
		t.Errorf("Action count mismatch: Timed=%d, Untimed=%d",
			len(timed.Actions), len(untimed.Actions))
	}
	if len(timed.Executions) != len(untimed.Executions) {
		t.Errorf("Execution count mismatch: Timed=%d, Untimed=%d",
			len(timed.Executions), len(untimed.Executions))
	}
	if timing.CallCount.Load() != 1 {
		t.Errorf("Expected CallCount=1, got %d", timing.CallCount.Load())
	}
}

func TestFilterTiming_Reset(t *testing.T) {
	ft := &FilterTiming{}

	ft.MetaLookupNs.Store(100)
	ft.GsCanonicalNs.Store(200)
	ft.OrdinalRootNs.Store(300)
	ft.FamilySlotsNs.Store(400)
	ft.NotifsLoopNs.Store(500)
	ft.ResetNs.Store(600)
	ft.CallCount.Store(10)
	ft.ReceiverHits.Store(50)
	ft.AuthorizerHits.Store(25)
	ft.Skipped.Store(5)

	ft.Reset()

	if ft.MetaLookupNs.Load() != 0 {
		t.Errorf("MetaLookupNs = %d, want 0", ft.MetaLookupNs.Load())
	}
	if ft.GsCanonicalNs.Load() != 0 {
		t.Errorf("GsCanonicalNs = %d, want 0", ft.GsCanonicalNs.Load())
	}
	if ft.OrdinalRootNs.Load() != 0 {
		t.Errorf("OrdinalRootNs = %d, want 0", ft.OrdinalRootNs.Load())
	}
	if ft.FamilySlotsNs.Load() != 0 {
		t.Errorf("FamilySlotsNs = %d, want 0", ft.FamilySlotsNs.Load())
	}
	if ft.NotifsLoopNs.Load() != 0 {
		t.Errorf("NotifsLoopNs = %d, want 0", ft.NotifsLoopNs.Load())
	}
	if ft.ResetNs.Load() != 0 {
		t.Errorf("ResetNs = %d, want 0", ft.ResetNs.Load())
	}
	if ft.CallCount.Load() != 0 {
		t.Errorf("CallCount = %d, want 0", ft.CallCount.Load())
	}
	if ft.ReceiverHits.Load() != 0 {
		t.Errorf("ReceiverHits = %d, want 0", ft.ReceiverHits.Load())
	}
	if ft.AuthorizerHits.Load() != 0 {
		t.Errorf("AuthorizerHits = %d, want 0", ft.AuthorizerHits.Load())
	}
	if ft.Skipped.Load() != 0 {
		t.Errorf("Skipped = %d, want 0", ft.Skipped.Load())
	}
}

func TestFilterTiming_GetBreakdown_ZeroTotal(t *testing.T) {
	ft := &FilterTiming{}

	metaPct, gsPct, ordPct, famPct, notifPct, resetPct := ft.GetBreakdown()

	if metaPct != 0 || gsPct != 0 || ordPct != 0 || famPct != 0 || notifPct != 0 || resetPct != 0 {
		t.Errorf("GetBreakdown with zero total should return all zeros, got: meta=%f, gs=%f, ord=%f, fam=%f, notif=%f, reset=%f",
			metaPct, gsPct, ordPct, famPct, notifPct, resetPct)
	}
}

func TestFilterTiming_GetBreakdown_WithValues(t *testing.T) {
	ft := &FilterTiming{}

	ft.MetaLookupNs.Store(100)
	ft.GsCanonicalNs.Store(200)
	ft.OrdinalRootNs.Store(300)
	ft.FamilySlotsNs.Store(100)
	ft.NotifsLoopNs.Store(200)
	ft.ResetNs.Store(100)

	metaPct, gsPct, ordPct, famPct, notifPct, resetPct := ft.GetBreakdown()

	total := 100.0 + 200.0 + 300.0 + 100.0 + 200.0 + 100.0

	expectedMeta := 100.0 / total * 100
	expectedGs := 200.0 / total * 100
	expectedOrd := 300.0 / total * 100
	expectedFam := 100.0 / total * 100
	expectedNotif := 200.0 / total * 100
	expectedReset := 100.0 / total * 100

	const epsilon = 0.01
	if abs(metaPct-expectedMeta) > epsilon {
		t.Errorf("metaPct = %f, want %f", metaPct, expectedMeta)
	}
	if abs(gsPct-expectedGs) > epsilon {
		t.Errorf("gsPct = %f, want %f", gsPct, expectedGs)
	}
	if abs(ordPct-expectedOrd) > epsilon {
		t.Errorf("ordPct = %f, want %f", ordPct, expectedOrd)
	}
	if abs(famPct-expectedFam) > epsilon {
		t.Errorf("famPct = %f, want %f", famPct, expectedFam)
	}
	if abs(notifPct-expectedNotif) > epsilon {
		t.Errorf("notifPct = %f, want %f", notifPct, expectedNotif)
	}
	if abs(resetPct-expectedReset) > epsilon {
		t.Errorf("resetPct = %f, want %f", resetPct, expectedReset)
	}

	sum := metaPct + gsPct + ordPct + famPct + notifPct + resetPct
	if abs(sum-100.0) > epsilon {
		t.Errorf("sum of percentages = %f, want 100.0", sum)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
