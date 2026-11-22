package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

func staleBlocks(storeData, compareData [][]byte) (*corereader.RawBlock, *corereader.RawBlock) {
	build := func(data [][]byte) *corereader.RawBlock {
		r := &corereader.RawBlock{BlockNum: 7}
		var raw []byte
		var offs, lens []uint32
		for i, d := range data {
			offs = append(offs, uint32(len(raw)))
			lens = append(lens, uint32(len(d)))
			raw = append(raw, d...)
			r.Actions = append(r.Actions, corereader.CanonicalAction{ActionOrdinal: uint32(i + 1), DataIndex: uint32(i)})
		}
		r.SetActionData(raw, offs, lens)
		return r
	}
	return build(storeData), build(compareData)
}

func newStaleComparer(enabled bool) *Comparer {
	c := &Comparer{cfg: CompareConfig{StaleEmptyData: enabled}}
	c.report = &CompareReport{Differences: map[string]int{}, StaleEmptyActions: map[string]int{}}
	return c
}

func TestStaleEmptyDataInBlockRecord(t *testing.T) {
	ra, rb := staleBlocks([][]byte{{1, 2}, {9, 9, 9}}, [][]byte{{1, 2}, {}})

	c := newStaleComparer(true)
	if _, _, stale := c.staleEmptyData(1, ra, rb, nil, nil); !stale || c.report.Differences[DiffBlockMismatch] != 0 {
		t.Errorf("stale store data over empty compare data: want stale and no difference, got %v %v", stale, c.report.Differences)
	}

	ra, rb = staleBlocks([][]byte{{1, 2}, {}}, [][]byte{{1, 2}, {9}})
	c = newStaleComparer(true)
	if _, _, stale := c.staleEmptyData(1, ra, rb, nil, nil); stale || c.report.Differences[DiffBlockMismatch] != 1 {
		t.Errorf("empty store data over compare data: want one difference, got %v %v", stale, c.report.Differences)
	}

	c = newStaleComparer(false)
	if _, _, stale := c.staleEmptyData(1, ra, rb, nil, nil); stale {
		t.Error("flag off: want no stale block")
	}
}

func TestStaleEmptyDataInActions(t *testing.T) {
	act := func(data, digest string) chain.ActionTrace {
		return chain.ActionTrace{
			Act:     chain.Action{Account: "eosmechanics", Name: "cpu", Data: data},
			Receipt: chain.ActionReceipt{ActDigest: digest},
		}
	}

	c := newStaleComparer(true)
	c.compareActions(1, 7, []chain.ActionTrace{act("0102", "aa")}, []chain.ActionTrace{act("", "bb")})
	if c.report.Differences[DiffActionMismatch] != 0 || c.report.StaleEmptyActions["eosmechanics::cpu"] != 1 {
		t.Errorf("stale store data: want no difference and one counted action, got %v %v", c.report.Differences, c.report.StaleEmptyActions)
	}

	c = newStaleComparer(true)
	c.compareActions(1, 7, []chain.ActionTrace{act("", "bb")}, []chain.ActionTrace{act("0102", "aa")})
	if c.report.Differences[DiffActionMismatch] != 1 {
		t.Errorf("empty store data over compare data: want one difference, got %v", c.report.Differences)
	}

	c = newStaleComparer(false)
	c.compareActions(1, 7, []chain.ActionTrace{act("0102", "aa")}, []chain.ActionTrace{act("", "bb")})
	if c.report.Differences[DiffActionMismatch] != 1 {
		t.Errorf("flag off: want one difference, got %v", c.report.Differences)
	}

	ta := &corereader.TransactionData{Actions: []chain.ActionTrace{act("0102", "aa"), act("03", "cc")}}
	tb := &corereader.TransactionData{Actions: []chain.ActionTrace{act("", "bb"), act("04", "dd")}}
	got := withoutStaleData(ta, tb)
	if got.Actions[0].Act.Data != "" || got.Actions[0].Receipt.ActDigest != "bb" || got.Actions[1].Act.Data != "03" || ta.Actions[0].Act.Data != "0102" {
		t.Errorf("withoutStaleData: got %+v", got.Actions)
	}
}
