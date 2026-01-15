package internal

import (
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestTimeMapper_Record(t *testing.T) {
	tm := NewTimeMapper()

	tm.Record(1593561600, 100, 200)

	if tm.Len() != 1 {
		t.Errorf("len: got %d, want 1", tm.Len())
	}

	minSeq, maxSeq := tm.GetSeqRangeForHours(484296, 484296)
	if minSeq != 100 || maxSeq != 200 {
		t.Errorf("range: got %d-%d, want 100-200", minSeq, maxSeq)
	}
}

func TestTimeMapper_RecordMerges(t *testing.T) {
	tm := NewTimeMapper()

	tm.Record(1593561600, 100, 200)
	tm.Record(1593565200, 150, 300)
	tm.Record(1593568800, 50, 100)

	if tm.Len() != 2 {
		t.Errorf("len: got %d, want 2 (first two same hour, third is next hour)", tm.Len())
	}

	minSeq, maxSeq := tm.GetSeqRangeForHours(484296, 484296)
	if minSeq != 100 || maxSeq != 300 {
		t.Errorf("hour 484296 range: got %d-%d, want 100-300", minSeq, maxSeq)
	}

	minSeq, maxSeq = tm.GetSeqRangeForHours(484297, 484297)
	if minSeq != 50 || maxSeq != 100 {
		t.Errorf("hour 484297 range: got %d-%d, want 50-100", minSeq, maxSeq)
	}
}

func TestTimeMapper_RecordLowerMin(t *testing.T) {
	tm := NewTimeMapper()

	tm.Record(1593561600, 200, 300)
	tm.Record(1593565200, 100, 250)

	minSeq, maxSeq := tm.GetSeqRangeForHours(484296, 484296)
	if minSeq != 100 || maxSeq != 300 {
		t.Errorf("range: got %d-%d, want 100-300", minSeq, maxSeq)
	}
}

func TestTimeMapper_GetSeqRangeForTime(t *testing.T) {
	tm := NewTimeMapper()

	tm.Record(1593561600, 100, 200)
	tm.Record(1593568800, 300, 400)
	tm.Record(1593576000, 500, 600)

	startTime := time.Unix(1743465600, 0)
	endTime := time.Unix(1743469200, 0)
	minSeq, maxSeq := tm.GetSeqRangeForTime(startTime, endTime)

	if minSeq != 100 || maxSeq != 400 {
		t.Errorf("range: got %d-%d, want 100-400", minSeq, maxSeq)
	}
}

func TestTimeMapper_GetSeqRangeForHours_Missing(t *testing.T) {
	tm := NewTimeMapper()

	tm.Record(1593561600, 100, 200)

	minSeq, maxSeq := tm.GetSeqRangeForHours(100, 200)
	if minSeq != 0 || maxSeq != 0 {
		t.Errorf("missing hours should return 0-0, got %d-%d", minSeq, maxSeq)
	}
}

func TestTimeMapper_RecordZeroSeqs(t *testing.T) {
	tm := NewTimeMapper()

	tm.Record(1593561600, 0, 0)

	if tm.Len() != 0 {
		t.Errorf("recording 0-0 should not create entry, len: %d", tm.Len())
	}
}

func TestTimeMapper_DirtyTracking(t *testing.T) {
	tm := NewTimeMapper()

	if tm.DirtyCount() != 0 {
		t.Errorf("new mapper should have 0 dirty, got %d", tm.DirtyCount())
	}

	tm.Record(1593561600, 100, 200)

	if tm.DirtyCount() != 1 {
		t.Errorf("after record: got %d dirty, want 1", tm.DirtyCount())
	}

	tm.Record(1593565200, 150, 250)

	if tm.DirtyCount() != 1 {
		t.Errorf("same hour should still be 1 dirty, got %d", tm.DirtyCount())
	}

	tm.Record(1593568800, 300, 400)

	if tm.DirtyCount() != 2 {
		t.Errorf("new hour should be 2 dirty, got %d", tm.DirtyCount())
	}
}

func TestTimeMapper_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "timemap-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}

	tm1 := NewTimeMapper()
	tm1.Record(1593561600, 100, 200)
	tm1.Record(1593568800, 300, 400)
	tm1.Record(1593576000, 500, 600)

	batch := db.NewBatch()
	if err := tm1.FlushToBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}

	if tm1.DirtyCount() != 0 {
		t.Errorf("after flush, dirty should be 0, got %d", tm1.DirtyCount())
	}

	tm2 := NewTimeMapper()
	if err := tm2.LoadFromDB(db); err != nil {
		t.Fatal(err)
	}

	if tm2.Len() != 3 {
		t.Errorf("loaded len: got %d, want 3", tm2.Len())
	}

	minSeq, maxSeq := tm2.GetSeqRangeForHours(484296, 484298)
	if minSeq != 100 || maxSeq != 600 {
		t.Errorf("loaded range: got %d-%d, want 100-600", minSeq, maxSeq)
	}

	db.Close()
}

func TestBlockTimeToHour(t *testing.T) {
	cases := []struct {
		blockTime uint32
		wantHour  uint32
		desc      string
	}{
		{0, 262968, "Antelope epoch (2000-01-01 00:00:00 UTC)"},
		{7200, 262969, "One hour after Antelope epoch"},
		{1593561600, 484296, "2025-04-01 00:00:00 UTC"},
		{1593568800, 484297, "2025-04-01 01:00:00 UTC"},
		{1593576000, 484298, "2025-04-01 02:00:00 UTC"},
	}

	for _, tc := range cases {
		got := BlockTimeToHour(tc.blockTime)
		if got != tc.wantHour {
			t.Errorf("BlockTimeToHour(%d) [%s]: got %d, want %d", tc.blockTime, tc.desc, got, tc.wantHour)
		}
	}
}
