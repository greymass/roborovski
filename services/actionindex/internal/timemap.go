package internal

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const (
	antelopeEpochUnix     = 946684800 // 2000-01-01 00:00:00 UTC
	antelopeBlockInterval = 500       // milliseconds per block slot
)

type SeqRange struct {
	MinSeq uint64
	MaxSeq uint64
}

type TimeMapper struct {
	mu           sync.RWMutex
	hourlyRanges map[uint32]SeqRange
	dirty        map[uint32]struct{}
}

func NewTimeMapper() *TimeMapper {
	return &TimeMapper{
		hourlyRanges: make(map[uint32]SeqRange),
		dirty:        make(map[uint32]struct{}),
	}
}

// BlockTimeToHour converts an Antelope block time to a Unix epoch hour number.
//
// IMPORTANT: Antelope block times are NOT Unix timestamps. They are counts of
// 500ms intervals since 2000-01-01 00:00:00 UTC (the Antelope epoch).
//
// To convert to Unix seconds: antelopeEpochUnix + (blockTime * 500 / 1000)
//
// This function returns the hour number since Unix epoch (1970-01-01), which
// is used consistently throughout the time mapping system for date range queries.
func BlockTimeToHour(blockTime uint32) uint32 {
	unixSeconds := antelopeEpochUnix + uint64(blockTime)*antelopeBlockInterval/1000
	return uint32(unixSeconds / 3600)
}

func (t *TimeMapper) Record(blockTime uint32, minSeq, maxSeq uint64) {
	if minSeq == 0 && maxSeq == 0 {
		return
	}

	hour := BlockTimeToHour(blockTime)

	t.mu.Lock()
	defer t.mu.Unlock()

	existing := t.hourlyRanges[hour]

	if existing.MinSeq == 0 || minSeq < existing.MinSeq {
		existing.MinSeq = minSeq
		t.dirty[hour] = struct{}{}
	}
	if maxSeq > existing.MaxSeq {
		existing.MaxSeq = maxSeq
		t.dirty[hour] = struct{}{}
	}

	t.hourlyRanges[hour] = existing
}

func (t *TimeMapper) GetSeqRangeForTime(startTime, endTime time.Time) (minSeq, maxSeq uint64) {
	startHour := uint32(startTime.Unix() / 3600)
	endHour := uint32(endTime.Unix() / 3600)

	t.mu.RLock()
	defer t.mu.RUnlock()

	for hour := startHour; hour <= endHour; hour++ {
		r, ok := t.hourlyRanges[hour]
		if !ok {
			continue
		}
		if minSeq == 0 || r.MinSeq < minSeq {
			minSeq = r.MinSeq
		}
		if r.MaxSeq > maxSeq {
			maxSeq = r.MaxSeq
		}
	}

	return minSeq, maxSeq
}

func (t *TimeMapper) GetSeqRangeForHours(startHour, endHour uint32) (minSeq, maxSeq uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for hour := startHour; hour <= endHour; hour++ {
		r, ok := t.hourlyRanges[hour]
		if !ok {
			continue
		}
		if minSeq == 0 || r.MinSeq < minSeq {
			minSeq = r.MinSeq
		}
		if r.MaxSeq > maxSeq {
			maxSeq = r.MaxSeq
		}
	}

	return minSeq, maxSeq
}

func (t *TimeMapper) FlushToBatch(batch *pebble.Batch) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for hour := range t.dirty {
		r := t.hourlyRanges[hour]
		key := makeTimeMapKey(hour)
		val := makeTimeMapValue(r.MinSeq, r.MaxSeq)
		if err := batch.Set(key, val, nil); err != nil {
			return err
		}
	}

	clear(t.dirty)
	return nil
}

func (t *TimeMapper) LoadFromDB(db *pebble.DB) error {
	prefix := []byte{PrefixTimeMap}
	upperBound := []byte{PrefixTimeMap + 1}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	t.mu.Lock()
	defer t.mu.Unlock()

	for iter.First(); iter.Valid(); iter.Next() {
		hour, ok := parseTimeMapKey(iter.Key())
		if !ok {
			continue
		}

		minSeq, maxSeq, ok := parseTimeMapValue(iter.Value())
		if !ok {
			continue
		}

		t.hourlyRanges[hour] = SeqRange{
			MinSeq: minSeq,
			MaxSeq: maxSeq,
		}
	}

	return iter.Error()
}

func (t *TimeMapper) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.hourlyRanges)
}

func (t *TimeMapper) DirtyCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.dirty)
}

func (t *TimeMapper) HourRange() (minHour, maxHour uint32) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for hour := range t.hourlyRanges {
		if minHour == 0 || hour < minHour {
			minHour = hour
		}
		if hour > maxHour {
			maxHour = hour
		}
	}
	return minHour, maxHour
}

func (t *TimeMapper) MaxSeq() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var maxSeq uint64
	for _, r := range t.hourlyRanges {
		if r.MaxSeq > maxSeq {
			maxSeq = r.MaxSeq
		}
	}
	return maxSeq
}

func (t *TimeMapper) SeqToHour(seq uint64) (uint32, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for hour, r := range t.hourlyRanges {
		if seq >= r.MinSeq && seq <= r.MaxSeq {
			return hour, true
		}
	}
	return 0, false
}
