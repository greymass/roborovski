package internal

import (
	"encoding/binary"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/logger"
)

type ChunkReader struct {
	db        *pebble.DB
	metadata  *ChunkMetadata
	walReader *WALReader
}

type seqRangeFilter struct {
	minSeq, maxSeq uint64
	hasRange       bool
}

func newSeqRangeFilter(minSeq, maxSeq uint64) seqRangeFilter {
	return seqRangeFilter{
		minSeq:   minSeq,
		maxSeq:   maxSeq,
		hasRange: minSeq > 0 || maxSeq > 0,
	}
}

func (f seqRangeFilter) contains(seq uint64) bool {
	if !f.hasRange {
		return true
	}
	if f.minSeq > 0 && seq < f.minSeq {
		return false
	}
	if f.maxSeq > 0 && seq > f.maxSeq {
		return false
	}
	return true
}

func (f seqRangeFilter) belowMin(seq uint64) bool {
	return f.hasRange && f.minSeq > 0 && seq < f.minSeq
}

func (f seqRangeFilter) aboveMax(seq uint64) bool {
	return f.hasRange && f.maxSeq > 0 && seq > f.maxSeq
}

func (f seqRangeFilter) chunkBelowMin(baseSeq uint64, chunkLen int) bool {
	return f.hasRange && f.minSeq > 0 && baseSeq+uint64(chunkLen) < f.minSeq
}

func (f seqRangeFilter) chunkAboveMax(baseSeq uint64) bool {
	return f.hasRange && f.maxSeq > 0 && baseSeq > f.maxSeq
}

func NewChunkReader(db *pebble.DB, metadata *ChunkMetadata, walReader *WALReader) *ChunkReader {
	return &ChunkReader{
		db:        db,
		metadata:  metadata,
		walReader: walReader,
	}
}

func (r *ChunkReader) GetLastN(account uint64, n int) ([]uint64, error) {
	t0 := time.Now()
	chunkCount := r.metadata.GetAllActionsChunkCount(account)
	t1 := time.Now()
	if chunkCount == 0 {
		if r.walReader != nil {
			seqs, err := r.walReader.GetEntriesForAccount(account)
			logger.Printf("debug-perf", "[GetLastN] account=%d chunkCount=0 walScan=%v", account, time.Since(t1))
			return seqs, err
		}
		return nil, nil
	}

	_, lastBase := r.metadata.GetAllActionsSeqRange(account)
	t2 := time.Now()

	prefix := makeAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	t3 := time.Now()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	seekKey := makeAccountActionsKey(account, lastBase)
	found := iter.SeekGE(seekKey)
	t4 := time.Now()

	for ; iter.Valid() && len(seqs) < n; iter.Prev() {
		_, baseSeq, ok := parseAccountActionsKey(iter.Key())
		if !ok {
			continue
		}
		chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
		if err != nil {
			continue
		}

		needed := n - len(seqs)
		start := len(chunk.Seqs) - needed
		if start < 0 {
			start = 0
		}
		seqs = append(chunk.Seqs[start:], seqs...)
	}
	t5 := time.Now()

	if r.walReader != nil {
		ws, err := r.walReader.GetEntriesForAccount(account)
		if err == nil && len(ws) > 0 {
			seqs = mergeAndDedupe(seqs, ws)
			if len(seqs) > n {
				seqs = seqs[len(seqs)-n:]
			}
		}
	}
	t6 := time.Now()

	logger.Printf("debug-perf", "[GetLastN] account=%d meta=%v seqRange=%v newIter=%v seekGE=%v (found=%v) loop=%v wal=%v total=%v",
		account, t1.Sub(t0), t2.Sub(t1), t3.Sub(t2), t4.Sub(t3), found, t5.Sub(t4), t6.Sub(t5), t6.Sub(t0))

	return seqs, iter.Error()
}

func (r *ChunkReader) GetFirstN(account uint64, n int) ([]uint64, error) {
	chunkCount := r.metadata.GetAllActionsChunkCount(account)
	if chunkCount == 0 {
		if r.walReader != nil {
			return r.walReader.GetEntriesForAccount(account)
		}
		return nil, nil
	}

	firstBase, _ := r.metadata.GetAllActionsSeqRange(account)

	prefix := makeAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	seekKey := makeAccountActionsKey(account, firstBase)
	for iter.SeekGE(seekKey); iter.Valid() && len(seqs) < n; iter.Next() {
		_, baseSeq, ok := parseAccountActionsKey(iter.Key())
		if !ok {
			continue
		}
		chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
		if err != nil {
			continue
		}

		needed := n - len(seqs)
		end := needed
		if end > len(chunk.Seqs) {
			end = len(chunk.Seqs)
		}
		seqs = append(seqs, chunk.Seqs[:end]...)
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForAccount(account)
		if err == nil && len(walSeqs) > 0 {
			seqs = mergeAndDedupe(seqs, walSeqs)
			if len(seqs) > n {
				seqs = seqs[:n]
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetRange(account uint64, pos, count int) ([]uint64, error) {
	if pos < 0 {
		return r.GetLastN(account, count)
	}

	startChunk := uint32(pos / ChunkSize)
	offsetInChunk := pos % ChunkSize

	key := makeLegacyAccountActionsKey(account, startChunk)
	prefix := makeLegacyAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	iter.SeekGE(key)
	first := true

	for iter.Valid() && len(seqs) < count {
		chunk, err := DecodeChunk(iter.Value())
		if err != nil {
			iter.Next()
			continue
		}

		startIdx := 0
		if first {
			startIdx = offsetInChunk
			first = false
		}

		for i := startIdx; i < len(chunk.Seqs) && len(seqs) < count; i++ {
			seqs = append(seqs, chunk.Seqs[i])
		}

		iter.Next()
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetFromCursor(account uint64, cursorSeq uint64, limit int, descending bool) ([]uint64, error) {
	return r.GetFromCursorWithSeqRange(account, cursorSeq, 0, 0, limit, descending)
}

func (r *ChunkReader) GetFromCursorWithSeqRange(account uint64, cursorSeq uint64, minSeq, maxSeq uint64, limit int, descending bool) ([]uint64, error) {
	prefix := makeAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seekKey := makeAccountActionsKey(account, cursorSeq)

	if !iter.SeekGE(seekKey) {
		if !iter.Last() {
			return nil, iter.Error()
		}
	}

	_, baseSeq, ok := parseAccountActionsKey(iter.Key())
	if !ok {
		return nil, nil
	}

	if baseSeq > cursorSeq {
		if !iter.Prev() {
			return nil, iter.Error()
		}
		_, baseSeq, ok = parseAccountActionsKey(iter.Key())
		if !ok {
			return nil, nil
		}
	}

	chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
	if err != nil {
		return nil, err
	}

	cursorIdx := findSeqInChunk(chunk.Seqs, cursorSeq)
	var seqs []uint64
	filter := newSeqRangeFilter(minSeq, maxSeq)

	if descending {
		for i := cursorIdx - 1; i >= 0 && len(seqs) < limit; i-- {
			seq := chunk.Seqs[i]
			if filter.belowMin(seq) {
				return seqs, nil
			}
			if filter.contains(seq) {
				seqs = append(seqs, seq)
			}
		}
		for iter.Prev(); iter.Valid() && len(seqs) < limit; iter.Prev() {
			_, baseSeq, ok := parseAccountActionsKey(iter.Key())
			if !ok {
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				continue
			}
			if filter.chunkBelowMin(chunk.BaseSeq, len(chunk.Seqs)) {
				break
			}
			for i := len(chunk.Seqs) - 1; i >= 0 && len(seqs) < limit; i-- {
				seq := chunk.Seqs[i]
				if filter.belowMin(seq) {
					return seqs, nil
				}
				if filter.contains(seq) {
					seqs = append(seqs, seq)
				}
			}
		}
	} else {
		for i := cursorIdx + 1; i < len(chunk.Seqs) && len(seqs) < limit; i++ {
			seq := chunk.Seqs[i]
			if filter.aboveMax(seq) {
				return seqs, nil
			}
			if filter.contains(seq) {
				seqs = append(seqs, seq)
			}
		}
		for iter.Next(); iter.Valid() && len(seqs) < limit; iter.Next() {
			_, baseSeq, ok := parseAccountActionsKey(iter.Key())
			if !ok {
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				continue
			}
			if filter.chunkAboveMax(chunk.BaseSeq) {
				break
			}
			for i := 0; i < len(chunk.Seqs) && len(seqs) < limit; i++ {
				seq := chunk.Seqs[i]
				if filter.aboveMax(seq) {
					return seqs, nil
				}
				if filter.contains(seq) {
					seqs = append(seqs, seq)
				}
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractActionLastN(account, contract, action uint64, n int) ([]uint64, error) {
	prefix := makeContractActionPrefix(account, contract, action)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	for iter.Last(); iter.Valid() && len(seqs) < n; iter.Prev() {
		_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
		if !ok {
			continue
		}
		chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
		if err != nil {
			continue
		}

		needed := n - len(seqs)
		start := len(chunk.Seqs) - needed
		if start < 0 {
			start = 0
		}
		seqs = append(chunk.Seqs[start:], seqs...)
	}

	if r.walReader != nil {
		ws, err := r.walReader.GetEntriesForContractAction(account, contract, action)
		if err == nil && len(ws) > 0 {
			seqs = mergeAndDedupe(seqs, ws)
			if len(seqs) > n {
				seqs = seqs[len(seqs)-n:]
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractActionFirstN(account, contract, action uint64, n int) ([]uint64, error) {
	prefix := makeContractActionPrefix(account, contract, action)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	for iter.First(); iter.Valid() && len(seqs) < n; iter.Next() {
		_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
		if !ok {
			continue
		}
		chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
		if err != nil {
			continue
		}

		needed := n - len(seqs)
		end := needed
		if end > len(chunk.Seqs) {
			end = len(chunk.Seqs)
		}
		seqs = append(seqs, chunk.Seqs[:end]...)
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForContractAction(account, contract, action)
		if err == nil && len(walSeqs) > 0 {
			seqs = mergeAndDedupe(seqs, walSeqs)
			if len(seqs) > n {
				seqs = seqs[:n]
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractWildcardLastN(account, contract uint64, n int) ([]uint64, error) {
	prefix := makeContractWildcardPrefix(account, contract)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	for iter.Last(); iter.Valid() && len(seqs) < n; iter.Prev() {
		_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
		if !ok {
			continue
		}
		chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
		if err != nil {
			continue
		}

		needed := n - len(seqs)
		start := len(chunk.Seqs) - needed
		if start < 0 {
			start = 0
		}
		seqs = append(chunk.Seqs[start:], seqs...)
	}

	if r.walReader != nil {
		ws, err := r.walReader.GetEntriesForContractWildcard(account, contract)
		if err == nil && len(ws) > 0 {
			seqs = mergeAndDedupe(seqs, ws)
			if len(seqs) > n {
				seqs = seqs[len(seqs)-n:]
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractWildcardFirstN(account, contract uint64, n int) ([]uint64, error) {
	prefix := makeContractWildcardPrefix(account, contract)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	for iter.First(); iter.Valid() && len(seqs) < n; iter.Next() {
		_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
		if !ok {
			continue
		}
		chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
		if err != nil {
			continue
		}

		needed := n - len(seqs)
		end := needed
		if end > len(chunk.Seqs) {
			end = len(chunk.Seqs)
		}
		seqs = append(seqs, chunk.Seqs[:end]...)
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForContractWildcard(account, contract)
		if err == nil && len(walSeqs) > 0 {
			seqs = mergeAndDedupe(seqs, walSeqs)
			if len(seqs) > n {
				seqs = seqs[:n]
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractActionFromCursor(account, contract, action, cursorSeq uint64, limit int, descending bool) ([]uint64, error) {
	return r.GetContractActionFromCursorWithSeqRange(account, contract, action, cursorSeq, 0, 0, limit, descending)
}

func (r *ChunkReader) GetContractActionFromCursorWithSeqRange(account, contract, action, cursorSeq uint64, minSeq, maxSeq uint64, limit int, descending bool) ([]uint64, error) {
	prefix := makeContractActionPrefix(account, contract, action)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seekKey := makeContractActionKey(account, contract, action, cursorSeq)

	if !iter.SeekGE(seekKey) {
		if !iter.Last() {
			return nil, iter.Error()
		}
	}

	_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
	if !ok {
		return nil, nil
	}

	if baseSeq > cursorSeq {
		if !iter.Prev() {
			return nil, iter.Error()
		}
		_, _, _, baseSeq, ok = parseContractActionKey(iter.Key())
		if !ok {
			return nil, nil
		}
	}

	chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
	if err != nil {
		return nil, err
	}

	cursorIdx := findSeqInChunk(chunk.Seqs, cursorSeq)
	var seqs []uint64
	filter := newSeqRangeFilter(minSeq, maxSeq)

	if descending {
		for i := cursorIdx - 1; i >= 0 && len(seqs) < limit; i-- {
			seq := chunk.Seqs[i]
			if filter.belowMin(seq) {
				return seqs, nil
			}
			if filter.contains(seq) {
				seqs = append(seqs, seq)
			}
		}
		for iter.Prev(); iter.Valid() && len(seqs) < limit; iter.Prev() {
			_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
			if !ok {
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				continue
			}
			if filter.chunkBelowMin(chunk.BaseSeq, len(chunk.Seqs)) {
				break
			}
			for i := len(chunk.Seqs) - 1; i >= 0 && len(seqs) < limit; i-- {
				seq := chunk.Seqs[i]
				if filter.belowMin(seq) {
					return seqs, nil
				}
				if filter.contains(seq) {
					seqs = append(seqs, seq)
				}
			}
		}
	} else {
		for i := cursorIdx + 1; i < len(chunk.Seqs) && len(seqs) < limit; i++ {
			seq := chunk.Seqs[i]
			if filter.aboveMax(seq) {
				return seqs, nil
			}
			if filter.contains(seq) {
				seqs = append(seqs, seq)
			}
		}
		for iter.Next(); iter.Valid() && len(seqs) < limit; iter.Next() {
			_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
			if !ok {
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				continue
			}
			if filter.chunkAboveMax(chunk.BaseSeq) {
				break
			}
			for i := 0; i < len(chunk.Seqs) && len(seqs) < limit; i++ {
				seq := chunk.Seqs[i]
				if filter.aboveMax(seq) {
					return seqs, nil
				}
				if filter.contains(seq) {
					seqs = append(seqs, seq)
				}
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractWildcardFromCursor(account, contract, cursorSeq uint64, limit int, descending bool) ([]uint64, error) {
	return r.GetContractWildcardFromCursorWithSeqRange(account, contract, cursorSeq, 0, 0, limit, descending)
}

func (r *ChunkReader) GetContractWildcardFromCursorWithSeqRange(account, contract, cursorSeq uint64, minSeq, maxSeq uint64, limit int, descending bool) ([]uint64, error) {
	prefix := makeContractWildcardPrefix(account, contract)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seekKey := makeContractWildcardKey(account, contract, cursorSeq)

	if !iter.SeekGE(seekKey) {
		if !iter.Last() {
			return nil, iter.Error()
		}
	}

	_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
	if !ok {
		return nil, nil
	}

	if baseSeq > cursorSeq {
		if !iter.Prev() {
			return nil, iter.Error()
		}
		_, _, baseSeq, ok = parseContractWildcardKey(iter.Key())
		if !ok {
			return nil, nil
		}
	}

	chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
	if err != nil {
		return nil, err
	}

	cursorIdx := findSeqInChunk(chunk.Seqs, cursorSeq)
	var seqs []uint64
	filter := newSeqRangeFilter(minSeq, maxSeq)

	if descending {
		for i := cursorIdx - 1; i >= 0 && len(seqs) < limit; i-- {
			seq := chunk.Seqs[i]
			if filter.belowMin(seq) {
				return seqs, nil
			}
			if filter.contains(seq) {
				seqs = append(seqs, seq)
			}
		}
		for iter.Prev(); iter.Valid() && len(seqs) < limit; iter.Prev() {
			_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
			if !ok {
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				continue
			}
			if filter.chunkBelowMin(chunk.BaseSeq, len(chunk.Seqs)) {
				break
			}
			for i := len(chunk.Seqs) - 1; i >= 0 && len(seqs) < limit; i-- {
				seq := chunk.Seqs[i]
				if filter.belowMin(seq) {
					return seqs, nil
				}
				if filter.contains(seq) {
					seqs = append(seqs, seq)
				}
			}
		}
	} else {
		for i := cursorIdx + 1; i < len(chunk.Seqs) && len(seqs) < limit; i++ {
			seq := chunk.Seqs[i]
			if filter.aboveMax(seq) {
				return seqs, nil
			}
			if filter.contains(seq) {
				seqs = append(seqs, seq)
			}
		}
		for iter.Next(); iter.Valid() && len(seqs) < limit; iter.Next() {
			_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
			if !ok {
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				continue
			}
			if filter.chunkAboveMax(chunk.BaseSeq) {
				break
			}
			for i := 0; i < len(chunk.Seqs) && len(seqs) < limit; i++ {
				seq := chunk.Seqs[i]
				if filter.aboveMax(seq) {
					return seqs, nil
				}
				if filter.contains(seq) {
					seqs = append(seqs, seq)
				}
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetWithSeqRange(account uint64, minSeq, maxSeq uint64, limit int, descending bool) ([]uint64, error) {
	prefix := makeAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	if descending {
		seekKey := makeAccountActionsKey(account, maxSeq)
		if !iter.SeekGE(seekKey) {
			if !iter.Last() {
				return seqs, nil
			}
		}
		_, baseSeq, ok := parseAccountActionsKey(iter.Key())
		if ok && baseSeq > maxSeq {
			if !iter.Prev() {
				return seqs, nil
			}
		}

		for iter.Valid() && len(seqs) < limit {
			_, baseSeq, ok := parseAccountActionsKey(iter.Key())
			if !ok {
				iter.Prev()
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				iter.Prev()
				continue
			}
			if len(chunk.Seqs) > 0 && chunk.Seqs[len(chunk.Seqs)-1] < minSeq {
				break
			}
			for i := len(chunk.Seqs) - 1; i >= 0 && len(seqs) < limit; i-- {
				seq := chunk.Seqs[i]
				if seq > maxSeq {
					continue
				}
				if seq < minSeq {
					break
				}
				seqs = append(seqs, seq)
			}
			iter.Prev()
		}
	} else {
		seekKey := makeAccountActionsKey(account, minSeq)
		if !iter.SeekGE(seekKey) {
			if !iter.First() {
				return seqs, nil
			}
		}
		_, baseSeq, ok := parseAccountActionsKey(iter.Key())
		if ok && baseSeq > minSeq && iter.Prev() {
			_, baseSeq, _ = parseAccountActionsKey(iter.Key())
		} else if !ok {
			iter.First()
		}

		for iter.Valid() && len(seqs) < limit {
			_, baseSeq, ok := parseAccountActionsKey(iter.Key())
			if !ok {
				iter.Next()
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				iter.Next()
				continue
			}
			if chunk.BaseSeq > maxSeq {
				break
			}
			for _, seq := range chunk.Seqs {
				if seq > maxSeq {
					break
				}
				if seq >= minSeq && len(seqs) < limit {
					seqs = append(seqs, seq)
				}
			}
			iter.Next()
		}
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForAccount(account)
		if err == nil && len(walSeqs) > 0 {
			var filtered []uint64
			for _, seq := range walSeqs {
				if seq >= minSeq && seq <= maxSeq {
					filtered = append(filtered, seq)
				}
			}
			if len(filtered) > 0 {
				seqs = mergeAndDedupe(seqs, filtered)
				if descending {
					sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
				}
				if len(seqs) > limit {
					seqs = seqs[:limit]
				}
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractActionWithSeqRange(account, contract, action uint64, minSeq, maxSeq uint64, limit int, descending bool) ([]uint64, error) {
	prefix := makeContractActionPrefix(account, contract, action)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	if descending {
		seekKey := makeContractActionKey(account, contract, action, maxSeq)
		if !iter.SeekGE(seekKey) {
			if !iter.Last() {
				return seqs, nil
			}
		}
		_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
		if ok && baseSeq > maxSeq {
			if !iter.Prev() {
				return seqs, nil
			}
		}

		for iter.Valid() && len(seqs) < limit {
			_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
			if !ok {
				iter.Prev()
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				iter.Prev()
				continue
			}
			if len(chunk.Seqs) > 0 && chunk.Seqs[len(chunk.Seqs)-1] < minSeq {
				break
			}
			for i := len(chunk.Seqs) - 1; i >= 0 && len(seqs) < limit; i-- {
				seq := chunk.Seqs[i]
				if seq > maxSeq {
					continue
				}
				if seq < minSeq {
					break
				}
				seqs = append(seqs, seq)
			}
			iter.Prev()
		}
	} else {
		seekKey := makeContractActionKey(account, contract, action, minSeq)
		if !iter.SeekGE(seekKey) {
			if !iter.First() {
				return seqs, nil
			}
		}
		_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
		if ok && baseSeq > minSeq && iter.Prev() {
			_, _, _, baseSeq, _ = parseContractActionKey(iter.Key())
		} else if !ok {
			iter.First()
		}

		for iter.Valid() && len(seqs) < limit {
			_, _, _, baseSeq, ok := parseContractActionKey(iter.Key())
			if !ok {
				iter.Next()
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				iter.Next()
				continue
			}
			if chunk.BaseSeq > maxSeq {
				break
			}
			for _, seq := range chunk.Seqs {
				if seq > maxSeq {
					break
				}
				if seq >= minSeq && len(seqs) < limit {
					seqs = append(seqs, seq)
				}
			}
			iter.Next()
		}
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForContractAction(account, contract, action)
		if err == nil && len(walSeqs) > 0 {
			var filtered []uint64
			for _, seq := range walSeqs {
				if seq >= minSeq && seq <= maxSeq {
					filtered = append(filtered, seq)
				}
			}
			if len(filtered) > 0 {
				seqs = mergeAndDedupe(seqs, filtered)
				if descending {
					sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
				}
				if len(seqs) > limit {
					seqs = seqs[:limit]
				}
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetContractWildcardWithSeqRange(account, contract uint64, minSeq, maxSeq uint64, limit int, descending bool) ([]uint64, error) {
	prefix := makeContractWildcardPrefix(account, contract)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seqs []uint64

	if descending {
		seekKey := makeContractWildcardKey(account, contract, maxSeq)
		if !iter.SeekGE(seekKey) {
			if !iter.Last() {
				return seqs, nil
			}
		}
		_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
		if ok && baseSeq > maxSeq {
			if !iter.Prev() {
				return seqs, nil
			}
		}

		for iter.Valid() && len(seqs) < limit {
			_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
			if !ok {
				iter.Prev()
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				iter.Prev()
				continue
			}
			if len(chunk.Seqs) > 0 && chunk.Seqs[len(chunk.Seqs)-1] < minSeq {
				break
			}
			for i := len(chunk.Seqs) - 1; i >= 0 && len(seqs) < limit; i-- {
				seq := chunk.Seqs[i]
				if seq > maxSeq {
					continue
				}
				if seq < minSeq {
					break
				}
				seqs = append(seqs, seq)
			}
			iter.Prev()
		}
	} else {
		seekKey := makeContractWildcardKey(account, contract, minSeq)
		if !iter.SeekGE(seekKey) {
			if !iter.First() {
				return seqs, nil
			}
		}
		_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
		if ok && baseSeq > minSeq && iter.Prev() {
			_, _, baseSeq, _ = parseContractWildcardKey(iter.Key())
		} else if !ok {
			iter.First()
		}

		for iter.Valid() && len(seqs) < limit {
			_, _, baseSeq, ok := parseContractWildcardKey(iter.Key())
			if !ok {
				iter.Next()
				continue
			}
			chunk, err := DecodeLeanChunk(baseSeq, iter.Value())
			if err != nil {
				iter.Next()
				continue
			}
			if chunk.BaseSeq > maxSeq {
				break
			}
			for _, seq := range chunk.Seqs {
				if seq > maxSeq {
					break
				}
				if seq >= minSeq && len(seqs) < limit {
					seqs = append(seqs, seq)
				}
			}
			iter.Next()
		}
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForContractWildcard(account, contract)
		if err == nil && len(walSeqs) > 0 {
			var filtered []uint64
			for _, seq := range walSeqs {
				if seq >= minSeq && seq <= maxSeq {
					filtered = append(filtered, seq)
				}
			}
			if len(filtered) > 0 {
				seqs = mergeAndDedupe(seqs, filtered)
				if descending {
					sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
				}
				if len(seqs) > limit {
					seqs = seqs[:limit]
				}
			}
		}
	}

	return seqs, iter.Error()
}

func (r *ChunkReader) GetTotalCount(account uint64) (int, error) {
	prefix := makeAccountActionsPrefix(account)
	upperBound := incrementPrefix(prefix)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var total int
	for iter.First(); iter.Valid(); iter.Next() {
		val := iter.Value()
		if len(val) >= LeanChunkHeaderSize {
			count := binary.LittleEndian.Uint32(val[0:4])
			total += int(count)
		}
	}

	if err := iter.Error(); err != nil {
		return 0, err
	}

	if r.walReader != nil {
		walSeqs, err := r.walReader.GetEntriesForAccount(account)
		if err != nil {
			return 0, err
		}
		total += len(walSeqs)
	}

	return total, nil
}

func incrementPrefix(prefix []byte) []byte {
	result := make([]byte, len(prefix))
	copy(result, prefix)

	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 0xFF {
			result[i]++
			return result
		}
		result[i] = 0
	}

	return append(result, 0)
}

func findSeqInChunk(seqs []uint64, target uint64) int {
	idx := sort.Search(len(seqs), func(i int) bool {
		return seqs[i] >= target
	})

	if idx < len(seqs) && seqs[idx] == target {
		return idx
	}

	if idx > 0 {
		return idx - 1
	}

	return 0
}

func mergeAndDedupe(a, b []uint64) []uint64 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	seen := make(map[uint64]struct{}, len(a)+len(b))
	result := make([]uint64, 0, len(a)+len(b))

	for _, v := range a {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	for _, v := range b {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })

	return result
}
