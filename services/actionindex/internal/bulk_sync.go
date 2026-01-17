package internal

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/logger"
)

type Entry struct {
	Account   uint64
	Contract  uint64
	Action    uint64
	GlobalSeq uint64
}

type Buffer struct {
	mu sync.Mutex

	bufferA []Entry
	bufferB []Entry
	active  *[]Entry

	merging      atomic.Bool
	mergeDone    chan struct{}
	mergeErr     error
	lastMergeLen int

	db       *pebble.DB
	metadata *ChunkMetadata
}

func NewBuffer(db *pebble.DB, metadata *ChunkMetadata) *Buffer {
	b := &Buffer{
		bufferA:  make([]Entry, 0, 1_000_000),
		bufferB:  make([]Entry, 0, 1_000_000),
		db:       db,
		metadata: metadata,
	}
	b.active = &b.bufferA
	return b
}

func (b *Buffer) Add(entries []ActionEntry) {
	b.mu.Lock()
	for i := range entries {
		e := &entries[i]
		*b.active = append(*b.active, Entry{
			Account:   e.Account,
			Contract:  e.Contract,
			Action:    e.Action,
			GlobalSeq: e.GlobalSeq,
		})
	}
	b.mu.Unlock()
}

func (b *Buffer) Len() int {
	b.mu.Lock()
	n := len(*b.active)
	b.mu.Unlock()
	return n
}

func (b *Buffer) SwapAndMerge() error {
	if err := b.waitForPreviousMerge(); err != nil {
		return err
	}

	b.mu.Lock()
	toMerge := b.active
	if b.active == &b.bufferA {
		b.active = &b.bufferB
	} else {
		b.active = &b.bufferA
	}
	b.mu.Unlock()

	if len(*toMerge) == 0 {
		return nil
	}

	b.merging.Store(true)
	b.mergeDone = make(chan struct{})
	b.lastMergeLen = len(*toMerge)

	go func() {
		defer close(b.mergeDone)
		defer b.merging.Store(false)

		b.mergeErr = b.doMerge(*toMerge)

		*toMerge = (*toMerge)[:0]
	}()

	return nil
}

func (b *Buffer) SwapAndMergeSync() (MergeStats, error) {
	if err := b.waitForPreviousMerge(); err != nil {
		return MergeStats{}, err
	}

	b.mu.Lock()
	toMerge := b.active
	if b.active == &b.bufferA {
		b.active = &b.bufferB
	} else {
		b.active = &b.bufferA
	}
	b.mu.Unlock()

	if len(*toMerge) == 0 {
		return MergeStats{}, nil
	}

	stats, err := b.doMergeWithStats(*toMerge)

	*toMerge = (*toMerge)[:0]

	return stats, err
}

func (b *Buffer) waitForPreviousMerge() error {
	if b.mergeDone != nil {
		<-b.mergeDone
		b.mergeDone = nil
		if b.mergeErr != nil {
			err := b.mergeErr
			b.mergeErr = nil
			return err
		}
	}
	return nil
}

func (b *Buffer) WaitForMerge() error {
	return b.waitForPreviousMerge()
}

func (b *Buffer) IsMerging() bool {
	return b.merging.Load()
}

func (b *Buffer) doMerge(entries []Entry) error {
	_, err := b.doMergeWithStats(entries)
	return err
}

func (b *Buffer) doMergeWithStats(entries []Entry) (MergeStats, error) {
	merger := &Merger{
		db:       b.db,
		metadata: b.metadata,
	}
	return merger.MergeEntries(entries)
}

func (b *Buffer) Close() error {
	return b.waitForPreviousMerge()
}

type Merger struct {
	db       *pebble.DB
	metadata *ChunkMetadata
}

func (m *Merger) MergeEntries(entries []Entry) (MergeStats, error) {
	var stats MergeStats
	stats.EntriesProcessed = len(entries)

	if len(entries) == 0 {
		return stats, nil
	}

	groups := groupByAccount(entries)

	type result struct {
		chunks []ChunkToWrite
		err    error
	}

	aaChan := make(chan result, 1)
	caChan := make(chan result, 1)
	cwChan := make(chan result, 1)

	go func() {
		chunks, err := m.buildAllActionsFromIndices(entries, groups)
		aaChan <- result{chunks, err}
	}()

	go func() {
		chunks, err := m.buildContractActionFromIndices(entries, groups)
		caChan <- result{chunks, err}
	}()

	go func() {
		chunks, err := m.buildContractWildcardFromIndices(entries, groups)
		cwChan <- result{chunks, err}
	}()

	aaResult := <-aaChan
	if aaResult.err != nil {
		return stats, aaResult.err
	}

	caResult := <-caChan
	if caResult.err != nil {
		return stats, caResult.err
	}

	cwResult := <-cwChan
	if cwResult.err != nil {
		return stats, cwResult.err
	}

	allChunks := make([]ChunkToWrite, 0, len(aaResult.chunks)+len(caResult.chunks)+len(cwResult.chunks))
	allChunks = append(allChunks, aaResult.chunks...)
	allChunks = append(allChunks, caResult.chunks...)
	allChunks = append(allChunks, cwResult.chunks...)

	stats.AllActionsChunks = len(aaResult.chunks)
	stats.ContractActionChunks = len(caResult.chunks)
	stats.ContractWildcardChunks = len(cwResult.chunks)
	stats.ChunksWritten = len(allChunks)

	if err := m.writeChunks(allChunks); err != nil {
		return stats, err
	}

	var allActionsAdded int
	for _, chunk := range allChunks {
		stats.BytesWritten += int64(len(chunk.Encoded))
		if chunk.IndexType == PrefixLegacyAccountActions {
			m.metadata.AddAllActionsChunk(chunk.Account, chunk.BaseSeq)
			allActionsAdded++
		}
	}

	return stats, nil
}

func (m *Merger) writeChunks(chunks []ChunkToWrite) error {
	if len(chunks) == 0 {
		return nil
	}

	batch := m.db.NewBatch()
	defer batch.Close()

	for _, chunk := range chunks {
		key := chunk.MakeKey()
		if err := batch.Set(key, chunk.Encoded, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.NoSync)
}

func (m *Merger) buildAllActionsFromIndices(entries []Entry, groups []accountGroup) ([]ChunkToWrite, error) {
	if len(groups) == 0 {
		return nil, nil
	}

	numWorkers := 8
	if len(groups) < numWorkers {
		numWorkers = len(groups)
	}

	groupChan := make(chan accountGroup, len(groups))
	for _, g := range groups {
		groupChan <- g
	}
	close(groupChan)

	type workerResult struct {
		chunks []ChunkToWrite
	}
	resultChan := make(chan workerResult, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localChunks []ChunkToWrite

			for g := range groupChan {
				chunks := buildAAChunksForAccount(entries, g, m.metadata)
				localChunks = append(localChunks, chunks...)
			}

			resultChan <- workerResult{chunks: localChunks}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var allChunks []ChunkToWrite
	for res := range resultChan {
		allChunks = append(allChunks, res.chunks...)
	}

	return allChunks, nil
}

func (m *Merger) buildContractActionFromIndices(entries []Entry, groups []accountGroup) ([]ChunkToWrite, error) {
	if len(groups) == 0 {
		return nil, nil
	}

	numWorkers := 8
	if len(groups) < numWorkers {
		numWorkers = len(groups)
	}

	groupChan := make(chan accountGroup, len(groups))
	for _, g := range groups {
		groupChan <- g
	}
	close(groupChan)

	type workerResult struct {
		chunks []ChunkToWrite
	}
	resultChan := make(chan workerResult, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localChunks []ChunkToWrite

			for g := range groupChan {
				chunks := buildCAChunksForAccount(entries, g, m.metadata)
				localChunks = append(localChunks, chunks...)
			}

			resultChan <- workerResult{chunks: localChunks}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var allChunks []ChunkToWrite
	for res := range resultChan {
		allChunks = append(allChunks, res.chunks...)
	}

	return allChunks, nil
}

func (m *Merger) buildContractWildcardFromIndices(entries []Entry, groups []accountGroup) ([]ChunkToWrite, error) {
	if len(groups) == 0 {
		return nil, nil
	}

	numWorkers := 8
	if len(groups) < numWorkers {
		numWorkers = len(groups)
	}

	groupChan := make(chan accountGroup, len(groups))
	for _, g := range groups {
		groupChan <- g
	}
	close(groupChan)

	type workerResult struct {
		chunks []ChunkToWrite
	}
	resultChan := make(chan workerResult, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localChunks []ChunkToWrite

			for g := range groupChan {
				chunks := buildCWChunksForAccount(entries, g, m.metadata)
				localChunks = append(localChunks, chunks...)
			}

			resultChan <- workerResult{chunks: localChunks}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var allChunks []ChunkToWrite
	for res := range resultChan {
		allChunks = append(allChunks, res.chunks...)
	}

	return allChunks, nil
}

func buildAAChunksForAccount(entries []Entry, g accountGroup, metadata *ChunkMetadata) []ChunkToWrite {
	sortedIndices := make([]int32, len(g.indices))
	copy(sortedIndices, g.indices)

	sortIndicesByGlobalSeq(sortedIndices, entries)

	var chunks []ChunkToWrite
	var currentSeqs []uint64
	baseChunkID := uint32(metadata.GetAllActionsChunkCountNoLock(g.account))
	localChunkID := uint32(0)

	for _, idx := range sortedIndices {
		currentSeqs = append(currentSeqs, entries[idx].GlobalSeq)

		if len(currentSeqs) >= ChunkSize {
			baseSeq := currentSeqs[0]
			legacyEncoded, _ := EncodeChunk(currentSeqs)
			leanEncoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
			chunks = append(chunks,
				ChunkToWrite{
					IndexType: PrefixLegacyAccountActions,
					Account:   g.account,
					ChunkID:   baseChunkID + localChunkID,
					BaseSeq:   baseSeq,
					Encoded:   legacyEncoded,
				},
				ChunkToWrite{
					IndexType: PrefixAccountActions,
					Account:   g.account,
					BaseSeq:   baseSeq,
					Encoded:   leanEncoded,
				},
			)
			localChunkID++
			currentSeqs = currentSeqs[:0]
		}
	}

	if len(currentSeqs) > 0 {
		baseSeq := currentSeqs[0]
		legacyEncoded, _ := EncodeChunk(currentSeqs)
		leanEncoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
		chunks = append(chunks,
			ChunkToWrite{
				IndexType: PrefixLegacyAccountActions,
				Account:   g.account,
				ChunkID:   baseChunkID + localChunkID,
				BaseSeq:   baseSeq,
				Encoded:   legacyEncoded,
			},
			ChunkToWrite{
				IndexType: PrefixAccountActions,
				Account:   g.account,
				BaseSeq:   baseSeq,
				Encoded:   leanEncoded,
			},
		)
	}

	return chunks
}

func buildCAChunksForAccount(entries []Entry, g accountGroup, metadata *ChunkMetadata) []ChunkToWrite {
	sortedIndices := make([]int32, len(g.indices))
	copy(sortedIndices, g.indices)

	sortIndicesByContractAction(sortedIndices, entries)

	var chunks []ChunkToWrite
	var currentKey ContractActionKey
	var currentSeqs []uint64

	for _, idx := range sortedIndices {
		e := &entries[idx]
		key := ContractActionKey{Account: g.account, Contract: e.Contract, Action: e.Action}

		if key != currentKey && len(currentSeqs) > 0 {
			baseSeq := currentSeqs[0]
			encoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
			chunks = append(chunks, ChunkToWrite{
				IndexType: PrefixContractAction,
				Account:   currentKey.Account,
				Contract:  currentKey.Contract,
				Action:    currentKey.Action,
				BaseSeq:   baseSeq,
				Encoded:   encoded,
			})
			currentSeqs = currentSeqs[:0]
		}

		currentKey = key
		currentSeqs = append(currentSeqs, e.GlobalSeq)

		if len(currentSeqs) >= ChunkSize {
			baseSeq := currentSeqs[0]
			encoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
			chunks = append(chunks, ChunkToWrite{
				IndexType: PrefixContractAction,
				Account:   currentKey.Account,
				Contract:  currentKey.Contract,
				Action:    currentKey.Action,
				BaseSeq:   baseSeq,
				Encoded:   encoded,
			})
			currentSeqs = currentSeqs[:0]
		}
	}

	if len(currentSeqs) > 0 {
		baseSeq := currentSeqs[0]
		encoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
		chunks = append(chunks, ChunkToWrite{
			IndexType: PrefixContractAction,
			Account:   currentKey.Account,
			Contract:  currentKey.Contract,
			Action:    currentKey.Action,
			BaseSeq:   baseSeq,
			Encoded:   encoded,
		})
	}

	return chunks
}

func buildCWChunksForAccount(entries []Entry, g accountGroup, metadata *ChunkMetadata) []ChunkToWrite {
	sortedIndices := make([]int32, len(g.indices))
	copy(sortedIndices, g.indices)

	sortIndicesByContractSeq(sortedIndices, entries)

	var chunks []ChunkToWrite
	var currentKey ContractWildcardKey
	var currentSeqs []uint64

	for _, idx := range sortedIndices {
		e := &entries[idx]
		key := ContractWildcardKey{Account: g.account, Contract: e.Contract}

		if key != currentKey && len(currentSeqs) > 0 {
			baseSeq := currentSeqs[0]
			encoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
			chunks = append(chunks, ChunkToWrite{
				IndexType: PrefixContractWildcard,
				Account:   currentKey.Account,
				Contract:  currentKey.Contract,
				BaseSeq:   baseSeq,
				Encoded:   encoded,
			})
			currentSeqs = currentSeqs[:0]
		}

		currentKey = key
		currentSeqs = append(currentSeqs, e.GlobalSeq)

		if len(currentSeqs) >= ChunkSize {
			baseSeq := currentSeqs[0]
			encoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
			chunks = append(chunks, ChunkToWrite{
				IndexType: PrefixContractWildcard,
				Account:   currentKey.Account,
				Contract:  currentKey.Contract,
				BaseSeq:   baseSeq,
				Encoded:   encoded,
			})
			currentSeqs = currentSeqs[:0]
		}
	}

	if len(currentSeqs) > 0 {
		baseSeq := currentSeqs[0]
		encoded, _ := EncodeLeanChunk(baseSeq, currentSeqs)
		chunks = append(chunks, ChunkToWrite{
			IndexType: PrefixContractWildcard,
			Account:   currentKey.Account,
			Contract:  currentKey.Contract,
			BaseSeq:   baseSeq,
			Encoded:   encoded,
		})
	}

	return chunks
}

func sortIndicesByGlobalSeq(indices []int32, entries []Entry) {
	n := len(indices)
	if n < 2 {
		return
	}

	for i := 1; i < n; i++ {
		for j := i; j > 0 && entries[indices[j-1]].GlobalSeq > entries[indices[j]].GlobalSeq; j-- {
			indices[j-1], indices[j] = indices[j], indices[j-1]
		}
	}
}

const parallelIndexSortThreshold = 100_000

type chunkRange struct {
	start, end int
}

func mergeInto(dst, src []int32, a, b chunkRange, less func(i, j int32) bool) int {
	i, j, k := a.start, b.start, a.start
	for i < a.end && j < b.end {
		if less(src[i], src[j]) {
			dst[k] = src[i]
			i++
		} else {
			dst[k] = src[j]
			j++
		}
		k++
	}
	for i < a.end {
		dst[k] = src[i]
		i++
		k++
	}
	for j < b.end {
		dst[k] = src[j]
		j++
		k++
	}
	return k
}

func sortIndicesByContractAction(indices []int32, entries []Entry) {
	n := len(indices)
	if n < parallelIndexSortThreshold {
		slices.SortFunc(indices, func(i, j int32) int {
			a, b := &entries[i], &entries[j]
			if c := cmp.Compare(a.Contract, b.Contract); c != 0 {
				return c
			}
			if c := cmp.Compare(a.Action, b.Action); c != 0 {
				return c
			}
			return cmp.Compare(a.GlobalSeq, b.GlobalSeq)
		})
		return
	}

	numGoroutines := 16
	chunkSize := (n + numGoroutines - 1) / numGoroutines

	buf1 := make([]int32, n)
	buf2 := make([]int32, n)
	copy(buf1, indices)

	ranges := make([]chunkRange, 0, numGoroutines)
	for i := 0; i < n; i += chunkSize {
		end := i + chunkSize
		if end > n {
			end = n
		}
		ranges = append(ranges, chunkRange{i, end})
	}

	var wg sync.WaitGroup
	wg.Add(len(ranges))
	for _, r := range ranges {
		go func(start, end int) {
			defer wg.Done()
			slices.SortFunc(buf1[start:end], func(i, j int32) int {
				a, b := &entries[i], &entries[j]
				if c := cmp.Compare(a.Contract, b.Contract); c != 0 {
					return c
				}
				if c := cmp.Compare(a.Action, b.Action); c != 0 {
					return c
				}
				return cmp.Compare(a.GlobalSeq, b.GlobalSeq)
			})
		}(r.start, r.end)
	}
	wg.Wait()

	less := func(i, j int32) bool {
		a, b := &entries[i], &entries[j]
		if a.Contract != b.Contract {
			return a.Contract < b.Contract
		}
		if a.Action != b.Action {
			return a.Action < b.Action
		}
		return a.GlobalSeq < b.GlobalSeq
	}

	src, dst := buf1, buf2
	for len(ranges) > 1 {
		newRanges := make([]chunkRange, 0, (len(ranges)+1)/2)
		wg.Add(len(ranges) / 2)

		for i := 0; i < len(ranges)-1; i += 2 {
			go func(a, b chunkRange) {
				defer wg.Done()
				mergeInto(dst, src, a, b, less)
			}(ranges[i], ranges[i+1])
			newRanges = append(newRanges, chunkRange{ranges[i].start, ranges[i+1].end})
		}

		if len(ranges)%2 == 1 {
			last := ranges[len(ranges)-1]
			copy(dst[last.start:last.end], src[last.start:last.end])
			newRanges = append(newRanges, last)
		}

		wg.Wait()
		ranges = newRanges
		src, dst = dst, src
	}

	if &src[0] != &indices[0] {
		copy(indices, src)
	}
}

func sortIndicesByContractSeq(indices []int32, entries []Entry) {
	n := len(indices)
	if n < parallelIndexSortThreshold {
		slices.SortFunc(indices, func(i, j int32) int {
			a, b := &entries[i], &entries[j]
			if c := cmp.Compare(a.Contract, b.Contract); c != 0 {
				return c
			}
			return cmp.Compare(a.GlobalSeq, b.GlobalSeq)
		})
		return
	}

	numGoroutines := 16
	chunkSize := (n + numGoroutines - 1) / numGoroutines

	buf1 := make([]int32, n)
	buf2 := make([]int32, n)
	copy(buf1, indices)

	ranges := make([]chunkRange, 0, numGoroutines)
	for i := 0; i < n; i += chunkSize {
		end := i + chunkSize
		if end > n {
			end = n
		}
		ranges = append(ranges, chunkRange{i, end})
	}

	var wg sync.WaitGroup
	wg.Add(len(ranges))
	for _, r := range ranges {
		go func(start, end int) {
			defer wg.Done()
			slices.SortFunc(buf1[start:end], func(i, j int32) int {
				a, b := &entries[i], &entries[j]
				if c := cmp.Compare(a.Contract, b.Contract); c != 0 {
					return c
				}
				return cmp.Compare(a.GlobalSeq, b.GlobalSeq)
			})
		}(r.start, r.end)
	}
	wg.Wait()

	less := func(i, j int32) bool {
		a, b := &entries[i], &entries[j]
		if a.Contract != b.Contract {
			return a.Contract < b.Contract
		}
		return a.GlobalSeq < b.GlobalSeq
	}

	src, dst := buf1, buf2
	for len(ranges) > 1 {
		newRanges := make([]chunkRange, 0, (len(ranges)+1)/2)
		wg.Add(len(ranges) / 2)

		for i := 0; i < len(ranges)-1; i += 2 {
			go func(a, b chunkRange) {
				defer wg.Done()
				mergeInto(dst, src, a, b, less)
			}(ranges[i], ranges[i+1])
			newRanges = append(newRanges, chunkRange{ranges[i].start, ranges[i+1].end})
		}

		if len(ranges)%2 == 1 {
			last := ranges[len(ranges)-1]
			copy(dst[last.start:last.end], src[last.start:last.end])
			newRanges = append(newRanges, last)
		}

		wg.Wait()
		ranges = newRanges
		src, dst = dst, src
	}

	if &src[0] != &indices[0] {
		copy(indices, src)
	}
}

type accountGroup struct {
	account uint64
	indices []int32
}

func groupByAccount(entries []Entry) []accountGroup {
	counts := make(map[uint64]int32)
	for i := range entries {
		counts[entries[i].Account]++
	}

	groups := make(map[uint64][]int32, len(counts))
	for account, count := range counts {
		groups[account] = make([]int32, 0, count)
	}

	for i := range entries {
		acc := entries[i].Account
		groups[acc] = append(groups[acc], int32(i))
	}

	result := make([]accountGroup, 0, len(groups))
	for account, indices := range groups {
		result = append(result, accountGroup{account: account, indices: indices})
	}

	slices.SortFunc(result, func(a, b accountGroup) int {
		return cmp.Compare(a.account, b.account)
	})

	return result
}

type ChunkToWrite struct {
	IndexType byte
	Account   uint64
	Contract  uint64
	Action    uint64
	ChunkID   uint32
	BaseSeq   uint64
	Encoded   []byte
}

func (c *ChunkToWrite) MakeKey() []byte {
	switch c.IndexType {
	case PrefixLegacyAccountActions:
		return makeLegacyAccountActionsKey(c.Account, c.ChunkID)
	case PrefixAccountActions:
		return makeAccountActionsKey(c.Account, c.BaseSeq)
	case PrefixContractAction:
		return makeContractActionKey(c.Account, c.Contract, c.Action, c.BaseSeq)
	case PrefixContractWildcard:
		return makeContractWildcardKey(c.Account, c.Contract, c.BaseSeq)
	default:
		panic(fmt.Sprintf("unknown index type: %d", c.IndexType))
	}
}

type MergeStats struct {
	EntriesProcessed       int
	ChunksWritten          int
	AllActionsChunks       int
	ContractActionChunks   int
	ContractWildcardChunks int
	BytesWritten           int64
}

func (s *MergeStats) LogStats() {
	if s.EntriesProcessed == 0 {
		return
	}

	entryStr := logger.FormatCount(int64(s.EntriesProcessed))

	logger.Printf("debug-timing", "merge Entries=%s | Chunks=%d [AA:%d CA:%d CW:%d] | Bytes=%s",
		entryStr,
		s.ChunksWritten,
		s.AllActionsChunks,
		s.ContractActionChunks,
		s.ContractWildcardChunks,
		logger.FormatBytes(s.BytesWritten),
	)
}
