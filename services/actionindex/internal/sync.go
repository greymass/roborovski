package internal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

// calculateBulkCommitInterval computes the optimal commit interval based on memory budget.
// More memory = can hold more blocks in memory before flushing to disk = less GC pressure.
func calculateBulkCommitInterval(syncMemoryGB int) uint32 {
	blocks := uint32(syncMemoryGB * 2500) // ~2500 blocks per GB
	if blocks < 10000 {
		blocks = 10000
	}
	return blocks
}

type Syncer struct {
	indexes            ActionIndexer
	store              *Store
	reader             corereader.Reader
	config             *Config
	actionFilter       *corereader.ActionFilter
	includeFilter      *corereader.IncludeFilter
	broadcaster        *ActionBroadcaster
	timing             *SyncTiming
	bulkCommitInterval uint32

	librarySyncer *corereader.Syncer
	running       atomic.Bool
	stop          chan struct{}
	done          chan struct{}
}

func NewSyncer(indexes ActionIndexer, store *Store, reader corereader.Reader, config *Config, timing *SyncTiming, broadcaster *ActionBroadcaster) *Syncer {
	actionFilter := corereader.NewActionFilter(config.IgnoredActions)
	if !actionFilter.IsEmpty() {
		logger.Printf("startup", "Action exclusion filter: %s", actionFilter.Summary())
	} else {
		logger.Printf("startup", "Action exclusion filter: DISABLED (not excluding any actions)")
	}

	includeFilter := corereader.NewIncludeFilter(config.FilterContracts)
	if !includeFilter.IsEmpty() {
		logger.Printf("startup", "Contract include filter: %s", includeFilter.Summary())
	} else {
		logger.Printf("startup", "Contract include filter: DISABLED (indexing all contracts)")
	}

	if timing != nil {
		indexes.SetTiming(timing)
		if dp, ok := indexes.(DiagnosticsProvider); ok {
			timing.SetDiagnosticsProvider(dp)
		}
	}

	bulkCommitInterval := calculateBulkCommitInterval(config.SyncMemoryGB)

	return &Syncer{
		indexes:            indexes,
		store:              store,
		reader:             reader,
		config:             config,
		actionFilter:       actionFilter,
		includeFilter:      includeFilter,
		broadcaster:        broadcaster,
		timing:             timing,
		bulkCommitInterval: bulkCommitInterval,
		stop:               make(chan struct{}),
		done:               make(chan struct{}),
	}
}

func (s *Syncer) Start() error {
	if s.running.Swap(true) {
		return fmt.Errorf("syncer already running")
	}

	go s.syncLoop()
	return nil
}

func (s *Syncer) Stop() {
	if s.running.Load() {
		close(s.stop)
		<-s.done
	}
}

func (s *Syncer) combinedFilter() corereader.ActionFilterFunc {
	excludeFunc := s.actionFilter.AsFunc()
	includeFunc := s.includeFilter.AsFunc()

	if excludeFunc == nil && includeFunc == nil {
		return nil
	}
	if excludeFunc == nil {
		return includeFunc
	}
	if includeFunc == nil {
		return excludeFunc
	}
	return func(contract, action uint64) bool {
		return excludeFunc(contract, action) || includeFunc(contract, action)
	}
}

func (s *Syncer) syncLoop() {
	defer s.running.Store(false)
	defer close(s.done)

	libNum, _, err := s.indexes.GetProperties()
	if err != nil {
		logger.Printf("sync", "Error getting properties: %v", err)
		return
	}

	startBlock := libNum + 1
	if startBlock == 1 {
		startBlock = 2
	}

	firstAvailable, ok := s.reader.FindFirstAvailableBlock(startBlock)
	if !ok {
		logger.Printf("sync", "Error: no blocks available in history storage")
		return
	}
	if firstAvailable > startBlock {
		logger.Printf("sync", "History storage starts at block %d, adjusting start block", firstAvailable)
		startBlock = firstAvailable
	}

	_, chainLIB, err := s.reader.GetStateProps(true)
	if err != nil {
		logger.Printf("sync", "Error getting chain state: %v", err)
		return
	}

	logger.Printf("sync", "Starting sync from block %d (chain LIB: %d)", startBlock, chainLIB)

	processor := NewAccountHistoryProcessor(s)

	workerCount := s.config.Workers
	if workerCount < 1 {
		workerCount = 4
	}

	logInterval := 3 * time.Second
	if s.config.LogInterval != "" {
		if parsed, err := time.ParseDuration(s.config.LogInterval); err == nil {
			logInterval = parsed
		} else if secs, err := time.ParseDuration(s.config.LogInterval + "s"); err == nil {
			logInterval = secs
		}
	}

	cfg := corereader.SyncConfig{
		Workers:       workerCount,
		BulkThreshold: 1000,
		LogInterval:   logInterval,
		ActionFilter:  s.combinedFilter(),
		Debug:         s.config.Debug,
	}

	s.librarySyncer = corereader.NewSyncer(s.reader, cfg)
	s.librarySyncer.SetDBSizeFunc(func() int64 { return int64(s.store.Size()) })

	go func() {
		<-s.stop
		if s.librarySyncer != nil {
			s.librarySyncer.Stop()
		}
	}()

	if err := s.librarySyncer.SyncActions(processor, startBlock); err != nil {
		logger.Printf("sync", "Sync error: %v", err)
	}
}

type AccountHistoryProcessor struct {
	syncer             *Syncer
	blocksProcessed    int
	lastProcessedBlock uint32
	lastMaxSeq         uint64
	timing             *SyncTiming
	batchBuf           []ActionEntry
}

func NewAccountHistoryProcessor(syncer *Syncer) *AccountHistoryProcessor {
	return &AccountHistoryProcessor{
		syncer:   syncer,
		timing:   syncer.timing,
		batchBuf: make([]ActionEntry, 0, 4096),
	}
}

func (p *AccountHistoryProcessor) ProcessBlock(block corereader.Block) error {
	var blockStart time.Time
	if p.timing != nil {
		blockStart = time.Now()
	}

	var batchBuildStart time.Time
	if p.timing != nil {
		batchBuildStart = time.Now()
	}

	actionCount := len(block.Actions)
	if actionCount > 0 {
		if cap(p.batchBuf) < actionCount {
			p.batchBuf = make([]ActionEntry, actionCount)
		} else {
			p.batchBuf = p.batchBuf[:actionCount]
		}
		for i := range block.Actions {
			action := &block.Actions[i]
			p.batchBuf[i] = ActionEntry{
				Account:   action.Account,
				Contract:  action.Contract,
				Action:    action.Action,
				GlobalSeq: action.GlobalSeq,
				BlockTime: block.BlockTime,
			}
		}
	}
	if p.timing != nil {
		p.timing.RecordBatchBuild(time.Since(batchBuildStart))
	}

	var indexAddStart time.Time
	if p.timing != nil {
		indexAddStart = time.Now()
	}
	if actionCount > 0 {
		p.syncer.indexes.AddBatch(p.batchBuf)
	}
	if p.timing != nil && actionCount > 0 {
		p.timing.RecordIndexAdd(time.Since(indexAddStart))
	}

	if p.syncer.broadcaster != nil && p.syncer.broadcaster.IsLiveMode() && actionCount > 0 {
		p.broadcastActions(block)
	}

	var blockTimeStart time.Time
	if p.timing != nil {
		blockTimeStart = time.Now()
	}
	if block.MinSeq > 0 {
		p.syncer.indexes.RecordBlockTime(block.BlockTime, block.MinSeq, block.MaxSeq)
	}
	if p.timing != nil {
		p.timing.RecordBlockTime(time.Since(blockTimeStart))
	}

	p.blocksProcessed++
	p.lastProcessedBlock = block.BlockNum
	if block.MaxSeq > p.lastMaxSeq {
		p.lastMaxSeq = block.MaxSeq
	}

	if p.syncer.broadcaster != nil && p.syncer.broadcaster.IsLiveMode() && block.MaxSeq > 0 {
		p.syncer.broadcaster.SetState(block.MaxSeq, block.MaxSeq)
	}

	if p.timing != nil {
		p.timing.RecordProcessBlock(time.Since(blockStart), actionCount)
	}

	return nil
}

func (p *AccountHistoryProcessor) broadcastActions(block corereader.Block) {
	for i := range block.Actions {
		action := &block.Actions[i]
		p.syncer.broadcaster.Broadcast(StreamedAction{
			GlobalSeq: action.GlobalSeq,
			BlockNum:  block.BlockNum,
			BlockTime: block.BlockTime,
			Contract:  action.Contract,
			Action:    action.Action,
			Receiver:  action.Account,
		})
	}
}

func (p *AccountHistoryProcessor) ShouldCommit(blocksProcessed int) bool {
	return blocksProcessed >= int(p.syncer.bulkCommitInterval) || blocksProcessed < 50
}

func (p *AccountHistoryProcessor) Commit(currentBlock uint32, bulkMode bool) error {
	var commitStart time.Time
	if p.timing != nil {
		commitStart = time.Now()
	}

	p.syncer.indexes.SetBulkMode(bulkMode)

	commitCount := (currentBlock / p.syncer.bulkCommitInterval) % 10
	var err error
	withSync := commitCount == 0
	err = p.syncer.indexes.CommitWithTiming(currentBlock, currentBlock, withSync, p.timing)

	if err != nil {
		return err
	}

	if p.timing != nil {
		p.timing.RecordCommit(time.Since(commitStart), withSync)
	}

	p.blocksProcessed = 0
	return nil
}

func (p *AccountHistoryProcessor) Flush() error {
	if p.blocksProcessed > 0 {
		logger.Printf("sync", "Flush: committing final %d blocks, lastProcessedBlock=%d", p.blocksProcessed, p.lastProcessedBlock)
		if err := p.Commit(p.lastProcessedBlock, true); err != nil {
			return err
		}
	}
	p.syncer.indexes.SetBulkMode(false)
	if p.syncer.broadcaster != nil {
		headSeq := p.lastMaxSeq
		if headSeq == 0 {
			headSeq = p.syncer.indexes.MaxSeq()
		}
		p.syncer.broadcaster.SetState(headSeq, headSeq)
		p.syncer.broadcaster.SetLiveMode(true)
		logger.Printf("sync", "Broadcaster initialized with headSeq=%d", headSeq)
	}
	logger.Printf("sync", "Flush complete, transitioned to live mode")
	return nil
}

// ProcessBatch processes multiple parsed blocks in one call.
// This flattens actions internally and records block times.
func (p *AccountHistoryProcessor) ProcessBatch(blocks []corereader.Block) error {
	var bulkStart time.Time
	if p.timing != nil {
		bulkStart = time.Now()
	}

	totalActions := 0
	for _, b := range blocks {
		totalActions += len(b.Actions)
	}

	if totalActions == 0 {
		if len(blocks) > 0 {
			p.blocksProcessed += len(blocks)
			p.lastProcessedBlock = blocks[len(blocks)-1].BlockNum
		}
		return nil
	}

	var batchBuildStart time.Time
	if p.timing != nil {
		batchBuildStart = time.Now()
	}

	if cap(p.batchBuf) < totalActions {
		p.batchBuf = make([]ActionEntry, totalActions)
	} else {
		p.batchBuf = p.batchBuf[:totalActions]
	}

	const parallelThreshold = 50000
	if totalActions >= parallelThreshold {
		p.buildBatchParallelFromBlocks(blocks)
	} else {
		idx := 0
		for _, block := range blocks {
			for i := range block.Actions {
				act := &block.Actions[i]
				p.batchBuf[idx] = ActionEntry{
					Account:   act.Account,
					Contract:  act.Contract,
					Action:    act.Action,
					GlobalSeq: act.GlobalSeq,
					BlockTime: 0,
				}
				idx++
			}
		}
	}

	if p.timing != nil {
		p.timing.RecordBatchBuild(time.Since(batchBuildStart))
	}

	var indexAddStart time.Time
	if p.timing != nil {
		indexAddStart = time.Now()
	}
	p.syncer.indexes.AddBatch(p.batchBuf)
	if p.timing != nil {
		p.timing.RecordIndexAdd(time.Since(indexAddStart))
	}

	var blockTimeStart time.Time
	if p.timing != nil {
		blockTimeStart = time.Now()
	}
	for _, b := range blocks {
		if b.MinSeq > 0 {
			p.syncer.indexes.RecordBlockTime(b.BlockTime, b.MinSeq, b.MaxSeq)
		}
	}
	if p.timing != nil {
		p.timing.RecordBlockTime(time.Since(blockTimeStart))
	}

	p.blocksProcessed += len(blocks)
	lastBlock := blocks[len(blocks)-1]
	p.lastProcessedBlock = lastBlock.BlockNum
	if lastBlock.MaxSeq > p.lastMaxSeq {
		p.lastMaxSeq = lastBlock.MaxSeq
	}

	if p.syncer.broadcaster != nil && p.syncer.broadcaster.IsLiveMode() && lastBlock.MaxSeq > 0 {
		p.syncer.broadcaster.SetState(lastBlock.MaxSeq, lastBlock.MaxSeq)
	}

	if p.timing != nil {
		p.timing.RecordProcessBlock(time.Since(bulkStart), totalActions)
	}

	return nil
}

// buildBatchParallelFromBlocks converts blocks' actions to ActionEntries using multiple goroutines.
func (p *AccountHistoryProcessor) buildBatchParallelFromBlocks(blocks []corereader.Block) {
	const numWorkers = 8

	offsets := make([]int, len(blocks)+1)
	for i, b := range blocks {
		offsets[i+1] = offsets[i] + len(b.Actions)
	}
	totalActions := offsets[len(blocks)]

	var wg sync.WaitGroup
	chunkSize := (totalActions + numWorkers - 1) / numWorkers

	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > totalActions {
			end = totalActions
		}
		if start >= totalActions {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			blockIdx := 0
			for blockIdx < len(blocks) && offsets[blockIdx+1] <= start {
				blockIdx++
			}
			actionIdx := start - offsets[blockIdx]

			for i := start; i < end; i++ {
				for actionIdx >= len(blocks[blockIdx].Actions) {
					blockIdx++
					actionIdx = 0
				}
				act := &blocks[blockIdx].Actions[actionIdx]
				p.batchBuf[i] = ActionEntry{
					Account:   act.Account,
					Contract:  act.Contract,
					Action:    act.Action,
					GlobalSeq: act.GlobalSeq,
					BlockTime: 0,
				}
				actionIdx++
			}
		}(start, end)
	}
	wg.Wait()
}
