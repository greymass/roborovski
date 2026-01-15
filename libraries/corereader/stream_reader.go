package corereader

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/corestream"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
)

var ErrStreamingNotSupported = errors.New("operation not supported in streaming mode - use sequential access via NextBlock()")

type StreamReader struct {
	client      *corestream.Client
	queryReader BaseReader
	address     string

	currentLIB  atomic.Uint32
	currentHEAD atomic.Uint32

	mu              sync.RWMutex
	blocksReceived  uint64
	lastBlockTime   time.Time
	connectTime     time.Time
	lastLogTime     time.Time
	lastLogBlocks   uint64
	metricsInterval time.Duration
}

func NewStreamReader(streamAddress string, queryReader BaseReader) (*StreamReader, error) {
	address := normalizeStreamAddress(streamAddress)

	config := corestream.DefaultClientConfig()
	config.Debug = false

	client := corestream.NewClient(address, config)

	sr := &StreamReader{
		client:          client,
		queryReader:     queryReader,
		address:         address,
		metricsInterval: 10 * time.Second,
	}

	return sr, nil
}

func normalizeStreamAddress(address string) string {
	if strings.HasPrefix(address, "stream://") {
		return strings.TrimPrefix(address, "stream://")
	}
	if strings.HasPrefix(address, "unix://") {
		return strings.TrimPrefix(address, "unix://")
	}
	return address
}

func (sr *StreamReader) Connect(startBlock uint32) error {
	sr.mu.Lock()
	sr.connectTime = time.Now()
	sr.lastLogTime = time.Now()
	sr.lastLogBlocks = 0
	sr.blocksReceived = 0
	sr.mu.Unlock()

	if err := sr.client.Connect(startBlock); err != nil {
		return fmt.Errorf("failed to connect to stream: %w", err)
	}

	head, lib, err := sr.client.QueryState(10 * time.Second)
	if err != nil {
		sr.currentLIB.Store(sr.client.GetLIB())
		sr.currentHEAD.Store(sr.client.GetHEAD())
		logger.Printf("stream", "Connected to stream (QueryState failed: %v, using handshake values)", err)
	} else {
		sr.currentLIB.Store(lib)
		sr.currentHEAD.Store(head)
	}

	logger.Printf("stream", "Connected to stream, starting from block %d (LIB: %d, HEAD: %d)",
		startBlock, sr.currentLIB.Load(), sr.currentHEAD.Load())

	return nil
}

func (sr *StreamReader) NextBlock() (*RawBlock, error) {
	blockNum, data, err := sr.client.NextBlock()
	if err != nil {
		return nil, err
	}

	return sr.processBlockData(blockNum, data)
}

func (sr *StreamReader) NextBlockWithTimeout(timeout time.Duration) (*RawBlock, error) {
	blockNum, data, err := sr.client.NextBlockWithTimeout(timeout)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	return sr.processBlockData(blockNum, data)
}

func (sr *StreamReader) processBlockData(blockNum uint32, data []byte) (*RawBlock, error) {
	sr.currentLIB.Store(sr.client.GetLIB())
	sr.currentHEAD.Store(sr.client.GetHEAD())

	sr.mu.Lock()
	sr.blocksReceived++
	sr.lastBlockTime = time.Now()
	blocksReceived := sr.blocksReceived
	sr.mu.Unlock()

	if sr.metricsInterval > 0 {
		sr.mu.RLock()
		lastLog := sr.lastLogTime
		lastLogBlocks := sr.lastLogBlocks
		sr.mu.RUnlock()

		if time.Since(lastLog) >= sr.metricsInterval {
			blocksSinceLog := blocksReceived - lastLogBlocks
			elapsed := time.Since(lastLog).Seconds()
			bps := float64(blocksSinceLog) / elapsed

			lib := sr.currentLIB.Load()
			behind := int64(lib) - int64(blockNum)
			mode := "live"
			if behind > 1000 {
				mode = "catchup"
			}

			logger.Printf("stream", "[%s] Block %d | LIB %d | Behind: %d | BPS: %.1f | Total: %d",
				mode, blockNum, lib, behind, bps, blocksReceived)

			sr.mu.Lock()
			sr.lastLogTime = time.Now()
			sr.lastLogBlocks = blocksReceived
			sr.mu.Unlock()
		}
	}

	blob := bytesToBlockBlob(data)
	notifs, actionMeta := extractNotificationsWithMetadata(blob)

	return &RawBlock{
		BlockNum:      blockNum,
		BlockTime:     blob.Block.BlockTimeUint32,
		Notifications: notifs,
		ActionMeta:    actionMeta,
	}, nil
}

func (sr *StreamReader) NextBlockFiltered(filterFunc ActionFilterFunc) (*RawBlock, error) {
	blockNum, data, err := sr.client.NextBlock()
	if err != nil {
		return nil, err
	}

	return sr.processBlockDataFiltered(blockNum, data, filterFunc)
}

func (sr *StreamReader) NextBlockWithTimeoutFiltered(timeout time.Duration, filterFunc ActionFilterFunc) (*RawBlock, error) {
	blockNum, data, err := sr.client.NextBlockWithTimeout(timeout)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	return sr.processBlockDataFiltered(blockNum, data, filterFunc)
}

func (sr *StreamReader) processBlockDataFiltered(blockNum uint32, data []byte, filterFunc ActionFilterFunc) (*RawBlock, error) {
	sr.currentLIB.Store(sr.client.GetLIB())
	sr.currentHEAD.Store(sr.client.GetHEAD())

	sr.mu.Lock()
	sr.blocksReceived++
	sr.lastBlockTime = time.Now()
	sr.mu.Unlock()

	blob := bytesToBlockBlob(data)
	notifs, actionMeta, _ := extractNotificationsWithMetadataFiltered(blob, filterFunc)

	return &RawBlock{
		BlockNum:      blockNum,
		BlockTime:     blob.Block.BlockTimeUint32,
		Notifications: notifs,
		ActionMeta:    actionMeta,
	}, nil
}

func (sr *StreamReader) GetStateProps(bypassCache bool) (head uint32, lib uint32, err error) {
	return sr.currentHEAD.Load(), sr.currentLIB.Load(), nil
}

func (sr *StreamReader) GetLIB() uint32 {
	return sr.currentLIB.Load()
}

func (sr *StreamReader) GetHEAD() uint32 {
	return sr.currentHEAD.Load()
}

func (sr *StreamReader) IsConnected() bool {
	return sr.client.IsConnected()
}

func (sr *StreamReader) GetMetrics() (blocksReceived uint64, connected bool, lastBlockTime time.Time, uptime time.Duration) {
	sr.mu.RLock()
	blocksReceived = sr.blocksReceived
	lastBlockTime = sr.lastBlockTime
	connectTime := sr.connectTime
	sr.mu.RUnlock()

	connected = sr.client.IsConnected()
	if !connectTime.IsZero() {
		uptime = time.Since(connectTime)
	}
	return
}

func (sr *StreamReader) GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error) {
	return nil, "", "", ErrStreamingNotSupported
}

func (sr *StreamReader) GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error) {
	return nil, nil, "", "", ErrStreamingNotSupported
}

func (sr *StreamReader) GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error) {
	return nil, nil, "", "", 0, 0, ErrStreamingNotSupported
}

func (sr *StreamReader) GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error) {
	return nil, ErrStreamingNotSupported
}

func (sr *StreamReader) GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	return nil, ErrStreamingNotSupported
}

func (sr *StreamReader) GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error) {
	if sr.queryReader != nil {
		return sr.queryReader.GetActionsByGlobalSeqs(globalSeqs)
	}

	if !sr.client.IsConnected() {
		return nil, nil, corestream.ErrNotConnected
	}

	resp, err := sr.client.QueryGlobs(globalSeqs, 30*time.Second)
	if err != nil {
		return nil, nil, err
	}

	results := make([]chain.ActionTrace, len(resp.Actions))
	for i, actionData := range resp.Actions {
		cat := catFromBytes(actionData.Data)
		blockTime := chain.Uint32ToTime(actionData.BlockTime)

		minimalBlock := &blockData{
			BlockNum:  actionData.BlockNum,
			BlockTime: blockTime,
		}

		at := catToActionTrace(cat, minimalBlock, actionData.BlockNum, actionData.GlobalSeq)
		results[i] = *at
	}

	return results, nil, nil
}

func (sr *StreamReader) GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error) {
	if sr.queryReader != nil {
		return sr.queryReader.GetRawActionsFiltered(blockNum, contractFilter, actionFilter)
	}

	if !sr.client.IsConnected() {
		return nil, "", "", corestream.ErrNotConnected
	}

	resp, err := sr.client.QueryBlock(blockNum, 30*time.Second)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlob(resp.Data)
	contractName := chain.StringToName(contractFilter)
	actionName := chain.StringToName(actionFilter)

	var actions []chain.ActionTrace
	for i, cat := range blob.Cats {
		contract := blob.Block.NamesInBlock[cat.ContractNameIndex]
		action := blob.Block.NamesInBlock[cat.ActionNameIndex]

		if (contractFilter == "" || contract == contractName) &&
			(actionFilter == "" || action == actionName) {
			glob := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
			at := cat.At(blob.Block, blockNum, glob)
			actions = append(actions, *at)
		}
	}

	blockID := fmt.Sprintf("%x", blob.Block.ProducerBlockID)

	return actions, blockID, "", nil
}

func (sr *StreamReader) GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int) {
	if sr.queryReader != nil {
		return sr.queryReader.GetBlockCacheStats()
	}
	return 0, 0, 0, 0
}

func (sr *StreamReader) Close() error {
	sr.mu.RLock()
	blocksReceived := sr.blocksReceived
	connectTime := sr.connectTime
	sr.mu.RUnlock()

	var uptime time.Duration
	if !connectTime.IsZero() {
		uptime = time.Since(connectTime)
	}

	logger.Printf("stream", "Closing stream reader (received %d blocks in %v)", blocksReceived, uptime)

	if err := sr.client.Close(); err != nil {
		return err
	}

	if sr.queryReader != nil {
		return sr.queryReader.Close()
	}
	return nil
}

func (sr *StreamReader) NextBlockTrxIDs() (blockNum uint32, trxIDs []string, err error) {
	blockNum, data, err := sr.client.NextBlock()
	if err != nil {
		return 0, nil, err
	}

	sr.currentLIB.Store(sr.client.GetLIB())
	sr.currentHEAD.Store(sr.client.GetHEAD())

	sr.mu.Lock()
	sr.blocksReceived++
	sr.lastBlockTime = time.Now()
	sr.mu.Unlock()

	blob := bytesToBlockBlobBlockOnly(data)
	trxIDs = make([]string, 0, len(blob.TrxIDInBlock))
	for _, trxID := range blob.TrxIDInBlock {
		trxIDs = append(trxIDs, fmt.Sprintf("%x", trxID[:]))
	}

	return blockNum, trxIDs, nil
}

func (sr *StreamReader) NextBlockTrxIDsWithTimeout(timeout time.Duration) (blockNum uint32, trxIDs []string, err error) {
	blockNum, data, err := sr.client.NextBlockWithTimeout(timeout)
	if err != nil {
		return 0, nil, err
	}
	if data == nil {
		return 0, nil, nil
	}

	sr.currentLIB.Store(sr.client.GetLIB())
	sr.currentHEAD.Store(sr.client.GetHEAD())

	sr.mu.Lock()
	sr.blocksReceived++
	sr.lastBlockTime = time.Now()
	sr.mu.Unlock()

	blob := bytesToBlockBlobBlockOnly(data)
	trxIDs = make([]string, 0, len(blob.TrxIDInBlock))
	for _, trxID := range blob.TrxIDInBlock {
		trxIDs = append(trxIDs, fmt.Sprintf("%x", trxID[:]))
	}

	return blockNum, trxIDs, nil
}

func (sr *StreamReader) FindFirstAvailableBlock(afterBlock uint32) (uint32, bool) {
	return afterBlock, true
}

func (sr *StreamReader) GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error) {
	if sr.queryReader != nil {
		return sr.queryReader.GetTransactionIDsOnly(blockNum, skipOnblock)
	}

	if !sr.client.IsConnected() {
		return nil, "", "", corestream.ErrNotConnected
	}

	resp, err := sr.client.QueryBlock(blockNum, 30*time.Second)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlobBlockOnly(resp.Data)

	blockID := fmt.Sprintf("%x", blob.ProducerBlockID)

	trxIDs := make([]string, 0, len(blob.TrxIDInBlock))
	for _, trxID := range blob.TrxIDInBlock {
		trxIDs = append(trxIDs, fmt.Sprintf("%x", trxID[:]))
	}

	return trxIDs, blockID, "", nil
}

// GetTransactionIDsRaw returns raw transaction IDs as [32]byte without hex encoding.
func (sr *StreamReader) GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error) {
	if sr.queryReader != nil {
		return sr.queryReader.GetTransactionIDsRaw(blockNum)
	}

	if !sr.client.IsConnected() {
		return nil, corestream.ErrNotConnected
	}

	resp, err := sr.client.QueryBlock(blockNum, 30*time.Second)
	if err != nil {
		return nil, err
	}

	blob := bytesToBlockBlobBlockOnly(resp.Data)
	return blob.TrxIDInBlock, nil
}

func (sr *StreamReader) GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error) {
	if sr.queryReader != nil {
		return sr.queryReader.GetTransactionData(blockNum, trxID)
	}

	if !sr.client.IsConnected() {
		return nil, corestream.ErrNotConnected
	}

	resp, err := sr.client.QueryBlock(blockNum, 30*time.Second)
	if err != nil {
		return nil, err
	}

	blob := bytesToBlockBlob(resp.Data)

	trxIndex := -1
	for i, id := range blob.Block.TrxIDInBlock {
		if fmt.Sprintf("%x", id[:]) == trxID {
			trxIndex = i
			break
		}
	}

	if trxIndex < 0 {
		return nil, fmt.Errorf("transaction %s not found in block %d", trxID, blockNum)
	}

	var actions []chain.ActionTrace
	for i, cat := range blob.Cats {
		if int(cat.TrxIDIndex) == trxIndex {
			globSeq := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
			action := cat.At(blob.Block, blockNum, globSeq)
			actions = append(actions, *action)
		}
	}

	meta := &blob.Block.TrxMetaInBlock[trxIndex]

	return &TransactionData{
		Actions:        actions,
		BlockTime:      blob.Block.BlockTime,
		Status:         meta.Status,
		CpuUsageUs:     meta.CpuUsageUs,
		NetUsageWords:  meta.NetUsageWords,
		Expiration:     meta.Expiration,
		RefBlockNum:    meta.RefBlockNum,
		RefBlockPrefix: meta.RefBlockPrefix,
		Signatures:     meta.Signatures,
		TrxIndex:       trxIndex,
	}, nil
}

var _ BaseReader = (*StreamReader)(nil)
