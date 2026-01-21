package corereader

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
)

type OpenConfig struct {
	StartBlock         uint32
	RetryTimeout       time.Duration
	PollInterval       time.Duration
	QueryReader        BaseReader
	SliceReaderOptions *SliceReaderOptions
}

func DefaultOpenConfig() OpenConfig {
	return OpenConfig{
		StartBlock:   2,
		RetryTimeout: 30 * time.Second,
		PollInterval: 500 * time.Millisecond,
	}
}

func Open(source string, cfg OpenConfig) (Reader, error) {
	if source == "" {
		return nil, fmt.Errorf("history source is required: set --history-source flag or history-source in config.ini (local path, http://, http+unix://, or stream:// URL)")
	}

	if cfg.RetryTimeout == 0 {
		cfg.RetryTimeout = 30 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}

	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		return openHTTP(source, false, cfg)
	}

	if strings.HasPrefix(source, "http+unix://") {
		return openHTTP(strings.TrimPrefix(source, "http+unix://"), true, cfg)
	}

	if strings.HasPrefix(source, "stream://") {
		return openStream(source, cfg)
	}

	return openLocal(source, cfg)
}

func openLocal(path string, cfg OpenConfig) (Reader, error) {
	var reader *SliceReader
	var err error

	if cfg.SliceReaderOptions != nil {
		reader, err = NewSliceReaderWithOptions(path, *cfg.SliceReaderOptions)
	} else {
		reader, err = NewSliceReader(path)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open local storage: %w", err)
	}

	return &syncReader{
		reader:       reader,
		currentBlock: cfg.StartBlock,
		pollInterval: cfg.PollInterval,
	}, nil
}

func openHTTP(address string, isUnixSocket bool, cfg OpenConfig) (Reader, error) {
	var reader *HTTPReader
	var err error

	retryCfg := RetryConfig{Timeout: cfg.RetryTimeout, MaxDelay: 32 * time.Second}
	err = RetryWithBackoff(retryCfg, "HTTP", func() error {
		reader, err = NewHTTPReader(address, isUnixSocket)
		if err != nil {
			return err
		}
		_, _, err = reader.GetStateProps(true)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to HTTP server: %w", err)
	}

	if isUnixSocket {
		logger.Printf("config", "Connected to HTTP server via unix socket: %s", address)
	} else {
		logger.Printf("config", "Connected to HTTP server: %s", address)
	}

	return &syncReader{
		reader:       reader,
		currentBlock: cfg.StartBlock,
		pollInterval: cfg.PollInterval,
	}, nil
}

func openStream(address string, cfg OpenConfig) (Reader, error) {
	addr := strings.TrimPrefix(address, "stream://")

	var streamReader *StreamReader
	var err error

	retryCfg := RetryConfig{Timeout: cfg.RetryTimeout, MaxDelay: 32 * time.Second}
	err = RetryWithBackoff(retryCfg, "Stream", func() error {
		streamReader, err = NewStreamReader(addr, cfg.QueryReader)
		if err != nil {
			return err
		}
		if err := streamReader.Connect(cfg.StartBlock); err != nil {
			streamReader.Close()
			return err
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to stream server: %w", err)
	}

	logger.Printf("config", "Connected to stream server: %s (starting at block %d)", address, cfg.StartBlock)

	return &streamSyncReader{
		stream:       streamReader,
		currentBlock: cfg.StartBlock,
	}, nil
}

type syncReader struct {
	reader       BaseReader
	currentBlock uint32
	pollInterval time.Duration
	mu           sync.RWMutex
	retryDelay   time.Duration
}

func (r *syncReader) GetBaseReader() BaseReader {
	return r.reader
}

func (r *syncReader) GetNextBatch(maxBlocks int, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	for {
		_, lib, err := r.reader.GetStateProps(true)
		if err != nil {
			if !IsRetriableError(err) {
				return nil, err
			}
			r.backoff()
			continue
		}
		r.resetBackoff()

		r.mu.RLock()
		current := r.currentBlock
		r.mu.RUnlock()

		if current > lib {
			time.Sleep(r.pollInterval)
			return []RawBlock{}, nil
		}

		endBlock := current + uint32(maxBlocks) - 1
		if endBlock > lib {
			endBlock = lib
		}

		var results []RawBlock
		if filterFunc != nil {
			results, err = r.reader.GetRawBlockBatchFiltered(current, endBlock, filterFunc)
		} else {
			results, err = r.reader.GetRawBlockBatch(current, endBlock)
		}

		if err != nil {
			if !IsRetriableError(err) {
				return nil, err
			}
			r.backoff()
			continue
		}
		r.resetBackoff()

		r.mu.Lock()
		r.currentBlock = endBlock + 1
		r.mu.Unlock()

		return results, nil
	}
}

func (r *syncReader) SetCurrentBlock(block uint32) {
	r.mu.Lock()
	r.currentBlock = block
	r.mu.Unlock()
}

func (r *syncReader) CurrentBlock() uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentBlock
}

func (r *syncReader) backoff() {
	if r.retryDelay == 0 {
		r.retryDelay = time.Second
	} else {
		r.retryDelay *= 2
		if r.retryDelay > 30*time.Second {
			r.retryDelay = 30 * time.Second
		}
	}
	logger.Printf("sync", "Connection error, retrying in %v...", r.retryDelay)
	time.Sleep(r.retryDelay)
}

func (r *syncReader) resetBackoff() {
	r.retryDelay = 0
}

func (r *syncReader) GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error) {
	return r.reader.GetNotificationsOnly(blockNum)
}

func (r *syncReader) GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error) {
	return r.reader.GetNotificationsWithActionMetadata(blockNum)
}

func (r *syncReader) GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error) {
	return r.reader.GetNotificationsWithActionMetadataFiltered(blockNum, filterFunc)
}

func (r *syncReader) GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error) {
	return r.reader.GetRawBlockBatch(startBlock, endBlock)
}

func (r *syncReader) GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	return r.reader.GetRawBlockBatchFiltered(startBlock, endBlock, filterFunc)
}

func (r *syncReader) GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error) {
	return r.reader.GetActionsByGlobalSeqs(globalSeqs)
}

func (r *syncReader) GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error) {
	return r.reader.GetRawActionsFiltered(blockNum, contractFilter, actionFilter)
}

func (r *syncReader) GetStateProps(bypassCache bool) (head uint32, lib uint32, err error) {
	return r.reader.GetStateProps(bypassCache)
}

func (r *syncReader) GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int) {
	return r.reader.GetBlockCacheStats()
}

func (r *syncReader) Close() error {
	return r.reader.Close()
}

func (r *syncReader) FindFirstAvailableBlock(afterBlock uint32) (uint32, bool) {
	return r.reader.FindFirstAvailableBlock(afterBlock)
}

func (r *syncReader) GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error) {
	return r.reader.GetTransactionIDsOnly(blockNum, skipOnblock)
}

func (r *syncReader) GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error) {
	return r.reader.GetTransactionIDsRaw(blockNum)
}

func (r *syncReader) GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error) {
	return r.reader.GetTransactionData(blockNum, trxID)
}

type streamSyncReader struct {
	stream       *StreamReader
	currentBlock uint32
	retryDelay   atomic.Int64
}

func (r *streamSyncReader) GetNextBatch(maxBlocks int, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	results := make([]RawBlock, 0, maxBlocks)

	for len(results) < maxBlocks {
		var block *RawBlock
		var err error

		timeout := 100 * time.Millisecond
		if len(results) > 0 {
			timeout = 10 * time.Millisecond
		}

		if filterFunc != nil {
			block, err = r.stream.NextBlockWithTimeoutFiltered(timeout, filterFunc)
		} else {
			block, err = r.stream.NextBlockWithTimeout(timeout)
		}

		if err != nil {
			if !IsRetriableError(err) {
				return results, err
			}
			r.backoff()
			if err := r.stream.Connect(r.currentBlock); err != nil {
				continue
			}
			r.resetBackoff()
			continue
		}
		r.resetBackoff()

		if block == nil {
			break
		}

		results = append(results, *block)
		r.currentBlock = block.BlockNum + 1
	}

	return results, nil
}

func (r *streamSyncReader) SetCurrentBlock(block uint32) {
	r.currentBlock = block
}

func (r *streamSyncReader) CurrentBlock() uint32 {
	return r.currentBlock
}

func (r *streamSyncReader) IsStreaming() bool {
	return true
}

func (r *streamSyncReader) backoff() {
	delay := r.retryDelay.Load()
	if delay == 0 {
		delay = int64(time.Second)
	} else {
		delay *= 2
		if delay > int64(30*time.Second) {
			delay = int64(30 * time.Second)
		}
	}
	r.retryDelay.Store(delay)
	logger.Printf("sync", "Stream connection error, retrying in %v...", time.Duration(delay))
	time.Sleep(time.Duration(delay))
}

func (r *streamSyncReader) resetBackoff() {
	r.retryDelay.Store(0)
}

func (r *streamSyncReader) GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error) {
	return r.stream.GetNotificationsOnly(blockNum)
}

func (r *streamSyncReader) GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error) {
	return r.stream.GetNotificationsWithActionMetadata(blockNum)
}

func (r *streamSyncReader) GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error) {
	return r.stream.GetNotificationsWithActionMetadataFiltered(blockNum, filterFunc)
}

func (r *streamSyncReader) GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error) {
	return r.stream.GetRawBlockBatch(startBlock, endBlock)
}

func (r *streamSyncReader) GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	return r.stream.GetRawBlockBatchFiltered(startBlock, endBlock, filterFunc)
}

func (r *streamSyncReader) GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error) {
	return r.stream.GetActionsByGlobalSeqs(globalSeqs)
}

func (r *streamSyncReader) GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error) {
	return r.stream.GetRawActionsFiltered(blockNum, contractFilter, actionFilter)
}

func (r *streamSyncReader) GetStateProps(bypassCache bool) (head uint32, lib uint32, err error) {
	return r.stream.GetStateProps(bypassCache)
}

func (r *streamSyncReader) GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int) {
	return r.stream.GetBlockCacheStats()
}

func (r *streamSyncReader) Close() error {
	return r.stream.Close()
}

func (r *streamSyncReader) FindFirstAvailableBlock(afterBlock uint32) (uint32, bool) {
	return r.stream.FindFirstAvailableBlock(afterBlock)
}

func (r *streamSyncReader) GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error) {
	return r.stream.GetTransactionIDsOnly(blockNum, skipOnblock)
}

func (r *streamSyncReader) GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error) {
	return r.stream.GetTransactionIDsRaw(blockNum)
}

func (r *streamSyncReader) GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error) {
	return r.stream.GetTransactionData(blockNum, trxID)
}

var _ Reader = (*syncReader)(nil)
var _ Reader = (*streamSyncReader)(nil)
var _ StreamingReader = (*streamSyncReader)(nil)
