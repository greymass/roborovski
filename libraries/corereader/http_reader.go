package corereader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/greymass/roborovski/libraries/corestream"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/encoding"
)

type HTTPReader struct {
	baseURL      string
	client       *http.Client
	transport    *http.Transport
	isUnixSocket bool
	cancel       context.CancelFunc
	ctx          context.Context
	mu           sync.RWMutex
	cachedHEAD   uint32
	cachedLIB    uint32
	cacheTime    time.Time
	cacheTTL     time.Duration
}

func NewHTTPReader(address string, isUnixSocket bool) (*HTTPReader, error) {
	var client *http.Client
	var transport *http.Transport
	var baseURL string

	if isUnixSocket {
		dialer := &net.Dialer{}
		transport = &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", address)
			},
		}
		client = &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		}
		baseURL = "http://unix"
	} else {
		transport = &http.Transport{}
		client = &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		}
		baseURL = address
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &HTTPReader{
		baseURL:      baseURL,
		client:       client,
		transport:    transport,
		isUnixSocket: isUnixSocket,
		ctx:          ctx,
		cancel:       cancel,
		cacheTTL:     500 * time.Millisecond,
	}, nil
}

func (hr *HTTPReader) Close() error {
	hr.cancel()
	hr.transport.CloseIdleConnections()
	hr.client.CloseIdleConnections()
	return nil
}

func (hr *HTTPReader) GetBlockCacheStats() (hits, misses uint64, sizeBytes int64, numBlocks int) {
	return 0, 0, 0, 0
}

func (hr *HTTPReader) GetStateProps(bypassCache bool) (head uint32, lib uint32, err error) {
	hr.mu.RLock()
	if !bypassCache && time.Since(hr.cacheTime) < hr.cacheTTL {
		head, lib = hr.cachedHEAD, hr.cachedLIB
		hr.mu.RUnlock()
		return head, lib, nil
	}
	hr.mu.RUnlock()

	req, err := http.NewRequestWithContext(hr.ctx, "GET", hr.baseURL+"/v1/history/get_state_binary", nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := hr.client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get state: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, 0, fmt.Errorf("get_state_binary failed: %s", string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read response: %w", err)
	}

	stateResp, err := corestream.DecodeGetStateResponse(data)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode state response: %w", err)
	}

	hr.mu.Lock()
	hr.cachedHEAD = stateResp.HEAD
	hr.cachedLIB = stateResp.LIB
	hr.cacheTime = time.Now()
	hr.mu.Unlock()

	return stateResp.HEAD, stateResp.LIB, nil
}

func (hr *HTTPReader) getBlock(blockNum uint32) (*corestream.GetBlockResponse, error) {
	reqBody := fmt.Sprintf(`{"block_num":%d}`, blockNum)

	req, err := http.NewRequestWithContext(hr.ctx, "POST", hr.baseURL+"/v1/history/get_block_binary", bytes.NewReader([]byte(reqBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := hr.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get_block_binary failed: %s", string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return corestream.DecodeGetBlockResponse(data)
}

func (hr *HTTPReader) GetNotificationsOnly(blockNum uint32) (map[uint64][]uint64, string, string, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlob(blockResp.Data)
	notifs := extractNotifications(blob)

	blockID := fmt.Sprintf("%x", blockResp.BlockID)
	previous := fmt.Sprintf("%x", blockResp.Previous)

	return notifs, blockID, previous, nil
}

func (hr *HTTPReader) GetNotificationsWithActionMetadata(blockNum uint32) (map[uint64][]uint64, []ActionMetadata, string, string, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, nil, "", "", err
	}

	blob := bytesToBlockBlob(blockResp.Data)
	notifs, actionMeta := extractNotificationsWithMetadata(blob)

	blockID := fmt.Sprintf("%x", blockResp.BlockID)
	previous := fmt.Sprintf("%x", blockResp.Previous)

	return notifs, actionMeta, blockID, previous, nil
}

func (hr *HTTPReader) GetNotificationsWithActionMetadataFiltered(blockNum uint32, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, string, string, int, uint32, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, nil, "", "", 0, 0, err
	}

	blob := bytesToBlockBlob(blockResp.Data)
	notifs, actionMeta, filteredCount := extractNotificationsWithMetadataFiltered(blob, filterFunc)

	blockID := fmt.Sprintf("%x", blockResp.BlockID)
	previous := fmt.Sprintf("%x", blockResp.Previous)
	blockTime := chain.TimeToUint32(blob.Block.BlockTime)

	return notifs, actionMeta, blockID, previous, filteredCount, blockTime, nil
}

func (hr *HTTPReader) GetRawBlockBatch(startBlock, endBlock uint32) ([]RawBlock, error) {
	return hr.GetRawBlockBatchFiltered(startBlock, endBlock, nil)
}

func (hr *HTTPReader) GetRawBlockBatchFiltered(startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	reqBody, err := encoding.JSONiter.Marshal(map[string]uint32{
		"start_block": startBlock,
		"end_block":   endBlock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(hr.ctx, 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST",
		hr.baseURL+"/v1/history/get_blocks_batch_binary",
		bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := hr.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	return hr.parseBlockBatchResponse(resp.Body, startBlock, endBlock, filterFunc)
}

func (hr *HTTPReader) parseBlockBatchResponse(r io.Reader, startBlock, endBlock uint32, filterFunc ActionFilterFunc) ([]RawBlock, error) {
	expectedBlocks := int(endBlock - startBlock + 1)
	results := make([]RawBlock, 0, expectedBlocks)

	expectedBlock := startBlock

	for {
		var blockNum uint32
		if err := binary.Read(r, binary.BigEndian, &blockNum); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("stream error at block %d: %w", expectedBlock, err)
		}

		if blockNum != expectedBlock {
			return nil, fmt.Errorf("block sequence error: expected %d, got %d (missing blocks: %d-%d)",
				expectedBlock, blockNum, expectedBlock, blockNum-1)
		}

		var dataLen uint32
		if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("failed to read length for block %d: %w", blockNum, err)
		}

		if dataLen == 0 {
			return nil, fmt.Errorf("block %d has zero length", blockNum)
		}
		if dataLen > 50*1024*1024 {
			return nil, fmt.Errorf("block %d has invalid length: %d bytes (too large)", blockNum, dataLen)
		}

		blockData := make([]byte, dataLen)
		if _, err := io.ReadFull(r, blockData); err != nil {
			return nil, fmt.Errorf("incomplete block %d: expected %d bytes, error: %w",
				blockNum, dataLen, err)
		}

		blob := bytesToBlockBlob(blockData)
		notifs, actionMeta, _ := extractNotificationsWithMetadataFiltered(blob, filterFunc)
		blockTime := chain.TimeToUint32(blob.Block.BlockTime)

		results = append(results, RawBlock{
			BlockNum:      blockNum,
			BlockTime:     blockTime,
			Notifications: notifs,
			ActionMeta:    actionMeta,
		})

		expectedBlock++
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no blocks received (expected %d-%d)", startBlock, endBlock)
	}

	lastReceived := results[len(results)-1].BlockNum
	if lastReceived != endBlock {
		return nil, fmt.Errorf("incomplete batch: received blocks %d-%d, missing %d-%d",
			startBlock, lastReceived, lastReceived+1, endBlock)
	}

	return results, nil
}

func (hr *HTTPReader) GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *FetchTimings, error) {
	if len(globalSeqs) == 0 {
		return []chain.ActionTrace{}, nil, nil
	}

	reqBody, err := encoding.JSONiter.Marshal(map[string]interface{}{
		"global_seqs": globalSeqs,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(hr.ctx, "POST", hr.baseURL+"/v1/history/get_actions_by_globs_binary", bytes.NewReader(reqBody))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := hr.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get actions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("get_actions_by_globs_binary failed: %s", string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	actionsResp, err := corestream.DecodeGetActionsByGlobsResponse(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode actions response: %w", err)
	}

	results := make([]chain.ActionTrace, len(actionsResp.Actions))
	for i, actionData := range actionsResp.Actions {
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

func (hr *HTTPReader) GetRawActionsFiltered(blockNum uint32, contractFilter, actionFilter string) ([]chain.ActionTrace, string, string, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlob(blockResp.Data)
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

	blockID := fmt.Sprintf("%x", blockResp.BlockID)
	previous := fmt.Sprintf("%x", blockResp.Previous)

	return actions, blockID, previous, nil
}

func extractNotifications(blob *blockBlob) map[uint64][]uint64 {
	notifs := make(map[uint64][]uint64)

	for i, cat := range blob.Cats {
		glob := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])

		notifiedSet := make(map[uint64]struct{}, len(cat.Auths)+1)

		receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
		notifiedSet[receiver] = struct{}{}
		notifs[receiver] = append(notifs[receiver], glob)

		for _, auth := range cat.Auths {
			account := blob.Block.NamesInBlock[auth.AccountIndex]
			if _, exists := notifiedSet[account]; !exists {
				notifiedSet[account] = struct{}{}
				notifs[account] = append(notifs[account], glob)
			}
		}
	}

	return notifs
}

func extractNotificationsWithMetadata(blob *blockBlob) (map[uint64][]uint64, []ActionMetadata) {
	notifs := make(map[uint64][]uint64)
	actionMeta := make([]ActionMetadata, 0, len(blob.Cats))

	for i, cat := range blob.Cats {
		glob := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
		contract := blob.Block.NamesInBlock[cat.ContractNameIndex]
		action := blob.Block.NamesInBlock[cat.ActionNameIndex]

		actionMeta = append(actionMeta, ActionMetadata{
			GlobalSeq: glob,
			Contract:  contract,
			Action:    action,
		})

		notifiedSet := make(map[uint64]struct{}, len(cat.Auths)+1)

		receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
		notifiedSet[receiver] = struct{}{}
		notifs[receiver] = append(notifs[receiver], glob)

		for _, auth := range cat.Auths {
			account := blob.Block.NamesInBlock[auth.AccountIndex]
			if _, exists := notifiedSet[account]; !exists {
				notifiedSet[account] = struct{}{}
				notifs[account] = append(notifs[account], glob)
			}
		}
	}

	return notifs, actionMeta
}

func extractNotificationsWithMetadataFiltered(blob *blockBlob, filterFunc ActionFilterFunc) (map[uint64][]uint64, []ActionMetadata, int) {
	notifs := make(map[uint64][]uint64)
	actionMeta := make([]ActionMetadata, 0, len(blob.Cats))
	filteredCount := 0

	for i, cat := range blob.Cats {
		glob := blob.Block.MinGlobInBlock + uint64(i) + uint64(blob.CatsOffset[i])
		contract := blob.Block.NamesInBlock[cat.ContractNameIndex]
		action := blob.Block.NamesInBlock[cat.ActionNameIndex]

		if filterFunc != nil && filterFunc(contract, action) {
			filteredCount++
			continue
		}

		actionMeta = append(actionMeta, ActionMetadata{
			GlobalSeq: glob,
			Contract:  contract,
			Action:    action,
		})

		notifiedSet := make(map[uint64]struct{}, len(cat.Auths)+1)

		receiver := blob.Block.NamesInBlock[cat.ReceiverIndex]
		notifiedSet[receiver] = struct{}{}
		notifs[receiver] = append(notifs[receiver], glob)

		for _, auth := range cat.Auths {
			account := blob.Block.NamesInBlock[auth.AccountIndex]
			if _, exists := notifiedSet[account]; !exists {
				notifiedSet[account] = struct{}{}
				notifs[account] = append(notifs[account], glob)
			}
		}
	}

	return notifs, actionMeta, filteredCount
}

func catToActionTrace(cat *compressedActionTrace, blkData *blockData, blockNum uint32, glob uint64) *chain.ActionTrace {
	return cat.At(blkData, blockNum, glob)
}

func (hr *HTTPReader) FindFirstAvailableBlock(afterBlock uint32) (uint32, bool) {
	return afterBlock, true
}

func (hr *HTTPReader) GetTransactionIDsOnly(blockNum uint32, skipOnblock bool) ([]string, string, string, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, "", "", err
	}

	blob := bytesToBlockBlobBlockOnly(blockResp.Data)

	blockID := fmt.Sprintf("%x", blockResp.BlockID)
	previous := fmt.Sprintf("%x", blockResp.Previous)

	trxIDs := make([]string, 0, len(blob.TrxIDInBlock))
	for _, trxID := range blob.TrxIDInBlock {
		trxIDs = append(trxIDs, fmt.Sprintf("%x", trxID[:]))
	}

	return trxIDs, blockID, previous, nil
}

// GetTransactionIDsRaw returns raw transaction IDs as [32]byte without hex encoding.
func (hr *HTTPReader) GetTransactionIDsRaw(blockNum uint32) ([][32]byte, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, err
	}

	blob := bytesToBlockBlobBlockOnly(blockResp.Data)
	return blob.TrxIDInBlock, nil
}

func (hr *HTTPReader) GetTransactionData(blockNum uint32, trxID string) (*TransactionData, error) {
	blockResp, err := hr.getBlock(blockNum)
	if err != nil {
		return nil, err
	}

	blob := bytesToBlockBlob(blockResp.Data)

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

var _ BaseReader = (*HTTPReader)(nil)
