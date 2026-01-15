package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/greymass/roborovski/libraries/corereader"
)

type TrxIndex struct {
	basePath string
	reader   corereader.Reader

	partitions [PartitionCount]*Partition
	partMu     sync.RWMutex

	wal      *WAL
	walIndex *WALIndex

	meta   *IndexMeta
	metaMu sync.RWMutex
}

func NewTrxIndex(basePath string, reader corereader.Reader) (*TrxIndex, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("create base path: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(basePath, "partitions"), 0755); err != nil {
		return nil, fmt.Errorf("create partitions dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(basePath, "wal"), 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(basePath, "temp"), 0755); err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	metaPath := filepath.Join(basePath, "meta.json")
	var meta *IndexMeta
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		meta = NewIndexMeta()
		if err := meta.Save(metaPath); err != nil {
			return nil, fmt.Errorf("save initial meta: %w", err)
		}
	} else {
		var err error
		meta, err = LoadIndexMeta(metaPath)
		if err != nil {
			return nil, fmt.Errorf("load meta: %w", err)
		}
	}

	walPath := filepath.Join(basePath, "wal", "current.wal")
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	walIndex := NewWALIndex()
	if err := walIndex.LoadFromWAL(wal); err != nil {
		wal.Close()
		return nil, fmt.Errorf("load WAL index: %w", err)
	}

	idx := &TrxIndex{
		basePath: basePath,
		reader:   reader,
		wal:      wal,
		walIndex: walIndex,
		meta:     meta,
	}

	for i := range PartitionCount {
		idx.partitions[i] = NewPartition(uint16(i), basePath)
	}

	return idx, nil
}

func (idx *TrxIndex) Close() error {
	idx.partMu.Lock()
	defer idx.partMu.Unlock()

	for i := range PartitionCount {
		if idx.partitions[i] != nil {
			idx.partitions[i].Unload()
		}
	}

	if idx.wal != nil {
		idx.wal.Sync()
		idx.wal.Close()
	}

	idx.SaveMeta()
	return nil
}

func (idx *TrxIndex) SaveMeta() error {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()
	return idx.meta.Save(filepath.Join(idx.basePath, "meta.json"))
}

func (idx *TrxIndex) GetMeta() *IndexMeta {
	idx.metaMu.RLock()
	defer idx.metaMu.RUnlock()
	return idx.meta
}

func (idx *TrxIndex) SetLastIndexedBlock(blockNum uint32) {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()
	idx.meta.LastIndexedBlock = blockNum
}

func (idx *TrxIndex) SetLastMergedBlock(blockNum uint32) {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()
	idx.meta.LastMergedBlock = blockNum
	idx.meta.LastMergeAt = time.Now()
}

func (idx *TrxIndex) SetMode(mode string) {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()
	idx.meta.Mode = mode
}

func (idx *TrxIndex) SetBulkProgress(runCount int, lastBlock uint32) {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()
	idx.meta.BulkRunCount = runCount
	idx.meta.BulkLastBlock = lastBlock
}

func (idx *TrxIndex) ClearBulkProgress() {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()
	idx.meta.BulkRunCount = 0
	idx.meta.BulkLastBlock = 0
}

func (idx *TrxIndex) Lookup(trxIDHex string) (uint32, error) {
	prefix5, err := TrxIDToPrefix5(trxIDHex)
	if err != nil {
		return 0, err
	}

	candidates := idx.walIndex.Get(prefix5)
	for i := len(candidates) - 1; i >= 0; i-- {
		blockNum := candidates[i]
		if idx.verifyTransaction(blockNum, trxIDHex) {
			return blockNum, nil
		}
	}

	partitionID := Prefix5ToPartitionID(prefix5)
	prefix3 := Prefix5ToPrefix3(prefix5)

	idx.partMu.RLock()
	partition := idx.partitions[partitionID]
	idx.partMu.RUnlock()

	if err := partition.Load(); err != nil {
		return 0, fmt.Errorf("load partition %04x: %w", partitionID, err)
	}

	blockNums, found := partition.BinarySearch(prefix3)
	if !found {
		return 0, ErrNotFound
	}

	for i := len(blockNums) - 1; i >= 0; i-- {
		blockNum := blockNums[i]
		if idx.verifyTransaction(blockNum, trxIDHex) {
			return blockNum, nil
		}
	}

	return 0, ErrNotFound
}

func (idx *TrxIndex) verifyTransaction(blockNum uint32, trxIDHex string) bool {
	trxIDs, _, _, err := idx.reader.GetTransactionIDsOnly(blockNum, false)
	if err != nil {
		return false
	}

	for _, id := range trxIDs {
		if id == trxIDHex {
			return true
		}
	}
	return false
}

func (idx *TrxIndex) Add(prefix5 [5]byte, blockNum uint32) error {
	if err := idx.wal.Append(prefix5, blockNum); err != nil {
		return err
	}
	idx.walIndex.Add(prefix5, blockNum)
	return nil
}

func (idx *TrxIndex) AddBatch(entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := idx.wal.AppendBatch(entries); err != nil {
		return err
	}
	idx.walIndex.AddBatch(entries)
	return nil
}

func (idx *TrxIndex) WALEntryCount() int {
	return idx.walIndex.Len()
}

func (idx *TrxIndex) GetPartition(id uint16) *Partition {
	idx.partMu.RLock()
	defer idx.partMu.RUnlock()
	return idx.partitions[id]
}

var ErrNotFound = fmt.Errorf("transaction not found")
