package internal

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type LiveSyncer struct {
	idx            *TrxIndex
	blockSource    BlockSource
	config         *Config
	mergeInterval  time.Duration
	mergeThreshold int
	running        atomic.Bool
	stopCh         chan struct{}
	doneCh         chan struct{}
}

func NewLiveSyncer(idx *TrxIndex, blockSource BlockSource, config *Config) *LiveSyncer {
	return &LiveSyncer{
		idx:            idx,
		blockSource:    blockSource,
		config:         config,
		mergeInterval:  config.MergeInterval(),
		mergeThreshold: config.MergeThreshold,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
}

func (s *LiveSyncer) Start() error {
	if s.running.Swap(true) {
		return nil
	}

	go s.syncLoop()
	return nil
}

func (s *LiveSyncer) Stop() {
	if !s.running.Load() {
		return
	}
	close(s.stopCh)
	<-s.doneCh
}

func (s *LiveSyncer) syncLoop() {
	defer s.running.Store(false)
	defer close(s.doneCh)

	meta := s.idx.GetMeta()
	currentBlock := meta.LastIndexedBlock

	lastLogTime := time.Now()
	lastLogBlock := currentBlock
	lastMergeCheck := time.Now()
	trxCount := 0

	logger.Printf("sync", "LIVE SYNC: Starting from block %d", currentBlock+1)

	for {
		select {
		case <-s.stopCh:
			logger.Printf("sync", "LIVE SYNC: Stopping...")
			s.idx.wal.Sync()
			s.idx.SaveMeta()
			return
		default:
		}

		block, err := s.blockSource.GetNextBlockWithTimeout(100 * time.Millisecond)
		if err != nil {
			logger.Printf("sync", "Block source error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if block == nil {
			continue
		}

		count, err := s.processBlock(block)
		if err != nil {
			logger.Printf("sync", "Error processing block %d: %v", block.BlockNum, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		trxCount += count
		currentBlock = block.BlockNum
		s.idx.SetLastIndexedBlock(currentBlock)

		_, chainLIB, _ := s.blockSource.GetStateProps()

		now := time.Now()
		if now.Sub(lastLogTime) >= s.config.GetLogInterval() {
			elapsed := now.Sub(lastLogTime).Seconds()
			bps := float64(currentBlock-lastLogBlock) / elapsed
			tps := float64(trxCount) / elapsed

			walCount := s.idx.WALEntryCount()
			logger.Printf("sync", "Block: %10d / %10d | %s tps | %s bps | WAL: %d",
				currentBlock, chainLIB, logger.FormatNumber(tps), logger.FormatNumber(bps), walCount)

			lastLogTime = now
			lastLogBlock = currentBlock
			trxCount = 0
		}

		if now.Sub(lastMergeCheck) >= s.mergeInterval {
			walCount := s.idx.WALEntryCount()
			if walCount >= s.mergeThreshold {
				logger.Printf("sync", "Triggering WAL merge (%d entries)", walCount)
				if err := s.TriggerMerge(); err != nil {
					logger.Printf("sync", "Merge error: %v", err)
				}
			}
			lastMergeCheck = now
		}
	}
}

func (s *LiveSyncer) processBlock(block *BlockData) (int, error) {
	var entries []WALEntry
	for _, trxIDHex := range block.TrxIDs {
		prefix5, err := TrxIDToPrefix5(trxIDHex)
		if err != nil {
			continue
		}
		entries = append(entries, WALEntry{
			Prefix5:  prefix5,
			BlockNum: block.BlockNum,
		})
	}

	if len(entries) > 0 {
		if err := s.idx.AddBatch(entries); err != nil {
			return 0, err
		}
	}

	return len(entries), nil
}

func (s *LiveSyncer) TriggerMerge() error {
	walEntries := s.idx.walIndex.GetAllEntries()
	if len(walEntries) == 0 {
		return nil
	}

	logger.Printf("sync", "Merging %d WAL entries into partitions", len(walEntries))

	partitionEntries := make(map[uint16][]struct {
		prefix3  [3]byte
		blockNum uint32
	})

	for _, entry := range walEntries {
		partitionID := Prefix5ToPartitionID(entry.Prefix5)
		prefix3 := Prefix5ToPrefix3(entry.Prefix5)
		partitionEntries[partitionID] = append(partitionEntries[partitionID], struct {
			prefix3  [3]byte
			blockNum uint32
		}{prefix3, entry.BlockNum})
	}

	for partitionID, entries := range partitionEntries {
		partition := s.idx.GetPartition(partitionID)

		if err := partition.Load(); err != nil {
			logger.Printf("sync", "Warning: could not load partition %04x: %v", partitionID, err)
		}

		path := PartitionIDToPath(s.idx.basePath, partitionID)
		tmpPath := path + ".tmp"
		writer := NewPartitionWriter(tmpPath)

		if partition.loaded && partition.header != nil {
			data := partition.mmapData[PartitionHeaderSize:]
			count := int(partition.header.EntryCount)
			for i := range count {
				off := i * PartitionEntrySize
				entry, _ := DecodePartitionEntry(data[off : off+PartitionEntrySize])
				if entry != nil {
					writer.AddEntry(entry.Prefix3, entry.BlockNum)
				}
			}
		}

		for _, e := range entries {
			writer.AddEntry(e.prefix3, e.blockNum)
		}

		if err := writer.Finalize(); err != nil {
			return err
		}

		partition.Unload()

		if err := os.Rename(tmpPath, path); err != nil {
			return err
		}
	}

	if err := s.idx.wal.Truncate(); err != nil {
		return err
	}
	s.idx.walIndex.Clear()

	meta := s.idx.GetMeta()
	s.idx.SetLastMergedBlock(meta.LastIndexedBlock)
	s.idx.SaveMeta()

	logger.Printf("sync", "Merge complete: %d partitions updated", len(partitionEntries))
	return nil
}
