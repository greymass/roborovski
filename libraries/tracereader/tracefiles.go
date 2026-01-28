package tracereader

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/greymass/go-eosio/pkg/chain"
	"github.com/greymass/roborovski/libraries/logger"
)

const HeaderVersion uint32 = 1

var ErrNotFound = errors.New("not found")

type Config struct {
	Debug  bool
	Stride uint32
	Dir    string
}

type BlockEntryV0 struct {
	Id     chain.Checksum256
	Number uint32
	Offset uint64
}

type LibEntryV0 struct {
	Lib uint32
}

type IndexEntryVariant struct {
	Block *BlockEntryV0
	Lib   *LibEntryV0
}

type EntryVariant struct {
	V0 *BlockTraceV0
	V1 *BlockTraceV1
	V2 *BlockTraceV2
}

type BlockTraceV0 struct {
	Id           chain.Checksum256    `json:"id"`
	Number       uint32               `json:"number"`
	PreviousId   chain.Checksum256    `json:"previous_id"`
	Timestamp    chain.BlockTimestamp `json:"timestamp"`
	Producer     chain.Name           `json:"producer"`
	Transactions []TransactionTraceV0 `json:"transactions"`
}

type BlockTraceV1 struct {
	BlockTraceV0
	TransactionMroot chain.Checksum256    `json:"transaction_mroot"`
	ActionMroot      chain.Checksum256    `json:"action_mroot"`
	ScheduleVersion  uint32               `json:"schedule_version"`
	TransactionsV1   []TransactionTraceV1 `json:"transactions"`
}

type BlockTraceV2 struct {
	Id                  chain.Checksum256    `json:"id"`
	Number              uint32               `json:"number"`
	PreviousId          chain.Checksum256    `json:"previous_id"`
	Timestamp           chain.BlockTimestamp `json:"timestamp"`
	Producer            chain.Name           `json:"producer"`
	TransactionMroot    chain.Checksum256    `json:"transaction_mroot"`
	ActionMroot         chain.Checksum256    `json:"action_mroot"`
	ScheduleVersion     uint32               `json:"schedule_version"`
	TransactionsVariant uint                 `json:"-"` // variant of one
	Transactions        []TransactionTraceV2 `json:"transactions"`
}

type TransactionTraceV0 struct {
	Id      chain.Checksum256 `json:"id"`
	Actions []ActionTraceV0   `json:"actions"`
}

type TransactionTraceV1 struct {
	TransactionTraceV0
	Status        uint8                   `json:"status"`
	CpuUsageUs    uint32                  `json:"cpu_usage_us"`
	NetUsageWords uint                    `json:"net_usage_words"`
	Signatures    []chain.Signature       `json:"signatures"`
	TrxHeader     chain.TransactionHeader `json:"transaction_header"`
}

type TransactionTraceV2 struct {
	Id             chain.Checksum256       `json:"id"`
	ActionsVariant uint                    `json:"-"` // variant of one
	Actions        []ActionTraceV1         `json:"actions"`
	Status         uint8                   `json:"status"`
	CpuUsageUs     uint32                  `json:"cpu_usage_us"`
	NetUsageWords  uint                    `json:"net_usage_words"`
	Signatures     []chain.Signature       `json:"signatures"`
	TrxHeader      chain.TransactionHeader `json:"transaction_header"`
}

type AccountDelta struct {
	Account chain.Name `json:"account"`
	Delta   int64      `json:"delta"`
}

type AuthSequence map[chain.Name]uint64

type ActionTraceV0 struct {
	ActionOrdinal                          uint                    `json:"action_ordinal"`
	CreatorActionOrdinal                   uint                    `json:"creator_action_ordinal"`
	ClosestUnnotifiedAncestorActionOrdinal uint                    `json:"closest_unnotified_ancestor_action_ordinal"`
	ReceiptReceiver                        chain.Name              `json:"receipt_receiver"`
	GlobalSequence                         uint64                  `json:"global_sequence"`
	RecvSequence                           uint64                  `json:"recv_sequence"`
	AuthSequence                           AuthSequence            `json:"auth_sequence"`
	CodeSequence                           uint                    `json:"code_sequence"`
	AbiSequence                            uint                    `json:"abi_sequence"`
	Receiver                               chain.Name              `json:"receiver"`
	Account                                chain.Name              `json:"account"`
	Name                                   chain.Name              `json:"name"`
	Authorization                          []chain.PermissionLevel `json:"authorization"`
	Data                                   chain.Bytes             `json:"data"`
	ContextFree                            bool                    `json:"context_free"`
	Elapsed                                int64                   `json:"elapsed"` // microseconds
	AccountRamDeltas                       []AccountDelta          `json:"account_ram_deltas"`
}

type ActionTraceV1 struct {
	ActionTraceV0
	ReturnValue chain.Bytes `json:"return_value"`
}

type RawBlockData struct {
	BlockNum uint32
	RawBytes []byte
}

func GetRawBlocksWithMetadata(blockNum uint32, limit int, conf *Config) ([]RawBlockData, error) {
	stride := strideFmt(blockNum, conf)
	sliceIndex, err := openSliceIndex(fmt.Sprintf("%s/trace_index_%s.log", conf.Dir, stride), conf)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer sliceIndex.Close()

	var found bool = false
	var offsets []uint64 = make([]uint64, 0)

	var entry IndexEntryVariant
	dec := chain.NewDecoder(sliceIndex)
	for {
		err = dec.DecodeVariant(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if entry.Block != nil && entry.Block.Number >= blockNum {
			if entry.Block.Number == blockNum {
				found = true
			}
			offsets = append(offsets, entry.Block.Offset)
			if limit > 0 && len(offsets) >= limit {
				err = dec.DecodeVariant(&entry)
				if err != io.EOF && entry.Block != nil {
					offsets = append(offsets, entry.Block.Offset)
				}
				break
			}
		}
	}

	if !found {
		return nil, ErrNotFound
	}

	numBlocks := limit
	if limit <= 0 || len(offsets) < limit {
		numBlocks = len(offsets)
		if numBlocks > 0 {
			numBlocks--
		}
	}

	slice, err := os.OpenFile(fmt.Sprintf("%s/trace_%s.log", conf.Dir, stride), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer slice.Close()

	sliceInfo, err := slice.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat trace file: %w", err)
	}
	traceFileSize := sliceInfo.Size()

	rawBlocks := make([]RawBlockData, numBlocks)

	for i := 0; i < numBlocks; i++ {
		var blockSize uint64
		if i+1 < len(offsets) {
			blockSize = offsets[i+1] - offsets[i]
		} else {
			blockSize = uint64(traceFileSize) - offsets[i]
		}

		currentBlockNum := blockNum + uint32(i)

		if int64(offsets[i]) >= traceFileSize {
			return nil, fmt.Errorf("block %d: offset %d exceeds trace file size %d (stride=%s)",
				currentBlockNum, offsets[i], traceFileSize, stride)
		}

		if int64(offsets[i]+blockSize) > traceFileSize {
			actualAvailable := traceFileSize - int64(offsets[i])
			return nil, fmt.Errorf("block %d: expected %d bytes at offset %d but only %d bytes available in trace file (size=%d, stride=%s)",
				currentBlockNum, blockSize, offsets[i], actualAvailable, traceFileSize, stride)
		}

		_, err = slice.Seek(int64(offsets[i]), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to block %d offset %d: %w", currentBlockNum, offsets[i], err)
		}

		rawBytes := make([]byte, blockSize)
		n, err := slice.Read(rawBytes)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read raw bytes for block %d: %w", currentBlockNum, err)
		}

		if uint64(n) != blockSize {
			return nil, fmt.Errorf("block %d: read %d bytes but expected %d (offset=%d, fileSize=%d, stride=%s)",
				currentBlockNum, n, blockSize, offsets[i], traceFileSize, stride)
		}

		rawBlocks[i] = RawBlockData{
			BlockNum: currentBlockNum,
			RawBytes: rawBytes,
		}
	}

	return rawBlocks, nil
}

type GetInfoResponse struct {
	Lib  uint32 `json:"last_irreversible_block_num"`
	Head uint32 `json:"head_block_num"`
}

var (
	getInfoCacheMu       sync.Mutex
	getInfoCacheDir      string
	getInfoCacheTime     time.Time
	getInfoCacheValue    GetInfoResponse
	getInfoCacheLastFile string
	getInfoCacheTTL      = 500 * time.Millisecond
)

func GetInfo(conf *Config) (GetInfoResponse, error) {
	getInfoCacheMu.Lock()
	age := time.Since(getInfoCacheTime)
	dirMatch := getInfoCacheDir == conf.Dir
	cacheValid := dirMatch && age < getInfoCacheTTL

	if conf.Debug {
		logger.Printf("debug", "GetInfo cache check: dirMatch=%v (cached='%s' current='%s'), age=%v, valid=%v",
			dirMatch, getInfoCacheDir, conf.Dir, age, cacheValid)
	}

	if cacheValid {
		cached := getInfoCacheValue
		getInfoCacheMu.Unlock()
		if conf.Debug {
			logger.Printf("debug", "GetInfo CACHE HIT: returning lib=%d head=%d", cached.Lib, cached.Head)
		}
		return cached, nil
	}
	getInfoCacheMu.Unlock()

	if conf.Debug {
		logger.Printf("debug", "GetInfo CACHE MISS: scanning directory")
	}

	var rv GetInfoResponse

	getInfoCacheMu.Lock()
	lastKnownFile := getInfoCacheLastFile
	getInfoCacheMu.Unlock()

	var lastSlice string

	if lastKnownFile != "" && dirMatch {
		lastSlice = lastKnownFile

		if len(lastKnownFile) >= 37 && lastKnownFile[:12] == "trace_index_" {
			upperStr := lastKnownFile[23:33]

			if upperBlock, err := strconv.ParseUint(upperStr, 10, 32); err == nil {
				nextLower := upperBlock
				nextUpper := nextLower + uint64(conf.Stride)
				nextFile := fmt.Sprintf("trace_index_%010d-%010d.log", nextLower, nextUpper)

				fullPath := fmt.Sprintf("%s/%s", conf.Dir, nextFile)
				_, statErr := os.Stat(fullPath)

				if statErr == nil {
					lastSlice = nextFile
					if conf.Debug {
						logger.Printf("debug", "GetInfo: detected new stride file %s", nextFile)
					}
				}
			}
		}
	}

	if lastSlice == "" {
		if conf.Debug {
			logger.Printf("debug", "GetInfo SLOW PATH: performing full directory scan")
		}

		dir, err := os.Open(conf.Dir)
		if err != nil {
			return rv, err
		}
		defer dir.Close()

		filesScanned := 0

		for {
			entries, err := dir.ReadDir(100) // Read 100 at a time
			if err != nil && err != io.EOF {
				return rv, err
			}
			if len(entries) == 0 {
				break
			}

			for _, entry := range entries {
				name := entry.Name()
				if len(name) > 12 && name[:12] == "trace_index_" {
					filesScanned++
					if lastSlice == "" || name > lastSlice {
						lastSlice = name
					}
				}
			}

			if err == io.EOF {
				break
			}
		}

		if conf.Debug {
			logger.Printf("debug", "GetInfo SLOW PATH: scanned %d files, found: %s", filesScanned, lastSlice)
		}
	}

	if lastSlice == "" {
		return rv, errors.New("no trace files found")
	}

	sliceIndex, err := openSliceIndex(fmt.Sprintf("%s/%s", conf.Dir, lastSlice), conf)
	if err != nil {
		return rv, err
	}
	defer sliceIndex.Close()

	var entry IndexEntryVariant
	dec := chain.NewDecoder(sliceIndex)
	for {
		err = dec.DecodeVariant(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			return rv, err
		}
		if entry.Block != nil && entry.Block.Number > rv.Head {
			rv.Head = entry.Block.Number
		} else if entry.Lib != nil && entry.Lib.Lib > rv.Lib {
			rv.Lib = entry.Lib.Lib
		}
	}

	getInfoCacheMu.Lock()
	getInfoCacheDir = conf.Dir
	getInfoCacheTime = time.Now()
	getInfoCacheValue = rv
	getInfoCacheLastFile = lastSlice
	getInfoCacheMu.Unlock()

	return rv, nil
}

func strideFmt(blockNum uint32, conf *Config) string {
	lb := blockNum - (blockNum % conf.Stride)
	ub := lb + conf.Stride
	return fmt.Sprintf("%s-%s", fmt.Sprintf("%010d", lb), fmt.Sprintf("%010d", ub))
}

func openSliceIndex(filename string, conf *Config) (*os.File, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	var version uint32
	err = binary.Read(f, binary.LittleEndian, &version)
	if err != nil {
		return nil, err
	}

	if version <= 2 {
		if version != HeaderVersion {
			return nil, fmt.Errorf("unsupported trace file version %d", version)
		}
		return f, nil
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	if conf.Debug {
		logger.Printf("debug", "WARNING: trace_index file %s appears headerless (read version=%d), assuming version %d", filename, version, HeaderVersion)
	}

	return f, nil
}
