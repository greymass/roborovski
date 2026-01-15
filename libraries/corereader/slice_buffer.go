package corereader

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/greymass/roborovski/libraries/compression"
	"github.com/greymass/roborovski/libraries/logger"
)

// sliceDataPool provides reusable buffers for reading slice files.
// Buffers start small and grow as needed - once grown, they stay large in the pool.
// This adapts automatically to different chain activity levels.
var sliceDataPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0)
		return &buf
	},
}

type SliceBufferPool struct {
	mu sync.RWMutex

	buffers   map[uint32]*SliceBuffer
	lruList   *list.List
	maxBytes  int64
	usedBytes int64

	slicesLoaded int64
	blocksServed int64
	evictions    int64
	skipped      int64
	enabled      bool
}

type SliceBuffer struct {
	sliceNum  uint32
	data      []byte
	index     map[uint32]blockIndexEntry
	sizeBytes int64
	lruElem   *list.Element
}

func NewSliceBufferPool(maxSizeMB int64) *SliceBufferPool {
	if maxSizeMB <= 0 {
		logger.Printf("buffer", "Slice buffer pool DISABLED (maxSizeMB=%d)", maxSizeMB)
		return &SliceBufferPool{enabled: false}
	}

	maxBytes := maxSizeMB * 1024 * 1024
	logger.Printf("buffer", "Slice buffer pool ENABLED: %d MB max (%d bytes)", maxSizeMB, maxBytes)

	return &SliceBufferPool{
		buffers:  make(map[uint32]*SliceBuffer),
		lruList:  list.New(),
		maxBytes: maxBytes,
		enabled:  true,
	}
}

func (pool *SliceBufferPool) GetSliceBuffer(sliceNum uint32) *SliceBuffer {
	if !pool.enabled {
		return nil
	}

	pool.mu.RLock()
	buffer, found := pool.buffers[sliceNum]
	pool.mu.RUnlock()

	if found {
		pool.mu.Lock()
		pool.blocksServed++
		pool.lruList.MoveToFront(buffer.lruElem)
		pool.mu.Unlock()
		return buffer
	}

	return nil
}

func (pool *SliceBufferPool) Has(sliceNum uint32) bool {
	if !pool.enabled {
		return false
	}

	pool.mu.RLock()
	_, found := pool.buffers[sliceNum]
	pool.mu.RUnlock()

	return found
}

func (pool *SliceBufferPool) LoadSlice(sliceNum uint32, data []byte, index map[uint32]blockIndexEntry, finalized bool) bool {
	if !pool.enabled {
		return false
	}

	if !finalized {
		pool.mu.Lock()
		pool.skipped++
		pool.mu.Unlock()
		return false
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if existing, found := pool.buffers[sliceNum]; found {
		pool.lruList.MoveToFront(existing.lruElem)
		return false
	}

	return pool.loadLocked(sliceNum, data, index)
}

func (pool *SliceBufferPool) loadLocked(sliceNum uint32, data []byte, index map[uint32]blockIndexEntry) bool {
	sizeBytes := int64(len(data))

	for pool.usedBytes+sizeBytes > pool.maxBytes && pool.lruList.Len() > 0 {
		oldest := pool.lruList.Back()
		if oldest == nil {
			break
		}

		oldSlice := oldest.Value.(uint32)
		oldBuffer := pool.buffers[oldSlice]

		ReturnSliceData(oldBuffer.data)

		pool.lruList.Remove(oldest)
		delete(pool.buffers, oldSlice)
		pool.usedBytes -= oldBuffer.sizeBytes
		pool.evictions++
	}

	buffer := &SliceBuffer{
		sliceNum:  sliceNum,
		data:      data,
		index:     index,
		sizeBytes: sizeBytes,
	}
	buffer.lruElem = pool.lruList.PushFront(sliceNum)
	pool.buffers[sliceNum] = buffer
	pool.usedBytes += sizeBytes
	pool.slicesLoaded++

	return true
}

func (buffer *SliceBuffer) GetBlockData(blockNum uint32) ([]byte, error) {
	entry, found := buffer.index[blockNum]
	if !found {
		return nil, fmt.Errorf("block %d not in buffered slice %d", blockNum, buffer.sliceNum)
	}

	offset := entry.Offset + 4
	size := entry.Size

	if offset+uint64(size) > uint64(len(buffer.data)) {
		return nil, fmt.Errorf("block %d offset out of bounds in buffered slice %d", blockNum, buffer.sliceNum)
	}

	rawData := buffer.data[offset : offset+uint64(size)]

	if len(rawData) >= 4 && binary.LittleEndian.Uint32(rawData[0:4]) == 0xFD2FB528 {
		decompressed, err := compression.ZstdDecompress(nil, rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress block %d from buffered slice: %w", blockNum, err)
		}
		return decompressed, nil
	}

	return rawData, nil
}

func (pool *SliceBufferPool) GetStats() (slicesLoaded, blocksServed, evictions, skipped int64, usedMB, maxMB int64, bufferCount int) {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	return pool.slicesLoaded, pool.blocksServed, pool.evictions, pool.skipped,
		pool.usedBytes / 1024 / 1024, pool.maxBytes / 1024 / 1024, len(pool.buffers)
}

func (pool *SliceBufferPool) LogStats() {
	loaded, served, evict, skip, usedMB, maxMB, count := pool.GetStats()

	logger.Printf("buffer", "=== SLICE BUFFER POOL STATS ===")
	logger.Printf("buffer", "Buffers in memory: %d slices (%d MB / %d MB)",
		count, usedMB, maxMB)
	logger.Printf("buffer", "Slices loaded: %d (from disk)", loaded)
	logger.Printf("buffer", "Blocks served: %d (from memory buffers)", served)
	logger.Printf("buffer", "Evictions: %d (LRU) | Skipped: %d (non-finalized)", evict, skip)
}

func loadSliceData(basePath string, sliceInfo SliceInfo) ([]byte, map[uint32]blockIndexEntry, error) {
	sliceDir := filepath.Join(basePath, fmt.Sprintf("history_%010d-%010d",
		sliceInfo.StartBlock, sliceInfo.MaxBlock))

	blockIdxPath := filepath.Join(sliceDir, "blocks.index")
	blockIdx, err := loadBlockIndexSimple(blockIdxPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load block index: %w", err)
	}

	dataLogPath := filepath.Join(sliceDir, "data.log")

	fi, err := os.Stat(dataLogPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat data.log: %w", err)
	}
	fileSize := fi.Size()

	bufPtr := sliceDataPool.Get().(*[]byte)
	buf := *bufPtr

	// Grow buffer if needed (update the pointer's target)
	if int64(cap(buf)) < fileSize {
		buf = make([]byte, fileSize)
		*bufPtr = buf
	}

	f, err := os.Open(dataLogPath)
	if err != nil {
		sliceDataPool.Put(bufPtr)
		return nil, nil, fmt.Errorf("failed to open data.log: %w", err)
	}

	n, err := io.ReadFull(f, buf[:fileSize])
	f.Close()
	if err != nil {
		sliceDataPool.Put(bufPtr)
		return nil, nil, fmt.Errorf("failed to read data.log: %w", err)
	}

	return buf[:n], blockIdx, nil
}

// loadSliceDataPooled is like loadSliceData but returns the pool pointer for proper return.
// The caller MUST call sliceDataPool.Put(poolPtr) when done with the data.
func loadSliceDataPooled(basePath string, sliceInfo SliceInfo) (data []byte, index map[uint32]blockIndexEntry, poolPtr *[]byte, err error) {
	sliceDir := filepath.Join(basePath, fmt.Sprintf("history_%010d-%010d",
		sliceInfo.StartBlock, sliceInfo.MaxBlock))

	blockIdxPath := filepath.Join(sliceDir, "blocks.index")
	blockIdx, err := loadBlockIndexSimple(blockIdxPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to load block index: %w", err)
	}

	dataLogPath := filepath.Join(sliceDir, "data.log")

	fi, err := os.Stat(dataLogPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to stat data.log: %w", err)
	}
	fileSize := fi.Size()

	bufPtr := sliceDataPool.Get().(*[]byte)
	buf := *bufPtr

	if int64(cap(buf)) < fileSize {
		buf = make([]byte, fileSize)
		*bufPtr = buf
	}

	f, err := os.Open(dataLogPath)
	if err != nil {
		sliceDataPool.Put(bufPtr)
		return nil, nil, nil, fmt.Errorf("failed to open data.log: %w", err)
	}

	n, err := io.ReadFull(f, buf[:fileSize])
	f.Close()
	if err != nil {
		sliceDataPool.Put(bufPtr)
		return nil, nil, nil, fmt.Errorf("failed to read data.log: %w", err)
	}

	return buf[:n], blockIdx, bufPtr, nil
}

// ReturnSliceData returns a buffer to the pool.
// DEPRECATED: Use loadSliceDataPooled and return the poolPtr directly.
func ReturnSliceData(data []byte) {
	if data == nil {
		return
	}
	// BUG: Creates new pointer, doesn't actually pool. Kept for API compatibility.
	buf := data[:cap(data)]
	sliceDataPool.Put(&buf)
}
