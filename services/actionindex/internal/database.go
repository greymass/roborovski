package internal

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/greymass/roborovski/libraries/logger"
)

var legacyBitmapMerger = &pebble.Merger{
	Name: "roaring64_bitmap_or",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		bm := roaring64.New()
		if len(value) > 0 {
			if _, err := bm.ReadFrom(bytes.NewReader(value)); err != nil {
				return nil, err
			}
		}
		return &bitmapValueMerger{bitmap: bm}, nil
	},
}

type bitmapValueMerger struct {
	bitmap *roaring64.Bitmap
}

func (m *bitmapValueMerger) MergeNewer(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	other := roaring64.New()
	if _, err := other.ReadFrom(bytes.NewReader(value)); err != nil {
		return err
	}
	m.bitmap.Or(other)
	return nil
}

func (m *bitmapValueMerger) MergeOlder(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	other := roaring64.New()
	if _, err := other.ReadFrom(bytes.NewReader(value)); err != nil {
		return err
	}
	m.bitmap.Or(other)
	return nil
}

func (m *bitmapValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	var buf bytes.Buffer
	if _, err := m.bitmap.WriteTo(&buf); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), nil, nil
}

type pebbleLogger struct{}

func (pebbleLogger) Infof(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)

	if strings.Contains(msg, "sstable created") ||
		strings.Contains(msg, "sstable deleted") ||
		strings.Contains(msg, "WAL deleted") ||
		strings.Contains(msg, "WAL created") ||
		strings.Contains(msg, "MANIFEST deleted") ||
		strings.Contains(msg, "MANIFEST created") ||
		strings.Contains(msg, "all initial table stats loaded") ||
		strings.Contains(msg, "compacting(") ||
		strings.Contains(msg, "compacting:") ||
		strings.Contains(msg, "flushing:") ||
		strings.Contains(msg, "OverlappingRatio") {
		return
	}

	if strings.Contains(msg, "stopped reading at offset") {
		if idx := strings.Index(msg, "replayed"); idx != -1 {
			logger.Printf("pebble", "WAL recovery: %s", msg[idx:])
			return
		}
	}

	logger.Printf("debug-pebble", "%s", msg)
}

func (pebbleLogger) Fatalf(format string, args ...interface{}) {
	logger.Fatal(format, args...)
}

func (pebbleLogger) Errorf(format string, args ...interface{}) {
	logger.Printf("pebble", "ERROR: "+format, args...)
}

func makeFriendlyEventListener() pebble.EventListener {
	return pebble.EventListener{
		CompactionEnd: func(info pebble.CompactionInfo) {
			var inputSize, outputSize uint64
			var inputTables int
			for _, level := range info.Input {
				for _, t := range level.Tables {
					inputSize += t.Size
					inputTables++
				}
			}
			for _, t := range info.Output.Tables {
				outputSize += t.Size
			}
			inputLevel := 0
			if len(info.Input) > 0 {
				inputLevel = info.Input[0].Level
			}
			rate := float64(outputSize) / (1024 * 1024) / info.TotalDuration.Seconds()
			logger.Printf("debug-pebble", "Compaction L%d→L%d: %d→%d files (%s→%s) in %.1fs @ %.1f MB/s",
				inputLevel, info.Output.Level,
				inputTables, len(info.Output.Tables),
				logger.FormatBytes(int64(inputSize)), logger.FormatBytes(int64(outputSize)),
				info.TotalDuration.Seconds(), rate)
		},
		FlushBegin: func(info pebble.FlushInfo) {},
		FlushEnd: func(info pebble.FlushInfo) {
			var outputSize uint64
			for _, t := range info.Output {
				outputSize += t.Size
			}
			rate := float64(outputSize) / (1024 * 1024) / info.Duration.Seconds()
			logger.Printf("debug-pebble", "Flush: %d memtables → %d files (%s) in %.1fs @ %.1f MB/s",
				info.Input, len(info.Output), logger.FormatBytes(int64(outputSize)),
				info.Duration.Seconds(), rate)
		},
		WALCreated: func(info pebble.WALCreateInfo) {
			logger.Printf("debug-pebble", "WAL created: %d", info.FileNum)
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			logger.Printf("pebble", "WARNING: Write stall: %s", info.Reason)
		},
		WriteStallEnd: func() {
			logger.Printf("pebble", "Write stall ended")
		},
		BackgroundError: func(err error) {
			logger.Printf("pebble", "ERROR: Background error: %v", err)
		},
	}
}

type Store struct {
	db *pebble.DB
}

// StoreConfig holds tunable parameters for the Pebble store
type StoreConfig struct {
	MemTableSizeMB        int   // Size of each MemTable in MB
	MemTableStopThreshold int   // Number of memtables before writes stall
	Compactors            int   // Number of concurrent compaction threads
	CacheSizeMB           int64 // Block cache size in MB
}

func NewStore(path string, readOnly bool, cfg StoreConfig) (*Store, error) {
	logger.Printf("startup", "Opening Pebble database: %s", path)
	plog := pebbleLogger{}

	memTableSize := cfg.MemTableSizeMB << 20
	if memTableSize < 64<<20 {
		memTableSize = 64 << 20 // Minimum 64 MB
	}

	memTableStopThreshold := cfg.MemTableStopThreshold
	if memTableStopThreshold < 4 {
		memTableStopThreshold = 4 // Minimum 4
	}

	compactors := cfg.Compactors
	if compactors < 1 {
		compactors = 4 // Default 4
	}

	eventListener := makeFriendlyEventListener()

	cacheSize := cfg.CacheSizeMB << 20
	if cacheSize < 64<<20 {
		cacheSize = 64 << 20 // Minimum 64 MB
	}
	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()

	snappyFn := func() *block.CompressionProfile { return block.SnappyCompression }

	opts := &pebble.Options{
		Logger:                      plog,
		EventListener:               &eventListener,
		Merger:                      legacyBitmapMerger,
		Cache:                       cache,
		MemTableSize:                uint64(memTableSize),
		MemTableStopWritesThreshold: memTableStopThreshold,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       12,
		LBaseMaxBytes:               64 << 20,
	}

	opts.Experimental.L0CompactionConcurrency = compactors
	opts.Experimental.CompactionDebtConcurrency = 1 << 30

	opts.Levels[0] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}
	opts.Levels[1] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}
	opts.Levels[2] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}
	opts.Levels[3] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}
	opts.Levels[4] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}
	opts.Levels[5] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}
	opts.Levels[6] = pebble.LevelOptions{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn}

	if readOnly {
		opts.ReadOnly = true
	}

	openStart := time.Now()
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	openDuration := time.Since(openStart)

	logger.Printf("startup", "Pebble database opened in %v", openDuration)
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *pebble.DB {
	return s.db
}

func (s *Store) NewBatch() *pebble.Batch {
	return s.db.NewBatch()
}

func (s *Store) Get(key []byte) ([]byte, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (s *Store) NewIterator(prefix []byte) (*pebble.Iterator, error) {
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	for i := len(upperBound) - 1; i >= 0; i-- {
		upperBound[i]++
		if upperBound[i] != 0 {
			break
		}
	}

	opts := &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	}
	return s.db.NewIter(opts)
}

func (s *Store) Commit(batch *pebble.Batch) error {
	return batch.Commit(pebble.Sync)
}

func (s *Store) CommitNoSync(batch *pebble.Batch) error {
	return batch.Commit(pebble.NoSync)
}

func (s *Store) Size() uint64 {
	metrics := s.db.Metrics()
	return metrics.DiskSpaceUsage()
}

// PebbleMetrics holds key metrics for monitoring Pebble performance
type PebbleMetrics struct {
	MemTableCount   int64  // Number of active memtables
	MemTableSize    uint64 // Total size of memtables
	FlushCount      int64  // Total flushes performed
	CompactionCount int64  // Total compactions performed
	CompactingBytes int64  // Bytes currently being compacted
	L0FileCount     int64  // Number of L0 files (affects read performance)
	DiskSpaceUsage  uint64 // Total disk space used
}

func (s *Store) GetMetrics() PebbleMetrics {
	m := s.db.Metrics()
	return PebbleMetrics{
		MemTableCount:   m.MemTable.Count,
		MemTableSize:    m.MemTable.Size,
		FlushCount:      m.Flush.Count,
		CompactionCount: m.Compact.Count,
		CompactingBytes: m.Compact.InProgressBytes,
		L0FileCount:     m.Levels[0].TablesCount,
		DiskSpaceUsage:  m.DiskSpaceUsage(),
	}
}

func (s *Store) LogMetrics() {
	m := s.GetMetrics()
	logger.Printf("debug-pebble", "MemTables=%d (%s) | L0Files=%d | Compacting=%s | Disk=%s",
		m.MemTableCount, logger.FormatBytes(int64(m.MemTableSize)),
		m.L0FileCount,
		logger.FormatBytes(m.CompactingBytes),
		logger.FormatBytes(int64(m.DiskSpaceUsage)))
}

