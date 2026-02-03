package internal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/greymass/roborovski/libraries/logger"
)

func InspectDatabase(path string) {
	logger.Printf("startup", "Inspecting Pebble database: %s", path)
	logger.Println("startup", "")

	desc, err := pebble.Peek(path, vfs.Default)
	if err != nil {
		logger.Printf("startup", "Peek failed: %v", err)
		return
	}

	if !desc.Exists {
		logger.Printf("startup", "No database found at %s", path)
		return
	}

	formatVersion := desc.FormatMajorVersion
	if formatVersion == pebble.FormatDefault {
		formatVersion = 1
	}

	logger.Printf("startup", "Format version: %d (%s)", formatVersion, formatVersion)
	logger.Printf("startup", "Manifest: %s", filepath.Base(desc.ManifestFilename))
	if desc.OptionsFilename != "" {
		logger.Printf("startup", "Options file: %s", filepath.Base(desc.OptionsFilename))
	}
	logger.Println("startup", "")

	logger.Printf("startup", "Compatibility:")
	logger.Printf("startup", "  Min supported by this binary: %d (%s)", pebble.FormatMinSupported, pebble.FormatMinSupported)
	logger.Printf("startup", "  Newest supported by this binary: %d (%s)", pebble.FormatNewest, pebble.FormatNewest)

	canOpen := formatVersion >= pebble.FormatMinSupported && formatVersion <= pebble.FormatNewest
	if canOpen {
		logger.Printf("startup", "  Status: compatible")
	} else {
		logger.Printf("startup", "  Status: INCOMPATIBLE — format version %d < minimum supported %d", formatVersion, pebble.FormatMinSupported)
		logger.Printf("startup", "  This database was created with an older version of Pebble (v1).")
		logger.Printf("startup", "  Migration: open with a pebble v1 binary and upgrade, or replay from scratch.")
	}
	logger.Println("startup", "")

	inspectFiles(path)

	if desc.OptionsFilename != "" {
		inspectOptionsFile(desc.OptionsFilename)
	}

	if canOpen {
		inspectLiveMetrics(path)
	}
}

func inspectFiles(path string) {
	entries, err := os.ReadDir(path)
	if err != nil {
		logger.Printf("startup", "Could not list directory: %v", err)
		return
	}

	var sstCount, walCount, manifestCount, optionsCount, otherCount int
	var totalSize int64

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		totalSize += info.Size()
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".sst"):
			sstCount++
		case strings.HasSuffix(name, ".log"):
			walCount++
		case strings.HasPrefix(name, "MANIFEST"):
			manifestCount++
		case strings.HasPrefix(name, "OPTIONS"):
			optionsCount++
		default:
			otherCount++
		}
	}

	logger.Printf("startup", "Files:")
	logger.Printf("startup", "  SST files: %d", sstCount)
	logger.Printf("startup", "  WAL files: %d", walCount)
	logger.Printf("startup", "  MANIFEST files: %d", manifestCount)
	logger.Printf("startup", "  OPTIONS files: %d", optionsCount)
	if otherCount > 0 {
		logger.Printf("startup", "  Other files: %d", otherCount)
	}
	logger.Printf("startup", "  Total size: %s", logger.FormatBytes(totalSize))
	logger.Println("startup", "")
}

func inspectOptionsFile(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		logger.Printf("startup", "Could not read OPTIONS file: %v", err)
		return
	}

	logger.Printf("startup", "OPTIONS (%s):", filepath.Base(filename))
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		logger.Printf("startup", "  %s", line)
	}
	logger.Println("startup", "")
}

var knownPrefixes = map[byte]string{
	PrefixAccountActions:       "AccountActions",
	PrefixContractAction:       "ContractAction",
	PrefixContractWildcard:     "ContractWildcard",
	PrefixProperties:           "Properties",
	PrefixWAL:                  "WAL",
	PrefixTimeMap:              "TimeMap",
}

func inspectPrefixes(db *pebble.DB) {
	logger.Printf("startup", "Key prefix scan:")

	for pfx := 0; pfx < 256; pfx++ {
		lower := []byte{byte(pfx)}
		var upper []byte
		if pfx < 255 {
			upper = []byte{byte(pfx + 1)}
		}

		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		})
		if err != nil {
			logger.Printf("startup", "  0x%02X: error creating iterator: %v", pfx, err)
			continue
		}

		var keyCount uint64
		var valueBytes uint64
		for iter.First(); iter.Valid(); iter.Next() {
			keyCount++
			valueBytes += uint64(len(iter.Value()))
		}
		if err := iter.Error(); err != nil {
			logger.Printf("startup", "  0x%02X: iterator error: %v", pfx, err)
		}
		iter.Close()

		if keyCount == 0 {
			continue
		}

		name := knownPrefixes[byte(pfx)]
		if name != "" {
			logger.Printf("startup", "  0x%02X %-22s %d keys, %s values",
				pfx, name, keyCount, logger.FormatBytes(int64(valueBytes)))
		} else {
			logger.Printf("startup", "  0x%02X %-22s %d keys, %s values",
				pfx, "(unknown)", keyCount, logger.FormatBytes(int64(valueBytes)))
		}
	}
	logger.Println("startup", "")
}

func inspectSyncState(db *pebble.DB) {
	logger.Printf("startup", "Sync state:")

	propsKey := makePropertiesKey()
	if val, closer, err := db.Get(propsKey); err == nil {
		if libNum, headNum, ok := parsePropertiesValue(val); ok {
			logger.Printf("startup", "  LIB block: %d", libNum)
			logger.Printf("startup", "  Head block: %d", headNum)
		}
		closer.Close()
	} else {
		logger.Printf("startup", "  No properties key found")
	}

	tmLower := []byte{PrefixTimeMap}
	tmUpper := []byte{PrefixTimeMap + 1}
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: tmLower,
		UpperBound: tmUpper,
	})
	if err != nil {
		logger.Printf("startup", "  TimeMap iterator error: %v", err)
		logger.Println("startup", "")
		return
	}
	defer iter.Close()

	var count int
	var firstHour, lastHour uint32
	var firstMinSeq, lastMaxSeq uint64

	for iter.First(); iter.Valid(); iter.Next() {
		hour, ok := parseTimeMapKey(iter.Key())
		if !ok {
			continue
		}
		val := iter.Value()
		if len(val) != 16 {
			continue
		}
		minSeq := binary.BigEndian.Uint64(val[0:8])
		maxSeq := binary.BigEndian.Uint64(val[8:16])

		if count == 0 {
			firstHour = hour
			firstMinSeq = minSeq
		}
		lastHour = hour
		lastMaxSeq = maxSeq
		count++
	}

	if count > 0 {
		firstTime := time.Unix(int64(firstHour)*3600, 0).UTC()
		lastTime := time.Unix(int64(lastHour)*3600, 0).UTC()
		duration := lastTime.Sub(firstTime)

		logger.Printf("startup", "  TimeMap entries: %d", count)
		logger.Printf("startup", "  First hour: %s (seq %d)", firstTime.Format("2006-01-02 15:04 UTC"), firstMinSeq)
		logger.Printf("startup", "  Last hour:  %s (seq %d)", lastTime.Format("2006-01-02 15:04 UTC"), lastMaxSeq)
		logger.Printf("startup", "  Duration: %s (%.1f days)", duration, duration.Hours()/24)
		logger.Printf("startup", "  Sequence range: %d → %d (%d total)", firstMinSeq, lastMaxSeq, lastMaxSeq-firstMinSeq)
	} else {
		logger.Printf("startup", "  No TimeMap entries found")
	}
	logger.Println("startup", "")
}

func inspectLiveMetrics(path string) {
	snappyFn := func() *block.CompressionProfile { return block.SnappyCompression }

	opts := &pebble.Options{
		ReadOnly: true,
		Logger:   pebbleLogger{},
		Levels: [7]pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyFn},
		},
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		logger.Printf("startup", "Could not open database for metrics: %v", err)
		return
	}
	defer db.Close()

	inspectPrefixes(db)
	inspectSyncState(db)

	m := db.Metrics()
	logger.Printf("startup", "Pebble metrics:")
	logger.Printf("startup", "  Disk usage: %s", logger.FormatBytes(int64(m.DiskSpaceUsage())))
	logger.Printf("startup", "  MemTable count: %d (%s)", m.MemTable.Count, logger.FormatBytes(int64(m.MemTable.Size)))
	logger.Printf("startup", "  Flush count: %d", m.Flush.Count)
	logger.Printf("startup", "  Compaction count: %d", m.Compact.Count)
	logger.Println("startup", "")
	logger.Printf("startup", "  Level details:")

	for i, l := range m.Levels {
		if l.TablesCount == 0 && l.TablesSize == 0 {
			continue
		}
		logger.Printf("startup", "    L%d: %d files (%s)", i, l.TablesCount, logger.FormatBytes(l.TablesSize))
	}

	fmt.Println()
	fmt.Println(m.String())
}
