package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

func inspectLiveMetrics(path string) {
	snappyFn := func() *block.CompressionProfile { return block.SnappyCompression }

	opts := &pebble.Options{
		ReadOnly: true,
		Logger:   pebbleLogger{},
		Merger:   legacyBitmapMerger,
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
