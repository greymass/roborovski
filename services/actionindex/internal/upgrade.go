package internal

import (
	"bytes"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/roaring64"
	pebblev1 "github.com/cockroachdb/pebble"
	bloomv1 "github.com/cockroachdb/pebble/bloom"
	pebblev2 "github.com/cockroachdb/pebble/v2"
	bloomv2 "github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/greymass/roborovski/libraries/logger"
)

var v1BitmapMerger = &pebblev1.Merger{
	Name: "roaring64_bitmap_or",
	Merge: func(key, value []byte) (pebblev1.ValueMerger, error) {
		bm := roaring64.New()
		if len(value) > 0 {
			if _, err := bm.ReadFrom(bytes.NewReader(value)); err != nil {
				return nil, err
			}
		}
		return &v1BitmapValueMerger{bitmap: bm}, nil
	},
}

type v1BitmapValueMerger struct {
	bitmap *roaring64.Bitmap
}

func (m *v1BitmapValueMerger) MergeNewer(value []byte) error {
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

func (m *v1BitmapValueMerger) MergeOlder(value []byte) error {
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

func (m *v1BitmapValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	var buf bytes.Buffer
	if _, err := m.bitmap.WriteTo(&buf); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), nil, nil
}

type v1PebbleLogger struct{}

func (v1PebbleLogger) Infof(format string, args ...interface{}) {
	logger.Printf("debug-pebble", format, args...)
}

func (v1PebbleLogger) Fatalf(format string, args ...interface{}) {
	logger.Fatal(format, args...)
}

func UpgradeDatabase(path string) error {
	desc, err := pebblev2.Peek(path, vfs.Default)
	if err != nil {
		return fmt.Errorf("peek database: %w", err)
	}

	if !desc.Exists {
		logger.Printf("startup", "No database found at %s, nothing to upgrade", path)
		return nil
	}

	formatVersion := desc.FormatMajorVersion
	if formatVersion == pebblev2.FormatDefault {
		formatVersion = 1
	}

	logger.Printf("startup", "Current format version: %d", formatVersion)
	logger.Printf("startup", "Target format version: %d (pebble v2 newest)", pebblev2.FormatNewest)

	if formatVersion < pebblev2.FormatMinSupported {
		logger.Printf("startup", "Phase 1: upgrading with pebble v1 (format %d → %d)", formatVersion, pebblev1.FormatNewest)
		if err := upgradeWithV1(path); err != nil {
			return fmt.Errorf("v1 upgrade: %w", err)
		}
	} else {
		logger.Printf("startup", "Phase 1: skipped (format %d >= v2 minimum %d)", formatVersion, pebblev2.FormatMinSupported)
	}

	logger.Printf("startup", "Phase 2: upgrading with pebble v2 (→ format %d)", pebblev2.FormatNewest)
	if err := upgradeWithV2(path); err != nil {
		return fmt.Errorf("v2 upgrade: %w", err)
	}

	return nil
}

func upgradeWithV1(path string) error {
	opts := &pebblev1.Options{
		Logger: v1PebbleLogger{},
		Merger: v1BitmapMerger,
		Levels: []pebblev1.LevelOptions{
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
			{FilterPolicy: bloomv1.FilterPolicy(10), Compression: pebblev1.SnappyCompression},
		},
	}

	db, err := pebblev1.Open(path, opts)
	if err != nil {
		return fmt.Errorf("open with v1: %w", err)
	}

	target := pebblev1.FormatNewest
	logger.Printf("startup", "  Ratcheting to format %d (v1 newest)", target)
	if err := db.RatchetFormatMajorVersion(target); err != nil {
		db.Close()
		return fmt.Errorf("ratchet to v1 format %d: %w", target, err)
	}

	logger.Printf("startup", "  Phase 1 complete, closing database")
	return db.Close()
}

func upgradeWithV2(path string) error {
	snappyFn := func() *block.CompressionProfile { return block.SnappyCompression }

	opts := &pebblev2.Options{
		Logger: pebbleLogger{},
		Merger: legacyBitmapMerger,
		Levels: [7]pebblev2.LevelOptions{
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
			{FilterPolicy: bloomv2.FilterPolicy(10), Compression: snappyFn},
		},
	}

	db, err := pebblev2.Open(path, opts)
	if err != nil {
		return fmt.Errorf("open with v2: %w", err)
	}

	target := pebblev2.FormatNewest
	logger.Printf("startup", "  Ratcheting to format %d (v2 newest)", target)
	if err := db.RatchetFormatMajorVersion(target); err != nil {
		db.Close()
		return fmt.Errorf("ratchet to v2 format %d: %w", target, err)
	}

	logger.Printf("startup", "  Phase 2 complete, closing database")
	return db.Close()
}
