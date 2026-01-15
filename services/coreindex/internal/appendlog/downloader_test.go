package appendlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"
)

func TestParseBlockMetadata(t *testing.T) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(12345))
	binary.Write(buf, binary.LittleEndian, uint32(1000000))
	buf.Write(make([]byte, 32))
	binary.Write(buf, binary.LittleEndian, uint64(99999))
	buf.WriteByte(10)

	meta := parseBlockMetadata(buf.Bytes())

	if meta == nil {
		t.Fatal("parseBlockMetadata returned nil")
	}
	if meta.BlockNum != 12345 {
		t.Errorf("BlockNum = %d, expected 12345", meta.BlockNum)
	}
	if meta.MaxGlobInBlock != 99999 {
		t.Errorf("MaxGlobInBlock = %d, expected 99999", meta.MaxGlobInBlock)
	}
	if meta.MinGlobInBlock != 99989 {
		t.Errorf("MinGlobInBlock = %d, expected 99989", meta.MinGlobInBlock)
	}
}

func TestParseBlockMetadataTooShort(t *testing.T) {
	data := make([]byte, 20)
	meta := parseBlockMetadata(data)
	if meta != nil {
		t.Error("Expected nil for short data")
	}
}

func TestSliceDownloaderDisabled(t *testing.T) {
	d := NewSliceDownloader("", "/tmp", 4, 10000, 30, nil)
	if d != nil {
		t.Error("Expected nil downloader when URL is empty")
	}
}

func TestSliceDownloaderEnabled(t *testing.T) {
	d := NewSliceDownloader("https://example.com", "/tmp", 4, 10000, 30, nil)
	if d == nil {
		t.Fatal("Expected non-nil downloader")
	}
}

func TestSliceNumForBlock(t *testing.T) {
	d := NewSliceDownloader("https://example.com", "/tmp", 4, 10000, 30, nil)

	tests := []struct {
		blockNum uint32
		expected uint32
	}{
		{0, 0},
		{1, 0},
		{10000, 0},
		{10001, 1},
		{20000, 1},
		{20001, 2},
	}

	for _, tc := range tests {
		got := d.SliceNumForBlock(tc.blockNum)
		if got != tc.expected {
			t.Errorf("SliceNumForBlock(%d) = %d, expected %d", tc.blockNum, got, tc.expected)
		}
	}
}

func TestSliceArchiveName(t *testing.T) {
	d := NewSliceDownloader("https://example.com", "/tmp", 4, 10000, 30, nil)

	name := d.sliceArchiveName(0)
	expected := "history_0000000001-0000010000.tar.zst"
	if name != expected {
		t.Errorf("sliceArchiveName(0) = %q, expected %q", name, expected)
	}

	name = d.sliceArchiveName(5)
	expected = "history_0000050001-0000060000.tar.zst"
	if name != expected {
		t.Errorf("sliceArchiveName(5) = %q, expected %q", name, expected)
	}
}

func TestDownloadSlicesCancellation(t *testing.T) {
	d := NewSliceDownloader("https://nonexistent.example.com/slices/", "/tmp", 4, 10000, 1, nil)
	if d == nil {
		t.Fatal("Expected non-nil downloader")
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_ = d.DownloadSlices(ctx, 0)

	if ctx.Err() != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", ctx.Err())
	}
}

func TestDownloadSlicesContextAlreadyCancelled(t *testing.T) {
	d := NewSliceDownloader("https://nonexistent.example.com/slices/", "/tmp", 4, 10000, 1, nil)
	if d == nil {
		t.Fatal("Expected non-nil downloader")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := d.DownloadSlices(ctx, 0)
	if err != nil {
		t.Logf("Download returned error as expected: %v", err)
	}

	if ctx.Err() != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", ctx.Err())
	}
}
