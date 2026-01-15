package appendlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

// Writer provides buffered append-only writes to log files
type Writer struct {
	file       *os.File
	buffer     *bufio.Writer
	offset     uint64
	mu         sync.Mutex
	scratchBuf [8]byte // Reusable buffer for size/CRC writes (avoid allocations)
}

// NewWriterWithHeader creates a writer for a new file with a header
// If file already exists and has a valid header, opens for append
// If file is new, writes header and positions for append
func NewWriterWithHeader(path string, header *DataLogHeader) (*Writer, error) {
	return NewWriterWithHeaderAndBufferSize(path, header, 16*1024*1024)
}

// NewWriterWithHeaderAndBufferSize creates a writer with header support and custom buffer
func NewWriterWithHeaderAndBufferSize(path string, header *DataLogHeader, bufferSize int) (*Writer, error) {
	stat, err := os.Stat(path)
	fileExists := err == nil

	if fileExists && stat.Size() > 0 {
		hasHdr, err := HasHeader(path)
		if err != nil {
			return nil, fmt.Errorf("failed to check header: %w", err)
		}
		if !hasHdr {
			return nil, fmt.Errorf("existing file has no header (legacy format not supported)")
		}

		existingHeader, err := ReadDataLogHeader(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read existing header: %w", err)
		}

		if existingHeader.StartBlock != header.StartBlock || existingHeader.EndBlock != header.EndBlock {
			return nil, fmt.Errorf("header mismatch: existing %d-%d vs new %d-%d",
				existingHeader.StartBlock, existingHeader.EndBlock,
				header.StartBlock, header.EndBlock)
		}

		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}

		return &Writer{
			file:   file,
			buffer: bufio.NewWriterSize(file, bufferSize),
			offset: uint64(stat.Size()),
		}, nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	if _, err := file.Write(header.Bytes()); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return &Writer{
		file:   file,
		buffer: bufio.NewWriterSize(file, bufferSize),
		offset: HeaderSize,
	}, nil
}

// Append writes data to the log file and returns the offset
func (w *Writer) Append(data []byte) (offset uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Record offset before write
	offset = w.offset

	// Calculate CRC32
	checksum := crc32.ChecksumIEEE(data)

	// Write size (4 bytes) using scratch buffer
	binary.LittleEndian.PutUint32(w.scratchBuf[:4], uint32(len(data)))
	if _, err := w.buffer.Write(w.scratchBuf[:4]); err != nil {
		return 0, err
	}

	// Write data
	if _, err := w.buffer.Write(data); err != nil {
		return 0, err
	}

	// Write CRC32 (4 bytes) using scratch buffer
	binary.LittleEndian.PutUint32(w.scratchBuf[4:8], checksum)
	if _, err := w.buffer.Write(w.scratchBuf[4:8]); err != nil {
		return 0, err
	}

	// Update offset
	w.offset += uint64(8 + len(data)) // size(4) + data + crc(4)

	return offset, nil
}

// Flush writes buffered data to disk
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Flush()
}

// Sync flushes and fsyncs to disk
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buffer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close flushes and closes the file
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buffer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Offset returns the current write offset
func (w *Writer) Offset() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}
