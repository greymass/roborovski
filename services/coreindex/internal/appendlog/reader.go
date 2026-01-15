package appendlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

var (
	ErrCorrupted = errors.New("data corruption detected")
	ErrNotFound  = errors.New("entry not found")
)

// Reader provides random-access reads from log files
type Reader struct {
	file   *os.File
	size   int64
	header *DataLogHeader
}

// NewReader opens a log file for reading (legacy, no header validation)
func NewReader(path string) (*Reader, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Reader{
		file: file,
		size: stat.Size(),
	}, nil
}

// NewReaderWithHeader opens a log file and reads/validates header
func NewReaderWithHeader(path string) (*Reader, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if stat.Size() < HeaderSize {
		file.Close()
		return nil, ErrNoHeader
	}

	buf := make([]byte, HeaderSize)
	if _, err := file.ReadAt(buf, 0); err != nil {
		file.Close()
		return nil, err
	}

	header, err := ParseDataLogHeader(buf)
	if err != nil {
		file.Close()
		return nil, err
	}

	if err := header.Validate(); err != nil {
		file.Close()
		return nil, err
	}

	return &Reader{
		file:   file,
		size:   stat.Size(),
		header: header,
	}, nil
}

// Header returns the parsed header (nil if opened without header validation)
func (r *Reader) Header() *DataLogHeader {
	return r.header
}

// ReadAt reads entry at given offset
func (r *Reader) ReadAt(offset uint64) ([]byte, error) {
	// Read size (4 bytes)
	sizeBytes := make([]byte, 4)
	if _, err := r.file.ReadAt(sizeBytes, int64(offset)); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(sizeBytes)

	// Validate size is reasonable
	if size == 0 || size > 100*1024*1024 { // Max 100MB per entry
		return nil, ErrCorrupted
	}

	// Read data + CRC (size + 4 bytes)
	dataAndCRC := make([]byte, size+4)
	if _, err := r.file.ReadAt(dataAndCRC, int64(offset)+4); err != nil {
		return nil, err
	}

	data := dataAndCRC[:size]
	storedCRC := binary.LittleEndian.Uint32(dataAndCRC[size:])

	// Verify CRC
	calculatedCRC := crc32.ChecksumIEEE(data)
	if calculatedCRC != storedCRC {
		return nil, ErrCorrupted
	}

	return data, nil
}

// Scan iterates through all entries in the log file
func (r *Reader) Scan(callback func(offset uint64, data []byte) error) error {
	offset := uint64(0)

	for offset < uint64(r.size) {
		// Read size
		sizeBytes := make([]byte, 4)
		n, err := r.file.ReadAt(sizeBytes, int64(offset))
		if err == io.EOF {
			break
		}
		if err != nil || n != 4 {
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBytes)

		if size == 0 || size > 100*1024*1024 {
			return ErrCorrupted
		}

		// Read data + CRC
		dataAndCRC := make([]byte, size+4)
		if _, err := r.file.ReadAt(dataAndCRC, int64(offset)+4); err != nil {
			return err
		}

		data := dataAndCRC[:size]
		storedCRC := binary.LittleEndian.Uint32(dataAndCRC[size:])

		// Verify CRC
		if crc32.ChecksumIEEE(data) != storedCRC {
			return ErrCorrupted
		}

		// Callback with data
		if err := callback(offset, data); err != nil {
			return err
		}

		// Move to next entry
		offset += uint64(8 + size) // size(4) + data + crc(4)
	}

	return nil
}

// Close closes the file
func (r *Reader) Close() error {
	return r.file.Close()
}

// Size returns the file size
func (r *Reader) Size() int64 {
	return r.size
}

// ScanFromOffset iterates through entries starting from a given offset
func (r *Reader) ScanFromOffset(startOffset uint64, callback func(offset uint64, data []byte) error) error {
	offset := startOffset

	for offset < uint64(r.size) {
		sizeBytes := make([]byte, 4)
		n, err := r.file.ReadAt(sizeBytes, int64(offset))
		if err == io.EOF {
			break
		}
		if err != nil || n != 4 {
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBytes)

		if size == 0 || size > 100*1024*1024 {
			return ErrCorrupted
		}

		dataAndCRC := make([]byte, size+4)
		if _, err := r.file.ReadAt(dataAndCRC, int64(offset)+4); err != nil {
			return err
		}

		data := dataAndCRC[:size]
		storedCRC := binary.LittleEndian.Uint32(dataAndCRC[size:])

		if crc32.ChecksumIEEE(data) != storedCRC {
			return ErrCorrupted
		}

		if err := callback(offset, data); err != nil {
			return err
		}

		offset += uint64(8 + size)
	}

	return nil
}

// ScanWithHeader scans entries after the header (convenience method)
func (r *Reader) ScanWithHeader(callback func(offset uint64, data []byte) error) error {
	if r.header == nil {
		return ErrNoHeader
	}
	return r.ScanFromOffset(HeaderSize, callback)
}
