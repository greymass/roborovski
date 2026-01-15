package appendlog

import (
	"encoding/binary"
	"fmt"

	"github.com/greymass/roborovski/libraries/compression"
)

// compressData compresses data using zstd at the specified level
// The resulting data will have zstd frame magic bytes (0x28 0xb5) at the start
func compressData(data []byte, level int) ([]byte, error) {
	compressed, err := compression.ZstdCompressLevel(nil, data, level)
	if err != nil {
		return nil, fmt.Errorf("zstd compression failed: %w", err)
	}
	return compressed, nil
}

// decompressData decompresses zstd-compressed data, auto-detecting compression
func decompressData(data []byte) ([]byte, error) {
	if len(data) >= 2 {
		magic := binary.LittleEndian.Uint16(data[0:2])
		if magic == 0xb528 {
			// Data is compressed, decompress it
			decompressed, err := compression.ZstdDecompress(nil, data)
			if err != nil {
				return nil, fmt.Errorf("zstd decompression failed: %w", err)
			}
			return decompressed, nil
		}
	}

	// Data is not compressed, return as-is (backward compatibility)
	return data, nil
}
