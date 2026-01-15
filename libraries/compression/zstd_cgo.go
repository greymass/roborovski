//go:build cgo
// +build cgo

package compression

import (
	"bytes"
	"io"

	"github.com/DataDog/zstd"
)

func ZstdCompressLevel(dst, src []byte, level int) ([]byte, error) {
	return zstd.CompressLevel(dst, src, level)
}

func ZstdDecompress(dst, src []byte) ([]byte, error) {
	return zstd.Decompress(dst, src)
}

func ZstdDecompressInto(dst, src []byte) (int, error) {
	return zstd.DecompressInto(dst, src)
}

func ZstdIsDstSizeTooSmallError(err error) bool {
	return zstd.IsDstSizeTooSmallError(err)
}

// ZstdDecompressPartial decompresses only the first maxBytes of a compressed block.
// This is useful when you only need the header/prefix of a large compressed block.
// Returns the decompressed data (up to maxBytes) and any error.
func ZstdDecompressPartial(src []byte, maxBytes int) ([]byte, error) {
	reader := zstd.NewReader(bytes.NewReader(src))
	defer reader.Close()

	dst := make([]byte, maxBytes)
	n, err := io.ReadFull(reader, dst)
	if err == io.ErrUnexpectedEOF {
		return dst[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}

// ZstdDecompressPartialInto decompresses into the provided buffer.
// Returns the number of bytes written and any error.
func ZstdDecompressPartialInto(dst, src []byte) (int, error) {
	reader := zstd.NewReader(bytes.NewReader(src))
	defer reader.Close()

	n, err := io.ReadFull(reader, dst)
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}
