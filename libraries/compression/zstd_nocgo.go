//go:build !cgo
// +build !cgo

package compression

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var errDstSizeTooSmall = errors.New("destination buffer too small")

// decoderPool provides thread-safe access to zstd decoders
var decoderPool = sync.Pool{
	New: func() interface{} {
		d, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		return d
	},
}

func getDecoder() *zstd.Decoder {
	return decoderPool.Get().(*zstd.Decoder)
}

func putDecoder(d *zstd.Decoder) {
	decoderPool.Put(d)
}

var (
	encoderMu    sync.RWMutex
	encoderCache = map[int]*zstd.Encoder{}
)

func getEncoder(level int) (*zstd.Encoder, error) {
	encoderMu.RLock()
	enc, ok := encoderCache[level]
	encoderMu.RUnlock()
	if ok {
		return enc, nil
	}

	encoderMu.Lock()
	defer encoderMu.Unlock()
	if enc, ok = encoderCache[level]; ok {
		return enc, nil
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, err
	}
	encoderCache[level] = enc
	return enc, nil
}

func ZstdCompressLevel(dst, src []byte, level int) ([]byte, error) {
	enc, err := getEncoder(level)
	if err != nil {
		return nil, err
	}
	return enc.EncodeAll(src, dst[:0]), nil
}

func ZstdDecompress(dst, src []byte) ([]byte, error) {
	dec := getDecoder()
	defer putDecoder(dec)
	return dec.DecodeAll(src, dst[:0])
}

func ZstdDecompressInto(dst, src []byte) (int, error) {
	dec := getDecoder()
	defer putDecoder(dec)
	result, err := dec.DecodeAll(src, dst[:0])
	if err != nil {
		return 0, err
	}
	if len(result) > len(dst) {
		return 0, errDstSizeTooSmall
	}
	return len(result), nil
}

func ZstdIsDstSizeTooSmallError(err error) bool {
	return errors.Is(err, errDstSizeTooSmall)
}

// ZstdDecompressPartial decompresses only the first maxBytes of a compressed block.
// This is useful when you only need the header/prefix of a large compressed block.
// Returns the decompressed data (up to maxBytes) and any error.
func ZstdDecompressPartial(src []byte, maxBytes int) ([]byte, error) {
	reader, err := zstd.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
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
	reader, err := zstd.NewReader(bytes.NewReader(src))
	if err != nil {
		return 0, err
	}
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
