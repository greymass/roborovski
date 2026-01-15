package compression

import (
	"bytes"
	"testing"
)

func TestZstdRoundTrip(t *testing.T) {
	original := []byte("Hello, this is test data for ZSTD compression!")

	compressed, err := ZstdCompressLevel(nil, original, 3)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	if len(compressed) >= len(original) {
		t.Logf("Warning: compressed size (%d) >= original size (%d)", len(compressed), len(original))
	}

	decompressed, err := ZstdDecompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Fatalf("Round trip failed: got %q, want %q", decompressed, original)
	}
}

func TestZstdCompressLevel(t *testing.T) {
	original := []byte("Hello, this is test data for ZSTD compression with levels!")

	for _, level := range []int{1, 3, 5} {
		compressed, err := ZstdCompressLevel(nil, original, level)
		if err != nil {
			t.Fatalf("CompressLevel(%d) failed: %v", level, err)
		}

		decompressed, err := ZstdDecompress(nil, compressed)
		if err != nil {
			t.Fatalf("Decompress (level %d) failed: %v", level, err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Fatalf("Round trip (level %d) failed", level)
		}
	}
}

func TestZstdDecompressInto(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		dstSize int
		wantErr bool
	}{
		{
			name:    "exact size buffer",
			data:    []byte("Hello, this is test data for decompression into buffer!"),
			dstSize: 56,
			wantErr: false,
		},
		{
			name:    "larger buffer",
			data:    []byte("Short message"),
			dstSize: 100,
			wantErr: false,
		},
		{
			name:    "small data",
			data:    []byte("Hi"),
			dstSize: 10,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := ZstdCompressLevel(nil, tt.data, 3)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}

			dst := make([]byte, tt.dstSize)
			n, err := ZstdDecompressInto(dst, compressed)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZstdDecompressInto() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if n != len(tt.data) {
					t.Errorf("ZstdDecompressInto() returned %d bytes, want %d", n, len(tt.data))
				}
				if !bytes.Equal(dst[:n], tt.data) {
					t.Errorf("ZstdDecompressInto() data mismatch: got %q, want %q", dst[:n], tt.data)
				}
			}
		})
	}
}

func TestZstdDecompressInto_BufferTooSmall(t *testing.T) {
	original := []byte("This is a longer message that will not fit in a tiny buffer")
	compressed, err := ZstdCompressLevel(nil, original, 3)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	dst := make([]byte, 10)
	_, err = ZstdDecompressInto(dst, compressed)

	if err == nil {
		t.Error("ZstdDecompressInto() expected error for undersized buffer, got nil")
	}
	if !ZstdIsDstSizeTooSmallError(err) {
		t.Errorf("ZstdDecompressInto() expected dst size too small error, got: %v", err)
	}
}

func TestZstdIsDstSizeTooSmallError(t *testing.T) {
	original := []byte("Test data that's longer than our tiny buffer will be")
	compressed, err := ZstdCompressLevel(nil, original, 3)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	dst := make([]byte, 5)
	_, err = ZstdDecompressInto(dst, compressed)

	if !ZstdIsDstSizeTooSmallError(err) {
		t.Errorf("Expected ZstdIsDstSizeTooSmallError to return true, got false for error: %v", err)
	}

	if ZstdIsDstSizeTooSmallError(nil) {
		t.Error("ZstdIsDstSizeTooSmallError(nil) should return false")
	}
}

func TestZstdDecompressPartial(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		maxBytes int
		wantLen  int
	}{
		{
			name:     "read first 10 bytes",
			data:     []byte("This is a longer message that we only want to partially decompress"),
			maxBytes: 10,
			wantLen:  10,
		},
		{
			name:     "read first 20 bytes",
			data:     []byte("Another longer message for partial decompression testing purposes"),
			maxBytes: 20,
			wantLen:  20,
		},
		{
			name:     "maxBytes larger than data",
			data:     []byte("Short"),
			maxBytes: 100,
			wantLen:  5,
		},
		{
			name:     "read exactly 1 byte",
			data:     []byte("Hello World"),
			maxBytes: 1,
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := ZstdCompressLevel(nil, tt.data, 3)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}

			partial, err := ZstdDecompressPartial(compressed, tt.maxBytes)
			if err != nil {
				t.Fatalf("ZstdDecompressPartial() error = %v", err)
			}

			if len(partial) != tt.wantLen {
				t.Errorf("ZstdDecompressPartial() returned %d bytes, want %d", len(partial), tt.wantLen)
			}

			expectedPrefix := tt.data[:tt.wantLen]
			if !bytes.Equal(partial, expectedPrefix) {
				t.Errorf("ZstdDecompressPartial() data mismatch: got %q, want %q", partial, expectedPrefix)
			}
		})
	}
}

func TestZstdDecompressPartialInto(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		dstSize int
		wantLen int
	}{
		{
			name:    "partial read into small buffer",
			data:    []byte("This is a message that's longer than our destination buffer"),
			dstSize: 15,
			wantLen: 15,
		},
		{
			name:    "partial read into medium buffer",
			data:    []byte("Another test message for partial decompression into a provided buffer"),
			dstSize: 30,
			wantLen: 30,
		},
		{
			name:    "buffer larger than data",
			data:    []byte("Tiny"),
			dstSize: 50,
			wantLen: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := ZstdCompressLevel(nil, tt.data, 3)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}

			dst := make([]byte, tt.dstSize)
			n, err := ZstdDecompressPartialInto(dst, compressed)
			if err != nil {
				t.Fatalf("ZstdDecompressPartialInto() error = %v", err)
			}

			if n != tt.wantLen {
				t.Errorf("ZstdDecompressPartialInto() returned %d bytes, want %d", n, tt.wantLen)
			}

			expectedPrefix := tt.data[:tt.wantLen]
			if !bytes.Equal(dst[:n], expectedPrefix) {
				t.Errorf("ZstdDecompressPartialInto() data mismatch: got %q, want %q", dst[:n], expectedPrefix)
			}
		})
	}
}

func TestZstdEdgeCases(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		compressed, err := ZstdCompressLevel(nil, []byte{}, 3)
		if err != nil {
			t.Fatalf("Compress empty data failed: %v", err)
		}

		decompressed, err := ZstdDecompress(nil, compressed)
		if err != nil {
			t.Fatalf("Decompress empty data failed: %v", err)
		}

		if len(decompressed) != 0 {
			t.Errorf("Expected empty result, got %d bytes", len(decompressed))
		}
	})

	t.Run("single byte", func(t *testing.T) {
		original := []byte{0x42}
		compressed, err := ZstdCompressLevel(nil, original, 3)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		decompressed, err := ZstdDecompress(nil, compressed)
		if err != nil {
			t.Fatalf("Decompress failed: %v", err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Errorf("Single byte round trip failed")
		}
	})

	t.Run("binary data with zeros", func(t *testing.T) {
		original := make([]byte, 1000)
		for i := range original {
			original[i] = byte(i % 256)
		}

		compressed, err := ZstdCompressLevel(nil, original, 3)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		decompressed, err := ZstdDecompress(nil, compressed)
		if err != nil {
			t.Fatalf("Decompress failed: %v", err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Errorf("Binary data round trip failed")
		}
	})

	t.Run("highly compressible data", func(t *testing.T) {
		original := bytes.Repeat([]byte("AAAA"), 10000)

		compressed, err := ZstdCompressLevel(nil, original, 3)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		if len(compressed) >= len(original)/10 {
			t.Logf("Compression ratio lower than expected: %d -> %d", len(original), len(compressed))
		}

		decompressed, err := ZstdDecompress(nil, compressed)
		if err != nil {
			t.Fatalf("Decompress failed: %v", err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Errorf("Highly compressible data round trip failed")
		}
	})

	t.Run("incompressible random-like data", func(t *testing.T) {
		original := make([]byte, 1000)
		for i := range original {
			original[i] = byte((i*7 + 13) % 256)
		}

		compressed, err := ZstdCompressLevel(nil, original, 3)
		if err != nil {
			t.Fatalf("Compress failed: %v", err)
		}

		decompressed, err := ZstdDecompress(nil, compressed)
		if err != nil {
			t.Fatalf("Decompress failed: %v", err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Errorf("Random-like data round trip failed")
		}
	})
}

func TestZstdDecompress_InvalidData(t *testing.T) {
	_, err := ZstdDecompress(nil, []byte("not valid zstd data"))
	if err == nil {
		t.Error("Expected error decompressing invalid data, got nil")
	}
}

func TestZstdDecompressInto_InvalidData(t *testing.T) {
	dst := make([]byte, 100)
	_, err := ZstdDecompressInto(dst, []byte("not valid zstd data"))
	if err == nil {
		t.Error("Expected error decompressing invalid data, got nil")
	}
}

func TestZstdDecompressPartial_InvalidData(t *testing.T) {
	_, err := ZstdDecompressPartial([]byte("not valid zstd data"), 10)
	if err == nil {
		t.Error("Expected error decompressing invalid data, got nil")
	}
}

func TestZstdDecompressPartialInto_InvalidData(t *testing.T) {
	dst := make([]byte, 100)
	_, err := ZstdDecompressPartialInto(dst, []byte("not valid zstd data"))
	if err == nil {
		t.Error("Expected error decompressing invalid data, got nil")
	}
}

func TestZstdCompressWithDstBuffer(t *testing.T) {
	original := []byte("Test data to compress with pre-allocated destination buffer")

	dst := make([]byte, 0, 200)
	compressed, err := ZstdCompressLevel(dst, original, 3)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := ZstdDecompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Round trip with pre-allocated dst failed")
	}
}

func TestZstdDecompressWithDstBuffer(t *testing.T) {
	original := []byte("Test data for decompression with pre-allocated buffer")

	compressed, err := ZstdCompressLevel(nil, original, 3)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	dst := make([]byte, 0, 100)
	decompressed, err := ZstdDecompress(dst, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompress with pre-allocated dst failed")
	}
}
