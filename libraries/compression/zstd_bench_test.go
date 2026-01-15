package compression

import (
	"bytes"
	"testing"
)

var (
	benchCompressed   []byte
	benchDecompressed []byte
	benchN            int
)

func BenchmarkZstdCompressLevel(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("ABCDEFGH12345678"), size.size/16)

		b.Run(size.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				benchCompressed, _ = ZstdCompressLevel(nil, data, 3)
			}
		})
	}
}

func BenchmarkZstdDecompress(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("ABCDEFGH12345678"), size.size/16)
		compressed, _ := ZstdCompressLevel(nil, data, 3)

		b.Run(size.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				benchDecompressed, _ = ZstdDecompress(nil, compressed)
			}
		})
	}
}

func BenchmarkZstdDecompressInto(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
	}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("ABCDEFGH12345678"), size.size/16)
		compressed, _ := ZstdCompressLevel(nil, data, 3)
		dst := make([]byte, len(data))

		b.Run(size.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				benchN, _ = ZstdDecompressInto(dst, compressed)
			}
		})
	}
}

func BenchmarkZstdDecompressPartial(b *testing.B) {
	data := bytes.Repeat([]byte("ABCDEFGH12345678"), 1024*1024/16)
	compressed, _ := ZstdCompressLevel(nil, data, 3)

	partialSizes := []int{64, 256, 1024, 4096}

	for _, partialSize := range partialSizes {
		b.Run(bytesName(partialSize), func(b *testing.B) {
			b.SetBytes(int64(partialSize))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				benchDecompressed, _ = ZstdDecompressPartial(compressed, partialSize)
			}
		})
	}
}

func BenchmarkZstdDecompressPartialInto(b *testing.B) {
	data := bytes.Repeat([]byte("ABCDEFGH12345678"), 1024*1024/16)
	compressed, _ := ZstdCompressLevel(nil, data, 3)

	partialSizes := []int{64, 256, 1024, 4096}

	for _, partialSize := range partialSizes {
		dst := make([]byte, partialSize)

		b.Run(bytesName(partialSize), func(b *testing.B) {
			b.SetBytes(int64(partialSize))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				benchN, _ = ZstdDecompressPartialInto(dst, compressed)
			}
		})
	}
}

func BenchmarkZstdCompressionLevels(b *testing.B) {
	data := bytes.Repeat([]byte("ABCDEFGH12345678"), 10*1024/16)

	levels := []int{1, 3, 5, 7, 9}
	for _, level := range levels {
		b.Run(levelName(level), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				benchCompressed, _ = ZstdCompressLevel(nil, data, level)
			}
		})
	}
}

func BenchmarkZstdRoundTrip(b *testing.B) {
	data := bytes.Repeat([]byte("ABCDEFGH12345678"), 10*1024/16)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		compressed, _ := ZstdCompressLevel(nil, data, 3)
		benchDecompressed, _ = ZstdDecompress(nil, compressed)
	}
}

func bytesName(size int) string {
	if size >= 1024 {
		return string(rune('0'+size/1024)) + "KB"
	}
	return string(rune('0'+size/64)) + "x64B"
}

func levelName(level int) string {
	return "level-" + string(rune('0'+level))
}
