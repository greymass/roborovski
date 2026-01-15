package tracereader

import (
	"path/filepath"
	"testing"
)

// BenchmarkGetInfoWithCache measures GetInfo performance with caching enabled (realistic)
func BenchmarkGetInfoWithCache(b *testing.B) {
	// Create temp directory with a trace index file
	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexPath, 100, 50); err != nil {
		b.Fatalf("Failed to create index file: %v", err)
	}

	conf := &Config{
		Stride: 500,
		Dir:    tmpDir,
		Debug:  false,
	}

	// Clear cache before benchmark
	getInfoCacheMu.Lock()
	getInfoCacheDir = ""
	getInfoCacheMu.Unlock()

	b.ResetTimer()

	// Benchmark includes both cache hits and misses (realistic scenario)
	// First call will miss, subsequent calls will hit until TTL expires
	for i := 0; i < b.N; i++ {
		_, err := GetInfo(conf)
		if err != nil {
			b.Fatalf("GetInfo failed: %v", err)
		}
	}
}

// BenchmarkGetInfoColdCache measures GetInfo performance without caching (worst case)
func BenchmarkGetInfoColdCache(b *testing.B) {
	// Create temp directory with a trace index file
	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "trace_index_0000000000-0000000500.log")
	if err := createTestIndexFile(indexPath, 100, 50); err != nil {
		b.Fatalf("Failed to create index file: %v", err)
	}

	conf := &Config{
		Stride: 500,
		Dir:    tmpDir,
		Debug:  false,
	}

	b.ResetTimer()

	// Force cache miss on every call by clearing cache
	for i := 0; i < b.N; i++ {
		// Clear cache to simulate cold start
		getInfoCacheMu.Lock()
		getInfoCacheDir = ""
		getInfoCacheMu.Unlock()

		_, err := GetInfo(conf)
		if err != nil {
			b.Fatalf("GetInfo failed: %v", err)
		}
	}
}
