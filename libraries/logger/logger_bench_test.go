package logger

import (
	"io"
	"testing"
)

// discardWriter is an io.Writer that discards all data
type discardWriter struct{}

func (discardWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func BenchmarkPrintf(b *testing.B) {
	SetOutput(discardWriter{})
	defer SetOutput(nil) // Restore stdout
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Printf("sync", "Block: %d / %d", i, 12345678)
	}
}

func BenchmarkPrintfWithValidation(b *testing.B) {
	SetOutput(discardWriter{})
	defer SetOutput(nil) // Restore stdout
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Use non-constant category to force validation
		cat := "sync"
		Printf(cat, "Block: %d / %d", i, 12345678)
	}
}

func BenchmarkPrintln(b *testing.B) {
	SetOutput(discardWriter{})
	defer SetOutput(nil) // Restore stdout
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Println("sync", "Processing block", i)
	}
}

func BenchmarkError(b *testing.B) {
	SetOutput(discardWriter{})
	defer SetOutput(nil) // Restore stdout
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Error("Failed to process: %d", i)
	}
}

func BenchmarkConcurrentPrintf(b *testing.B) {
	SetOutput(discardWriter{})
	defer SetOutput(nil) // Restore stdout
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Printf("sync", "Block: %d / %d", i, 12345678)
			i++
		}
	})
}

func BenchmarkBufferPooling(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := getBuffer()
		buf.WriteString("test message")
		putBuffer(buf)
	}
}

func BenchmarkCategoryValidation(b *testing.B) {
	categories := []string{"sync", "error", "warning", "INVALID", "Test"}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		validateCategory(categories[i%len(categories)])
	}
}

// Baseline: stdlib fmt.Printf for comparison
func BenchmarkStdlibFprintf(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		io.WriteString(discardWriter{}, "sync\tBlock: 12345 / 12345678\n")
	}
}
