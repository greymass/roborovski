package logger

import (
	"bytes"
	"strings"
	"testing"
)

// TestPrintf tests basic formatted logging
func TestPrintf(t *testing.T) {
	tests := []struct {
		name     string
		category string
		format   string
		args     []interface{}
		want     string
	}{
		{
			name:     "simple message",
			category: "test",
			format:   "hello world",
			args:     nil,
			want:     "test hello world",
		},
		{
			name:     "formatted message",
			category: "sync",
			format:   "Block: %d / %d",
			args:     []interface{}{100, 1000},
			want:     "sync Block: 100 / 1000",
		},
		{
			name:     "error category",
			category: "error",
			format:   "Failed to process: %s",
			args:     []interface{}{"block 123"},
			want:     "error Failed to process: block 123",
		},
		{
			name:     "warning category",
			category: "warning",
			format:   "Memory usage: %d%%",
			args:     []interface{}{85},
			want:     "warning Memory usage: 85%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			defer SetOutput(nil)

			Printf(tt.category, tt.format, tt.args...)

			got := buf.String()
			// Check that output contains expected category and message
			if !strings.Contains(got, tt.want) {
				t.Errorf("Printf() output = %q, want to contain %q", got, tt.want)
			}
			// Check for newline
			if !strings.HasSuffix(got, "\n") {
				t.Error("Printf() output should end with newline")
			}
			// Check for timestamp (format: 2006-01-02 15:04:05)
			if len(got) < 19 {
				t.Errorf("Printf() output too short to contain timestamp: %q", got)
			}
		})
	}
}

// TestPrintln tests basic message logging
func TestPrintln(t *testing.T) {
	tests := []struct {
		name     string
		category string
		args     []interface{}
		want     string
	}{
		{
			name:     "single arg",
			category: "test",
			args:     []interface{}{"hello"},
			want:     "test hello",
		},
		{
			name:     "multiple args",
			category: "sync",
			args:     []interface{}{"Processing", "block", 100},
			want:     "sync Processing block 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			defer SetOutput(nil)

			Println(tt.category, tt.args...)

			got := buf.String()
			if !strings.Contains(got, tt.want) {
				t.Errorf("Println() output = %q, want to contain %q", got, tt.want)
			}
			if !strings.HasSuffix(got, "\n") {
				t.Error("Println() output should end with newline")
			}
		})
	}
}

// TestError tests error logging convenience function
func TestError(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Error("Test error: %d", 404)

	got := buf.String()
	if !strings.Contains(got, "error") {
		t.Errorf("Error() output = %q, should contain 'error' category", got)
	}
	if !strings.Contains(got, "Test error: 404") {
		t.Errorf("Error() output = %q, should contain message", got)
	}
}

// TestWarning tests warning logging convenience function
func TestWarning(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Warning("Test warning: %s", "memory high")

	got := buf.String()
	if !strings.Contains(got, "warning") {
		t.Errorf("Warning() output = %q, should contain 'warning' category", got)
	}
	if !strings.Contains(got, "Test warning: memory high") {
		t.Errorf("Warning() output = %q, should contain message", got)
	}
}

// TestSetOutput tests changing output destination
func TestSetOutput(t *testing.T) {
	// Test setting custom output
	var buf bytes.Buffer
	SetOutput(&buf)
	Printf("test", "custom output")

	if buf.Len() == 0 {
		t.Error("SetOutput() failed to redirect output to buffer")
	}

	// Test restoring stdout with nil
	SetOutput(nil)
	buf.Reset()
	Printf("test", "should not go to buffer")

	if buf.Len() != 0 {
		t.Error("SetOutput(nil) failed to restore stdout")
	}
}

// TestRegisterCategories tests category width alignment
func TestRegisterCategories(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	// Register categories with different lengths
	RegisterCategories("a", "medium", "verylongcategory")

	// Log with short category - should be padded
	Printf("a", "test1")
	line1 := buf.String()
	buf.Reset()

	// Log with long category
	Printf("verylongcategory", "test2")
	line2 := buf.String()

	// Both should align (have same distance between timestamp and message)
	// Extract position of "test1" and "test2" after category
	idx1 := strings.Index(line1, "test1")
	idx2 := strings.Index(line2, "test2")

	if idx1 != idx2 {
		t.Errorf("RegisterCategories() alignment failed: test1 at pos %d, test2 at pos %d\nline1: %q\nline2: %q",
			idx1, idx2, line1, line2)
	}
}

// TestLoggerInstance tests creating and using a custom Logger instance
func TestLoggerInstance(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:   &buf,
		minLevel: LevelInfo,
	}

	logger.Printf("test", "instance message")

	got := buf.String()
	if !strings.Contains(got, "test instance message") {
		t.Errorf("Logger instance Printf() = %q, want to contain message", got)
	}
}

// TestLoggerInstanceMethods tests Logger instance methods
func TestLoggerInstanceMethods(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:   &buf,
		minLevel: LevelInfo,
	}

	t.Run("Error method", func(t *testing.T) {
		buf.Reset()
		logger.Error("test error: %d", 500)
		if !strings.Contains(buf.String(), "error") {
			t.Error("Logger.Error() should log with error category")
		}
	})

	t.Run("Warning method", func(t *testing.T) {
		buf.Reset()
		logger.Warning("test warning: %s", "disk space")
		if !strings.Contains(buf.String(), "warning") {
			t.Error("Logger.Warning() should log with warning category")
		}
	})

	t.Run("Println method", func(t *testing.T) {
		buf.Reset()
		logger.Println("info", "test", "println")
		if !strings.Contains(buf.String(), "info test println") {
			t.Errorf("Logger.Println() = %q, want message", buf.String())
		}
	})
}

// TestLoggerWithCategoryWidth tests category padding
func TestLoggerWithCategoryWidth(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:   &buf,
		minLevel: LevelInfo,
	}

	// Register categories to set width
	logger.RegisterCategories("short", "verylongcategory")

	// Log with short category
	logger.Printf("short", "msg1")
	line1 := buf.String()
	buf.Reset()

	// Log with long category
	logger.Printf("verylongcategory", "msg2")
	line2 := buf.String()

	// Check alignment
	idx1 := strings.Index(line1, "msg1")
	idx2 := strings.Index(line2, "msg2")

	if idx1 != idx2 {
		t.Errorf("Category padding failed: msg1 at %d, msg2 at %d", idx1, idx2)
	}
}

// TestConcurrentLogging tests thread-safe logging
func TestConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			Printf("test", "concurrent log %d", n)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have 10 lines
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 10 {
		t.Errorf("Concurrent logging: got %d lines, want 10", len(lines))
	}
}

// TestInvalidCategory tests that invalid categories are handled
func TestInvalidCategory(t *testing.T) {
	tests := []struct {
		name     string
		category string
	}{
		{"uppercase", "INVALID"},
		{"mixed case", "MixedCase"},
		{"with uppercase letter", "testError"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			defer SetOutput(nil)

			Printf(tt.category, "test message")

			// Should replace with "invalid_category"
			got := buf.String()
			if !strings.Contains(got, "invalid_category") {
				t.Errorf("Invalid category %q not replaced: %q", tt.category, got)
			}
		})
	}
}

// TestMessageWithNewline tests that messages ending with newline aren't double-newlined
func TestMessageWithNewline(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Printf("test", "message with newline\n")

	got := buf.String()
	// Should have exactly one newline at the end
	if strings.HasSuffix(got, "\n\n") {
		t.Error("Printf() should not double-newline messages that already end with \\n")
	}
}

// TestTimestampFormat tests that timestamp is in correct format
func TestTimestampFormat(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Printf("test", "check timestamp")

	got := buf.String()
	// Timestamp should be first 19 characters: "2006-01-02 15:04:05"
	if len(got) < 19 {
		t.Fatalf("Output too short: %q", got)
	}

	timestamp := got[:19]
	// Check format roughly (contains date separators and time)
	if !strings.Contains(timestamp, "-") || !strings.Contains(timestamp, ":") {
		t.Errorf("Timestamp format incorrect: %q", timestamp)
	}
}
