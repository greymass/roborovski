package logger

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestValidateCategory tests category validation logic
func TestValidateCategory(t *testing.T) {
	tests := []struct {
		name     string
		category string
		want     bool
	}{
		{"empty string", "", false},
		{"valid lowercase", "test", true},
		{"valid with underscore", "test_category", true},
		{"valid with numbers", "test123", true},
		{"valid with hyphen", "test-category", true},
		{"single uppercase", "A", false},
		{"uppercase word", "TEST", false},
		{"mixed case", "Test", false},
		{"mixed case middle", "testCase", false},
		{"multiple words lowercase", "test category", true},
		{"special chars lowercase", "test:category", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateCategory(tt.category)
			if got != tt.want {
				t.Errorf("validateCategory(%q) = %v, want %v", tt.category, got, tt.want)
			}
		})
	}
}

// TestLevelString tests Level.String() method
func TestLevelString(t *testing.T) {
	tests := []struct {
		level Level
		want  string
	}{
		{LevelDebug, "DEBUG"},
		{LevelInfo, "INFO"},
		{LevelWarning, "WARNING"},
		{LevelError, "ERROR"},
		{LevelFatal, "FATAL"},
		{Level(999), "UNKNOWN"},
		{Level(-1), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.level.String()
			if got != tt.want {
				t.Errorf("Level(%d).String() = %q, want %q", tt.level, got, tt.want)
			}
		})
	}
}

// TestLevelForCategory tests category to level mapping
func TestLevelForCategory(t *testing.T) {
	tests := []struct {
		category string
		want     Level
	}{
		{"error", LevelError},
		{"warning", LevelWarning},
		{"debug", LevelDebug},
		{"debug-startup", LevelDebug},
		{"debug-timing", LevelDebug},
		{"debugging", LevelDebug}, // starts with "debug"
		{"info", LevelInfo},
		{"sync", LevelInfo},
		{"custom", LevelInfo},
		{"", LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.category, func(t *testing.T) {
			got := levelForCategory(tt.category)
			if got != tt.want {
				t.Errorf("levelForCategory(%q) = %v, want %v", tt.category, got, tt.want)
			}
		})
	}
}

// TestBufferPool tests buffer pooling functionality
func TestBufferPool(t *testing.T) {
	// Get a buffer
	buf1 := getBuffer()
	if buf1 == nil {
		t.Fatal("getBuffer() returned nil")
	}

	// Buffer should be empty
	if buf1.Len() != 0 {
		t.Errorf("getBuffer() returned non-empty buffer: len=%d", buf1.Len())
	}

	// Write some data
	buf1.WriteString("test data")

	// Return to pool
	putBuffer(buf1)

	// Get another buffer (might be the same one)
	buf2 := getBuffer()
	if buf2 == nil {
		t.Fatal("getBuffer() returned nil on second call")
	}

	// Should be reset/empty
	if buf2.Len() != 0 {
		t.Errorf("getBuffer() returned non-reset buffer: len=%d", buf2.Len())
	}

	putBuffer(buf2)
}

// TestBufferPoolLargeBuffer tests that large buffers are not returned to pool
func TestBufferPoolLargeBuffer(t *testing.T) {
	buf := getBuffer()

	// Write a large amount of data (> 64KB)
	largeData := make([]byte, 65*1024)
	for i := range largeData {
		largeData[i] = 'x'
	}
	buf.Write(largeData)

	// Capacity should be > 64KB
	if buf.Cap() <= 64*1024 {
		t.Fatalf("Buffer capacity too small for test: %d", buf.Cap())
	}

	// Return to pool (should not actually be pooled)
	putBuffer(buf)

	// This test verifies the behavior exists; we can't easily verify
	// that the buffer wasn't pooled without inspecting pool internals
}

// TestLoggerMinLevel tests log level filtering
func TestLoggerMinLevel(t *testing.T) {
	tests := []struct {
		name      string
		minLevel  Level
		category  string
		shouldLog bool
	}{
		{"debug logs when min is debug", LevelDebug, "debug", true},
		{"info logs when min is debug", LevelDebug, "info", true},
		{"debug filtered when min is info", LevelInfo, "debug", false},
		{"info logs when min is info", LevelInfo, "info", true},
		{"warning logs when min is info", LevelInfo, "warning", true},
		{"error logs when min is info", LevelInfo, "error", true},
		{"info filtered when min is error", LevelError, "info", false},
		{"warning filtered when min is error", LevelError, "warning", false},
		{"error logs when min is error", LevelError, "error", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := &Logger{
				output:   &buf,
				minLevel: tt.minLevel,
			}

			logger.Printf(tt.category, "test message")

			logged := buf.Len() > 0
			if logged != tt.shouldLog {
				t.Errorf("minLevel=%v category=%q: logged=%v, want shouldLog=%v",
					tt.minLevel, tt.category, logged, tt.shouldLog)
			}
		})
	}
}

// TestExplicitFilterOverridesLevel tests that explicit filter allows debug categories without debug level
func TestExplicitFilterOverridesLevel(t *testing.T) {
	tests := []struct {
		name      string
		minLevel  Level
		filter    []string
		category  string
		shouldLog bool
	}{
		// Without filter, level controls
		{"debug blocked at info level", LevelInfo, nil, "debug-startup", false},
		{"debug allowed at debug level", LevelDebug, nil, "debug-startup", true},

		// Explicit filter overrides level
		{"debug allowed by explicit filter at info level", LevelInfo, []string{"debug-startup"}, "debug-startup", true},
		{"debug blocked if not in filter", LevelInfo, []string{"sync"}, "debug-startup", false},

		// --debug + filter: both must pass
		{"debug allowed at debug level with matching filter", LevelDebug, []string{"debug-startup"}, "debug-startup", true},
		{"debug blocked at debug level without matching filter", LevelDebug, []string{"sync"}, "debug-startup", false},

		// Non-debug categories still work normally
		{"info allowed at info level", LevelInfo, nil, "sync", true},
		{"info allowed with matching filter", LevelInfo, []string{"sync"}, "sync", true},
		{"info blocked with non-matching filter", LevelInfo, []string{"store"}, "sync", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := &Logger{
				output:   &buf,
				minLevel: tt.minLevel,
			}

			if tt.filter != nil {
				logger.SetCategoryFilter(tt.filter)
			}

			logger.Printf(tt.category, "test message")

			logged := buf.Len() > 0
			if logged != tt.shouldLog {
				t.Errorf("minLevel=%v filter=%v category=%q: logged=%v, want shouldLog=%v",
					tt.minLevel, tt.filter, tt.category, logged, tt.shouldLog)
			}
		})
	}
}

// TestEmptyCategory tests logging with empty category
func TestEmptyCategory(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Printf("", "test message")

	got := buf.String()
	// Empty category is invalid and should be replaced
	if !strings.Contains(got, "invalid_category") {
		t.Errorf("Empty category not replaced with invalid_category: %q", got)
	}
}

// TestMultipleSpacesInMessage tests that message formatting is preserved
func TestMultipleSpacesInMessage(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Printf("test", "message  with   multiple    spaces")

	got := buf.String()
	if !strings.Contains(got, "message  with   multiple    spaces") {
		t.Error("Message spacing not preserved")
	}
}

// TestSpecialCharactersInMessage tests special character handling
func TestSpecialCharactersInMessage(t *testing.T) {
	tests := []struct {
		name    string
		message string
	}{
		{"tabs", "message\twith\ttabs"},
		{"unicode", "message with unicode: 你好"},
		{"emoji", "message with emoji: 🚀"},
		{"quotes", `message with "quotes"`},
		{"backslash", `message with \backslash`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			defer SetOutput(nil)

			Printf("test", "%s", tt.message)

			got := buf.String()
			if !strings.Contains(got, tt.message) {
				t.Errorf("Special characters not preserved: want %q in %q", tt.message, got)
			}
		})
	}
}

// TestLongCategoryName tests handling of very long category names
func TestLongCategoryName(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	longCategory := strings.Repeat("a", 100)
	Printf(longCategory, "test")

	got := buf.String()
	if !strings.Contains(got, longCategory) {
		t.Error("Long category name not logged")
	}
}

// TestCategoryWidthZero tests that zero width disables padding
func TestCategoryWidthZero(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0, // No padding
	}

	logger.Printf("short", "message1")
	line1 := buf.String()
	buf.Reset()

	logger.Printf("verylongcategory", "message2")
	line2 := buf.String()

	// Without padding, positions should differ
	idx1 := strings.Index(line1, "message1")
	idx2 := strings.Index(line2, "message2")

	if idx1 == idx2 {
		t.Error("Category width 0 should not pad categories")
	}
}

// TestRegisterCategoriesEmpty tests registering zero categories
func TestRegisterCategoriesEmpty(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	// Register no categories
	logger.RegisterCategories()

	// Width should be 1 (0 + 1)
	if logger.categoryWidth != 1 {
		t.Errorf("RegisterCategories() with no args: width=%d, want 1", logger.categoryWidth)
	}
}

// TestRegisterCategoriesSingle tests registering one category
func TestRegisterCategoriesSingle(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	logger.RegisterCategories("test")

	// Width should be len("test") + 1 = 5
	if logger.categoryWidth != 5 {
		t.Errorf("RegisterCategories(\"test\"): width=%d, want 5", logger.categoryWidth)
	}
}

// TestDefaultLoggerInit tests that default logger is initialized
func TestDefaultLoggerInit(t *testing.T) {
	if defaultLogger == nil {
		t.Fatal("defaultLogger is nil")
	}

	if defaultLogger.output == nil {
		t.Error("defaultLogger.output is nil")
	}

	if defaultLogger.minLevel != LevelInfo {
		t.Errorf("defaultLogger.minLevel = %v, want %v", defaultLogger.minLevel, LevelInfo)
	}
}

// TestPrintfFormattingEdgeCases tests Printf with various format strings
func TestPrintfFormattingEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		format string
		args   []interface{}
		want   string
	}{
		{"no args", "simple message", nil, "simple message"},
		{"percent sign", "100%% complete", nil, "100% complete"},
		{"multiple formats", "%s: %d/%d (%.1f%%)", []interface{}{"Progress", 50, 100, 50.0}, "Progress: 50/100 (50.0%)"},
		{"verb without arg", "missing %s", nil, "missing %!s(MISSING)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			defer SetOutput(nil)

			Printf("test", tt.format, tt.args...)

			got := buf.String()
			if !strings.Contains(got, tt.want) {
				t.Errorf("Printf formatting failed: want %q in %q", tt.want, got)
			}
		})
	}
}

// TestPrintlnMultipleArgs tests Println with various argument combinations
func TestPrintlnMultipleArgs(t *testing.T) {
	tests := []struct {
		name string
		args []interface{}
		want string
	}{
		{"one string", []interface{}{"hello"}, "hello"},
		{"two strings", []interface{}{"hello", "world"}, "hello world"},
		{"mixed types", []interface{}{"count", 42, "done"}, "count 42 done"},
		{"with nil", []interface{}{"value", nil}, "value <nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			defer SetOutput(nil)

			Println("test", tt.args...)

			got := buf.String()
			if !strings.Contains(got, tt.want) {
				t.Errorf("Println() = %q, want to contain %q", got, tt.want)
			}
		})
	}
}

// TestCategoryPaddingExactWidth tests category padding when name equals width
func TestCategoryPaddingExactWidth(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	// Register category that will set width to 5 ("test" = 4 + 1)
	logger.RegisterCategories("test")

	// Log with exact width (4 chars)
	logger.Printf("test", "msg")
	line := buf.String()

	// Should have one space padding (to reach width 5)
	if !strings.Contains(line, "test ") {
		t.Errorf("Category padding incorrect: %q", line)
	}
}

// TestCategoryPaddingShort tests category padding for short names
func TestCategoryPaddingShort(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	// Register longer category to set width
	logger.RegisterCategories("a", "longestcategory")

	// Log with short category
	logger.Printf("a", "msg")
	line := buf.String()

	// Count spaces after 'a' before 'msg'
	aPos := strings.Index(line, " a ")
	msgPos := strings.Index(line, "msg")
	if aPos == -1 || msgPos == -1 {
		t.Fatalf("Could not find category or message in: %q", line)
	}

	// Should have padding
	spaceBetween := msgPos - (aPos + 2)               // +2 for " a"
	expectedPadding := len("longestcategory") + 1 - 1 // width - len("a")
	if spaceBetween < expectedPadding {
		t.Errorf("Insufficient padding: got %d spaces, want at least %d", spaceBetween, expectedPadding)
	}
}

// TestMessageAlreadyEndsWithNewline tests Printf doesn't double newline
func TestMessageAlreadyEndsWithNewline(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	logger.Printf("test", "message\n")
	got := buf.String()

	// Count newlines at end
	newlines := 0
	for i := len(got) - 1; i >= 0 && got[i] == '\n'; i-- {
		newlines++
	}

	if newlines > 1 {
		t.Errorf("Printf() added extra newlines: %d newlines at end", newlines)
	}
}

// TestPrintlnAlwaysAddsNewline tests that Println always adds newline
func TestPrintlnAlwaysAddsNewline(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	logger.Println("test", "message without newline")
	got := buf.String()

	if !strings.HasSuffix(got, "\n") {
		t.Error("Println() should always end with newline")
	}
}

// TestGlobalFunctions tests that global functions use defaultLogger
func TestGlobalFunctions(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	// Test Printf
	Printf("test", "printf: %d", 1)
	if !strings.Contains(buf.String(), "printf: 1") {
		t.Error("Printf() global function failed")
	}
	buf.Reset()

	// Test Println
	Println("test", "println", 2)
	if !strings.Contains(buf.String(), "println 2") {
		t.Error("Println() global function failed")
	}
	buf.Reset()

	// Test Error
	Error("error: %s", "test")
	if !strings.Contains(buf.String(), "error: test") {
		t.Error("Error() global function failed")
	}
	buf.Reset()

	// Test Warning
	Warning("warning: %d", 3)
	if !strings.Contains(buf.String(), "warning: 3") {
		t.Error("Warning() global function failed")
	}
}

// TestGlobalRegisterCategories tests global RegisterCategories function
func TestGlobalRegisterCategories(t *testing.T) {
	// Save original width
	originalWidth := defaultLogger.categoryWidth
	defer func() {
		defaultLogger.categoryWidth = originalWidth
	}()

	RegisterCategories("cat1", "cat2", "verylongcategory")

	expectedWidth := len("verylongcategory") + 1
	if defaultLogger.categoryWidth != expectedWidth {
		t.Errorf("RegisterCategories() set width to %d, want %d",
			defaultLogger.categoryWidth, expectedWidth)
	}
}

// TestPrintlnWithPaddingAndInvalidCategory tests Println with category padding and validation
func TestPrintlnWithPaddingAndInvalidCategory(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 15, // Set padding width
	}

	// Test with invalid category (uppercase)
	logger.Println("INVALID", "test message")

	got := buf.String()
	if !strings.Contains(got, "invalid_category") {
		t.Error("Println() should replace invalid category")
	}
	if !strings.Contains(got, "test message") {
		t.Error("Println() should contain message")
	}
}

// TestPrintlnMinLevelFiltering tests that Println respects minLevel
func TestPrintlnMinLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelError, // Only error and above
		categoryWidth: 0,
	}

	// Try to log info (should be filtered)
	logger.Println("info", "should not appear")
	if buf.Len() > 0 {
		t.Error("Println() should filter messages below minLevel")
	}

	// Log error (should appear)
	logger.Println("error", "should appear")
	if buf.Len() == 0 {
		t.Error("Println() should log messages at or above minLevel")
	}
}

// TestPrintlnWithCategoryPadding tests Println with various padding scenarios
func TestPrintlnWithCategoryPadding(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 10,
	}

	// Short category should be padded
	logger.Println("a", "msg")
	line := buf.String()

	// Check that padding was applied
	if !strings.Contains(line, "a ") {
		t.Error("Println() should pad short categories")
	}

	// Category longer than width (padding should be 0)
	buf.Reset()
	logger.Println("verylongcategory", "msg2")
	line2 := buf.String()
	if !strings.Contains(line2, "verylongcategory") {
		t.Error("Println() should handle long categories")
	}
}

// TestSetLogFile tests file logging setup
func TestSetLogFile(t *testing.T) {
	tmpFile := t.TempDir() + "/test.log"

	err := SetLogFile(tmpFile)
	if err != nil {
		t.Fatalf("SetLogFile() error = %v", err)
	}
	defer Close()

	Printf("test", "log to file")

	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !strings.Contains(string(content), "log to file") {
		t.Errorf("Log file should contain message, got: %q", string(content))
	}
}

// TestSetLogFileReplace tests replacing an existing log file
func TestSetLogFileReplace(t *testing.T) {
	tmpDir := t.TempDir()
	file1 := tmpDir + "/test1.log"
	file2 := tmpDir + "/test2.log"

	err := SetLogFile(file1)
	if err != nil {
		t.Fatalf("SetLogFile(file1) error = %v", err)
	}

	Printf("test", "message to file1")

	err = SetLogFile(file2)
	if err != nil {
		t.Fatalf("SetLogFile(file2) error = %v", err)
	}
	defer Close()

	Printf("test", "message to file2")

	content2, err := os.ReadFile(file2)
	if err != nil {
		t.Fatalf("Failed to read file2: %v", err)
	}

	if !strings.Contains(string(content2), "message to file2") {
		t.Errorf("File2 should contain message, got: %q", string(content2))
	}
}

// TestSetLogFileError tests SetLogFile with invalid path
func TestSetLogFileError(t *testing.T) {
	err := SetLogFile("/nonexistent/path/to/file.log")
	if err == nil {
		Close()
		t.Error("SetLogFile() should return error for invalid path")
	}
}

// TestClose tests closing log file
func TestClose(t *testing.T) {
	tmpFile := t.TempDir() + "/test.log"

	err := SetLogFile(tmpFile)
	if err != nil {
		t.Fatalf("SetLogFile() error = %v", err)
	}

	Close()

	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	Printf("test", "after close")

	if buf.Len() == 0 {
		t.Error("After Close(), logging should still work to stdout")
	}
}

// TestCloseWithoutFile tests Close when no file is open
func TestCloseWithoutFile(t *testing.T) {
	Close()
}

// TestGlobalSetMinLevel tests the global SetMinLevel function
func TestGlobalSetMinLevel(t *testing.T) {
	originalLevel := defaultLogger.minLevel
	defer func() {
		mu.Lock()
		defaultLogger.minLevel = originalLevel
		mu.Unlock()
	}()

	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	SetMinLevel(LevelError)

	Printf("info", "should not appear")
	if buf.Len() > 0 {
		t.Error("SetMinLevel(LevelError) should filter info messages")
	}

	Printf("error", "should appear")
	if buf.Len() == 0 {
		t.Error("SetMinLevel(LevelError) should allow error messages")
	}
}

// TestInstanceSetMinLevel tests Logger.SetMinLevel
func TestInstanceSetMinLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	logger.SetMinLevel(LevelWarning)

	logger.Printf("info", "should not appear")
	if buf.Len() > 0 {
		t.Error("SetMinLevel(LevelWarning) should filter info messages")
	}

	logger.Printf("warning", "should appear")
	if buf.Len() == 0 {
		t.Error("SetMinLevel(LevelWarning) should allow warning messages")
	}
}

// TestGlobalSetCategoryFilter tests the global SetCategoryFilter function
func TestGlobalSetCategoryFilter(t *testing.T) {
	originalFilter := defaultLogger.categoryFilter
	defer func() {
		mu.Lock()
		defaultLogger.categoryFilter = originalFilter
		mu.Unlock()
	}()

	var buf bytes.Buffer
	SetOutput(&buf)
	defer SetOutput(nil)

	SetCategoryFilter([]string{"allowed"})

	Printf("allowed", "should appear")
	if buf.Len() == 0 {
		t.Error("SetCategoryFilter should allow filtered category")
	}

	buf.Reset()
	Printf("blocked", "should not appear")
	if buf.Len() > 0 {
		t.Error("SetCategoryFilter should block non-filtered category")
	}

	SetCategoryFilter([]string{})
	buf.Reset()
	Printf("anycat", "should appear after clear")
	if buf.Len() == 0 {
		t.Error("SetCategoryFilter([]) should allow all categories")
	}
}

// TestIsCategoryEnabled tests the IsCategoryEnabled function
func TestIsCategoryEnabled(t *testing.T) {
	originalFilter := defaultLogger.categoryFilter
	defer func() {
		mu.Lock()
		defaultLogger.categoryFilter = originalFilter
		mu.Unlock()
	}()

	if !IsCategoryEnabled("test") {
		t.Error("IsCategoryEnabled should return true when no filter is set")
	}

	SetCategoryFilter([]string{"allowed"})

	if !IsCategoryEnabled("allowed") {
		t.Error("IsCategoryEnabled should return true for filtered category")
	}

	if IsCategoryEnabled("blocked") {
		t.Error("IsCategoryEnabled should return false for non-filtered category")
	}

	SetCategoryFilter([]string{})
	if !IsCategoryEnabled("any") {
		t.Error("IsCategoryEnabled should return true after filter cleared")
	}
}

// TestFormatNumber tests the FormatNumber utility function
func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{0, "  0.0 "},
		{1, "  1.0 "},
		{100, "100.0 "},
		{999, "999.0 "},
		{1000, "  1.0k"},
		{1500, "  1.5k"},
		{10000, " 10.0k"},
		{100000, "100.0k"},
		{999999, "1000.0k"},
		{1000000, "  1.0m"},
		{1500000, "  1.5m"},
		{10000000, " 10.0m"},
		{100000000, "100.0m"},
		{999999999, "1000.0m"},
		{1000000000, "  1.0b"},
		{1500000000, "  1.5b"},
		{10000000000, " 10.0b"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatNumber(tt.input)
			if got != tt.want {
				t.Errorf("FormatNumber(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestPrintlnCategoryFilterWithExplicitAllow tests Println respects explicit category filter
func TestPrintlnCategoryFilterWithExplicitAllow(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		output:        &buf,
		minLevel:      LevelInfo,
		categoryWidth: 0,
	}

	logger.SetCategoryFilter([]string{"debug-test"})

	logger.Println("debug-test", "explicit allow bypasses level")

	if buf.Len() == 0 {
		t.Error("Println should log explicitly allowed debug category even at info level")
	}

	buf.Reset()
	logger.Println("other", "should not appear")

	if buf.Len() > 0 {
		t.Error("Println should filter categories not in explicit list")
	}
}
