package logger

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Logger struct {
	output         io.Writer
	minLevel       Level
	categoryWidth  int
	categoryFilter map[string]bool
}

var (
	defaultLogger *Logger
	mu            sync.Mutex
	logFile       *os.File
)

func init() {
	defaultLogger = &Logger{
		output:   os.Stdout,
		minLevel: LevelInfo,
	}
}

func RegisterCategories(categories ...string) {
	defaultLogger.RegisterCategories(categories...)
}

func (l *Logger) RegisterCategories(categories ...string) {
	mu.Lock()
	defer mu.Unlock()

	maxLen := 0
	for _, cat := range categories {
		if len(cat) > maxLen {
			maxLen = len(cat)
		}
	}
	l.categoryWidth = maxLen + 1
}

func SetOutput(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	if w == nil {
		defaultLogger.output = os.Stdout
	} else {
		defaultLogger.output = w
	}
}

func SetLogFile(path string) error {
	mu.Lock()
	defer mu.Unlock()

	if logFile != nil {
		logFile.Close()
		logFile = nil
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	logFile = f
	defaultLogger.output = io.MultiWriter(os.Stdout, f)
	return nil
}

func Close() {
	mu.Lock()
	defer mu.Unlock()

	if logFile != nil {
		logFile.Sync()
		logFile.Close()
		logFile = nil
		defaultLogger.output = os.Stdout
	}
}

func SetMinLevel(level Level) {
	mu.Lock()
	defer mu.Unlock()
	defaultLogger.minLevel = level
}

func (l *Logger) SetMinLevel(level Level) {
	mu.Lock()
	defer mu.Unlock()
	l.minLevel = level
}

func Printf(category string, format string, v ...interface{}) {
	defaultLogger.Printf(category, format, v...)
}

func Println(category string, v ...interface{}) {
	defaultLogger.Println(category, v...)
}

func Error(format string, v ...interface{}) {
	defaultLogger.Printf("error", format, v...)
}

func Warning(format string, v ...interface{}) {
	defaultLogger.Printf("warning", format, v...)
}

func (l *Logger) shouldLog(category string) (bool, string) {
	explicitlyAllowed := l.categoryFilter != nil && l.categoryFilter[category]

	if !explicitlyAllowed {
		level := levelForCategory(category)
		if level < l.minLevel {
			return false, ""
		}
	}

	if !explicitlyAllowed && !l.isCategoryAllowed(category) && category != "error" && category != "warning" {
		return false, ""
	}

	if !validateCategory(category) {
		category = "invalid_category"
	}

	return true, category
}

func (l *Logger) writePrefix(buf *bytes.Buffer, category string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	buf.WriteString(timestamp)
	buf.WriteByte(' ')

	buf.WriteString(category)
	if l.categoryWidth > 0 {
		padding := l.categoryWidth - len(category)
		for i := 0; i < padding; i++ {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(' ')
}

func (l *Logger) Printf(category string, format string, v ...interface{}) {
	ok, category := l.shouldLog(category)
	if !ok {
		return
	}

	buf := getBuffer()
	defer putBuffer(buf)

	l.writePrefix(buf, category)
	fmt.Fprintf(buf, format, v...)

	if buf.Len() > 0 && buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}

	mu.Lock()
	l.output.Write(buf.Bytes())
	mu.Unlock()
}

func (l *Logger) Println(category string, v ...interface{}) {
	ok, category := l.shouldLog(category)
	if !ok {
		return
	}

	buf := getBuffer()
	defer putBuffer(buf)

	l.writePrefix(buf, category)
	fmt.Fprintln(buf, v...)

	mu.Lock()
	l.output.Write(buf.Bytes())
	mu.Unlock()
}

func (l *Logger) Error(format string, v ...interface{}) {
	l.Printf("error", format, v...)
}

func (l *Logger) Warning(format string, v ...interface{}) {
	l.Printf("warning", format, v...)
}

func (l *Logger) Fatal(format string, v ...interface{}) {
	l.Printf("error", format, v...)
	os.Exit(1)
}

func Fatal(format string, v ...interface{}) {
	defaultLogger.Fatal(format, v...)
}

func SetCategoryFilter(categories []string) {
	defaultLogger.SetCategoryFilter(categories)
}

func (l *Logger) SetCategoryFilter(categories []string) {
	mu.Lock()
	defer mu.Unlock()

	if len(categories) == 0 {
		l.categoryFilter = nil
		return
	}

	l.categoryFilter = make(map[string]bool)
	for _, cat := range categories {
		l.categoryFilter[cat] = true
	}
}

func (l *Logger) isCategoryAllowed(category string) bool {
	if l.categoryFilter == nil {
		return true
	}
	return l.categoryFilter[category]
}

func IsCategoryEnabled(category string) bool {
	mu.Lock()
	defer mu.Unlock()
	return defaultLogger.isCategoryAllowed(category)
}

func FormatNumber(n float64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%5.1fb", n/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%5.1fm", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%5.1fk", n/1_000)
	default:
		return fmt.Sprintf("%5.1f ", n)
	}
}

func FormatCount(n int64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func FormatRate(n float64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", n/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", n/1_000)
	}
	return fmt.Sprintf("%.0f", n)
}

func FormatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.1f TB", float64(b)/TB)
	case b >= GB:
		return fmt.Sprintf("%.1f GB", float64(b)/GB)
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/MB)
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/KB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
