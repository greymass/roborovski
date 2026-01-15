package logger

import "strings"

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarning
	LevelError
	LevelFatal
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarning:
		return "WARNING"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

func levelForCategory(category string) Level {
	switch category {
	case "error":
		return LevelError
	case "warning":
		return LevelWarning
	default:
		if strings.HasPrefix(category, "debug") {
			return LevelDebug
		}
		return LevelInfo
	}
}
