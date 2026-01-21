package corereader

import (
	"fmt"
	"strings"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type RetryConfig struct {
	Timeout  time.Duration
	MaxDelay time.Duration
}

func RetryWithBackoff(cfg RetryConfig, name string, fn func() error) error {
	deadline := time.Now().Add(cfg.Timeout)
	attempt := 0

	for time.Now().Before(deadline) {
		err := fn()
		if err == nil {
			return nil
		}

		if !IsRetriableError(err) {
			return err
		}

		attempt++
		remaining := time.Until(deadline)

		if remaining > 0 {
			delay := time.Duration(1<<uint(min(attempt-1, 5))) * time.Second
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
			if delay > remaining {
				delay = remaining
			}

			logger.Printf("config", "%s connection attempt %d failed: %v. Retrying in %v... (%v remaining)",
				name, attempt, err, delay, remaining.Round(time.Second))
			time.Sleep(delay)
		}
	}

	return fmt.Errorf("failed to connect after %v timeout", cfg.Timeout)
}

func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	retriable := []string{
		"connection refused",
		"connection reset",
		"no route to host",
		"network is unreachable",
		"timeout",
		"no such host",
		"i/o timeout",
		"block index too small",
		"block not found",
		"not found in slice",
	}

	for _, pattern := range retriable {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}
