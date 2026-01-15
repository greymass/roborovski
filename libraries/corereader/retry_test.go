package corereader

import (
	"errors"
	"testing"
	"time"
)

func TestIsRetriableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"connection refused", errors.New("dial tcp: connection refused"), true},
		{"connection reset", errors.New("connection reset by peer"), true},
		{"timeout", errors.New("i/o timeout"), true},
		{"no route", errors.New("no route to host"), true},
		{"network unreachable", errors.New("network is unreachable"), true},
		{"no such host", errors.New("no such host"), true},
		{"generic timeout", errors.New("timeout exceeded"), true},
		{"block not found", errors.New("block not found"), true},
		{"not found in slice", errors.New("not found in slice"), true},
		{"block index too small", errors.New("block index too small"), true},
		{"invalid request", errors.New("invalid request"), false},
		{"random error", errors.New("something went wrong"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetriableError(tt.err)
			if result != tt.expected {
				t.Errorf("IsRetriableError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestRetryWithBackoff_ImmediateSuccess(t *testing.T) {
	cfg := RetryConfig{Timeout: 5 * time.Second, MaxDelay: 1 * time.Second}
	calls := 0

	err := RetryWithBackoff(cfg, "test", func() error {
		calls++
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestRetryWithBackoff_EventualSuccess(t *testing.T) {
	cfg := RetryConfig{Timeout: 10 * time.Second, MaxDelay: 100 * time.Millisecond}
	calls := 0

	err := RetryWithBackoff(cfg, "test", func() error {
		calls++
		if calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetryWithBackoff_NonRetriableError(t *testing.T) {
	cfg := RetryConfig{Timeout: 5 * time.Second, MaxDelay: 100 * time.Millisecond}
	calls := 0

	err := RetryWithBackoff(cfg, "test", func() error {
		calls++
		return errors.New("invalid request")
	})

	if err == nil {
		t.Error("expected error, got nil")
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry for non-retriable), got %d", calls)
	}
}

func TestRetryWithBackoff_Timeout(t *testing.T) {
	cfg := RetryConfig{Timeout: 500 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	calls := 0

	start := time.Now()
	err := RetryWithBackoff(cfg, "test", func() error {
		calls++
		return errors.New("connection refused")
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected timeout error, got nil")
	}
	if elapsed < 400*time.Millisecond {
		t.Errorf("expected to retry until timeout, elapsed: %v", elapsed)
	}
	if calls < 2 {
		t.Errorf("expected multiple calls before timeout, got %d", calls)
	}
}
