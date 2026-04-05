package internal

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

type mockReader struct {
	corereader.Reader
	callCount  atomic.Int32
	failUntil  int32
	returnData []chain.ActionTrace
}

func (m *mockReader) GetActionsByGlobalSeqs(globalSeqs []uint64) ([]chain.ActionTrace, *corereader.FetchTimings, error) {
	n := m.callCount.Add(1)
	if n <= m.failUntil {
		return nil, nil, fmt.Errorf("glob %d not found in any slice", globalSeqs[0])
	}
	return m.returnData, &corereader.FetchTimings{}, nil
}

func TestFetchActionDataWithRetry_SucceedsOnFirstAttempt(t *testing.T) {
	mock := &mockReader{
		failUntil:  0,
		returnData: []chain.ActionTrace{{Receiver: "test"}},
	}
	p := &AccountHistoryProcessor{
		syncer: &Syncer{reader: mock},
	}

	traces, _, err := p.fetchActionDataWithRetry(100, []uint64{5000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(traces) != 1 || traces[0].Receiver != "test" {
		t.Fatalf("unexpected traces: %v", traces)
	}
	if mock.callCount.Load() != 1 {
		t.Errorf("expected 1 call, got %d", mock.callCount.Load())
	}
}

func TestFetchActionDataWithRetry_SucceedsAfterRetries(t *testing.T) {
	mock := &mockReader{
		failUntil:  3,
		returnData: []chain.ActionTrace{{Receiver: "recovered"}},
	}
	p := &AccountHistoryProcessor{
		syncer: &Syncer{reader: mock},
	}

	start := time.Now()
	traces, _, err := p.fetchActionDataWithRetry(100, []uint64{5000})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(traces) != 1 || traces[0].Receiver != "recovered" {
		t.Fatalf("unexpected traces: %v", traces)
	}
	if mock.callCount.Load() != 4 {
		t.Errorf("expected 4 calls (3 failures + 1 success), got %d", mock.callCount.Load())
	}
	if elapsed < 200*time.Millisecond+400*time.Millisecond+800*time.Millisecond {
		t.Errorf("expected at least 1.4s of backoff delay, completed in %v", elapsed)
	}
	t.Logf("Succeeded on attempt 4 after %v", elapsed)
}

func TestFetchActionDataWithRetry_FailsAfterMaxAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long retry test in short mode")
	}

	mock := &mockReader{
		failUntil: 100,
	}
	p := &AccountHistoryProcessor{
		syncer: &Syncer{reader: mock},
	}

	_, _, err := p.fetchActionDataWithRetry(100, []uint64{5000})
	if err == nil {
		t.Fatal("expected error after max retries, got nil")
	}
	if mock.callCount.Load() != 10 {
		t.Errorf("expected 10 calls, got %d", mock.callCount.Load())
	}
	t.Logf("Failed as expected after %d attempts: %v", mock.callCount.Load(), err)
}
