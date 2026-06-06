package main

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/tracereader"
)

func TestClassifyReadError(t *testing.T) {
	const end = 1000
	cases := []struct {
		name      string
		err       error
		failBlock uint32
		want      readErrorClass
	}{
		{"short stride at tip", nil, 1000, readTransient},
		{"short stride just inside window", nil, end - liveEdgeWindow, readTransient},
		{"short stride deep in history", nil, 10, readFatal},
		{"availability err at tip", tracereader.ErrNotFound, 999, readTransient},
		{"incomplete-data err at tip", fmt.Errorf("x: %w", tracereader.ErrIncompleteData), 999, readTransient},
		{"availability err deep", tracereader.ErrNotFound, 5, readFatal},
		{"os error at tip is fatal", io.ErrClosedPipe, 1000, readFatal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyReadError(c.err, c.failBlock, end); got != c.want {
				t.Fatalf("classifyReadError = %v, want %v", got, c.want)
			}
		})
	}
}

func TestStallTracker(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	var st stallTracker
	thr := 30 * time.Second

	if st.onTransient(900, base, thr) {
		t.Fatal("should not escalate on first sighting")
	}
	if st.onTransient(900, base.Add(10*time.Second), thr) {
		t.Fatal("should not escalate before threshold")
	}
	if !st.onTransient(900, base.Add(31*time.Second), thr) {
		t.Fatal("should escalate after threshold")
	}
	if st.onTransient(901, base.Add(40*time.Second), thr) {
		t.Fatal("advancing to a new block should reset the stall timer")
	}
	st.reset()
	if st.onTransient(901, base.Add(100*time.Second), thr) {
		t.Fatal("after reset, first sighting should not escalate")
	}
}
