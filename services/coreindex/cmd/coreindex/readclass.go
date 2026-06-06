package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/greymass/roborovski/libraries/tracereader"
)

const liveEdgeWindow uint32 = 400

const stallEscalationThreshold = 30

type readErrorClass int

const (
	readFatal readErrorClass = iota
	readTransient
)

func isAvailabilityError(err error) bool {
	return err != nil &&
		(errors.Is(err, tracereader.ErrNotFound) || errors.Is(err, tracereader.ErrIncompleteData))
}

func classifyReadError(err error, failBlock, end uint32) readErrorClass {
	if err != nil && !isAvailabilityError(err) {
		return readFatal
	}
	if failBlock+liveEdgeWindow >= end {
		return readTransient
	}
	return readFatal
}

type transientReadError struct{ block uint32 }

func (e *transientReadError) Error() string {
	return fmt.Sprintf("transient trace read: block %d not yet durable at tip", e.block)
}

type fatalReadError struct {
	block uint32
	cause error
}

func (e *fatalReadError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("unrecoverable trace read at block %d: %v", e.block, e.cause)
	}
	return fmt.Sprintf("missing block %d in finalized history (gap)", e.block)
}

type stallTracker struct {
	block     uint32
	since     time.Time
	escalated bool
}

func (st *stallTracker) onTransient(block uint32, now time.Time, threshold time.Duration) bool {
	if block != st.block {
		st.block = block
		st.since = now
		st.escalated = false
		return false
	}
	if !st.escalated && now.Sub(st.since) >= threshold {
		st.escalated = true
		return true
	}
	return false
}

func (st *stallTracker) reset() { *st = stallTracker{} }

func (st *stallTracker) stalledFor(now time.Time) time.Duration {
	if st.block == 0 {
		return 0
	}
	return now.Sub(st.since)
}

func classifyStrideTail(ps *processedStride, end uint32) error {
	if classifyReadError(ps.readErr, ps.incompleteFrom, end) == readTransient {
		return &transientReadError{block: ps.incompleteFrom}
	}
	return &fatalReadError{block: ps.incompleteFrom, cause: ps.readErr}
}
