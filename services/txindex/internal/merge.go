package internal

import (
	"container/heap"
	"fmt"
	"io"
	"os"
)

type RunReader struct {
	file    *os.File
	buf     []byte
	pos     int
	current WALEntry
	done    bool
}

func NewRunReader(path string) (*RunReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open run file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat run file: %w", err)
	}

	buf := make([]byte, info.Size())
	if _, err := io.ReadFull(f, buf); err != nil {
		f.Close()
		return nil, fmt.Errorf("read run file: %w", err)
	}

	r := &RunReader{
		file: f,
		buf:  buf,
		pos:  0,
	}

	r.advance()
	return r, nil
}

func (r *RunReader) advance() {
	if r.pos >= len(r.buf) {
		r.done = true
		return
	}

	entry, err := DecodeWALEntry(r.buf[r.pos : r.pos+WALEntrySize])
	if err != nil {
		r.done = true
		return
	}

	r.current = *entry
	r.pos += WALEntrySize
}

func (r *RunReader) Current() WALEntry {
	return r.current
}

func (r *RunReader) Done() bool {
	return r.done
}

func (r *RunReader) Close() error {
	return r.file.Close()
}

func (r *RunReader) EntryCount() int {
	return len(r.buf) / WALEntrySize
}

type heapEntry struct {
	entry  WALEntry
	runIdx int
}

type mergeHeap []heapEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	return ComparePrefix5(h[i].entry.Prefix5, h[j].entry.Prefix5) < 0
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(heapEntry))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Merger struct {
	runs         []*RunReader
	heap         mergeHeap
	totalEntries int
}

func NewMerger(runPaths []string) (*Merger, error) {
	m := &Merger{
		runs: make([]*RunReader, 0, len(runPaths)),
		heap: make(mergeHeap, 0, len(runPaths)),
	}

	for i, path := range runPaths {
		r, err := NewRunReader(path)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("open run %d: %w", i, err)
		}
		m.runs = append(m.runs, r)
		m.totalEntries += r.EntryCount()

		if !r.Done() {
			m.heap = append(m.heap, heapEntry{
				entry:  r.Current(),
				runIdx: i,
			})
		}
	}

	heap.Init(&m.heap)
	return m, nil
}

func (m *Merger) Pop() WALEntry {
	if len(m.heap) == 0 {
		return WALEntry{}
	}

	item := heap.Pop(&m.heap).(heapEntry)
	entry := item.entry
	runIdx := item.runIdx

	m.runs[runIdx].advance()
	if !m.runs[runIdx].Done() {
		heap.Push(&m.heap, heapEntry{
			entry:  m.runs[runIdx].Current(),
			runIdx: runIdx,
		})
	}

	return entry
}

func (m *Merger) Len() int {
	return len(m.heap)
}

func (m *Merger) TotalEntries() int {
	return m.totalEntries
}

func (m *Merger) Close() error {
	var firstErr error
	for _, r := range m.runs {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
