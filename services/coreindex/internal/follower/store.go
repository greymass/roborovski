package follower

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

var ErrReadOnly = errors.New("coreindex is following storage it does not write (--read-only)")

type State int32

const (
	NotReady State = iota
	Ready
	Uncertain
)

func (s State) String() string {
	switch s {
	case Ready:
		return "ready"
	case Uncertain:
		return "head_uncertain"
	}
	return "not_ready"
}

// Hooks connect the follower to the serving paths; any may be nil.
type Hooks struct {
	Broadcast    func(blockNum uint32, data []byte)
	EpochChanged func(m *coremanifest.Manifest)
	SetLive      func(live bool)
}

// Store serves core storage another process writes, bounded by that writer's manifest.
type Store struct {
	root  string
	opts  corereader.SliceReaderOptions
	hooks Hooks

	mu     sync.RWMutex
	reader *corereader.SliceReader

	served      atomic.Uint32
	state       atomic.Int32
	manifest    atomic.Pointer[coremanifest.Manifest]
	manifestAt  atomic.Int64
	initialized bool
}

var _ appendlog.StoreInterface = (*Store)(nil)

func NewStore(root string, opts corereader.SliceReaderOptions, hooks Hooks) *Store {
	return &Store{root: root, opts: opts, hooks: hooks}
}

func (s *Store) SetHooks(h Hooks) { s.hooks = h }

func (s *Store) State() State { return State(s.state.Load()) }

// Manifest returns the manifest the served view comes from, or nil before one has been seen.
func (s *Store) Manifest() *coremanifest.Manifest { return s.manifest.Load() }

// ManifestAge is the time since the served manifest was written.
func (s *Store) ManifestAge() time.Duration {
	at := s.manifestAt.Load()
	if at == 0 {
		return 0
	}
	return time.Since(time.Unix(0, at))
}

func (s *Store) currentReader() *corereader.SliceReader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reader
}

// Poll reads the manifest and moves the served view to it. It is called every 100 ms.
func (s *Store) Poll() error {
	r := s.currentReader()
	if r == nil {
		if _, err := coremanifest.ReadManifest(s.root); err != nil {
			s.setState(NotReady)
			if errors.Is(err, coremanifest.ErrNoManifest) {
				return nil
			}
			return err
		}
		opened, err := corereader.NewSliceReaderWithOptions(s.root, s.opts)
		if err != nil {
			s.setState(NotReady)
			if errors.Is(err, corereader.ErrNotReady) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		s.reader = opened
		s.mu.Unlock()
		r = opened
	}

	epochChanged, err := r.RefreshView()
	if err != nil {
		return err
	}
	m := r.Manifest()
	if m == nil || m.Empty() {
		s.setState(NotReady)
		return nil
	}
	head, _, err := r.GetStateProps(true)
	if err != nil {
		if errors.Is(err, corereader.ErrNotReady) {
			s.setState(NotReady)
			return nil
		}
		return err
	}
	if fi, err := os.Stat(coremanifest.ManifestPath(s.root)); err == nil {
		s.manifestAt.Store(fi.ModTime().UnixNano())
	}
	prev := s.manifest.Swap(m)
	old := s.served.Load()
	metricCommittedHead.Set(float64(m.Head))
	metricABIHead.Set(float64(m.Head))

	switch {
	case !s.initialized:
		s.served.Store(head)
		s.initialized = true
		logger.Printf("sync", "Following published head %d (epoch %d)", head, m.Epoch)
	case epochChanged:
		s.served.Store(head)
		logger.Printf("sync", "Epoch %d -> %d: served head %d -> %d; disconnecting stream clients", epochOf(prev), m.Epoch, old, head)
		if s.hooks.EpochChanged != nil {
			s.hooks.EpochChanged(m)
		}
	case head > old:
		s.served.Store(head)
		metricCommitToServe.Observe(s.ManifestAge().Seconds())
		if s.hooks.Broadcast != nil {
			for b := old + 1; b <= head; b++ {
				data, err := r.GetBlockBytes(b)
				if err != nil {
					logger.Printf("warning", "Broadcast of block %d skipped: %v", b, err)
					continue
				}
				s.hooks.Broadcast(b, data)
			}
		}
	case head < old:
		logger.Printf("warning", "Published head fell from %d to %d without an epoch change", old, head)
		s.served.Store(head)
	}
	metricServedHead.Set(float64(s.served.Load()))

	if uncertain, err := HeadUncertain(s.root, m); err != nil {
		logger.Printf("warning", "Head certainty check failed: %v", err)
	} else if uncertain {
		s.setState(Uncertain)
		return nil
	}
	s.setState(Ready)
	return nil
}

func epochOf(m *coremanifest.Manifest) uint64 {
	if m == nil {
		return 0
	}
	return m.Epoch
}

func (s *Store) setState(st State) {
	prev := State(s.state.Swap(int32(st)))
	if prev != st {
		logger.Printf("sync", "Follower state: %s -> %s", prev, st)
	}
	if st == Uncertain {
		metricHeadUncertain.Set(1)
	} else {
		metricHeadUncertain.Set(0)
	}
	if s.hooks.SetLive != nil {
		s.hooks.SetLive(st == Ready)
	}
}

// HeadUncertain applies the rule from the publication protocol: the physical tail extends past the manifest and no writer holds the core lock.
func HeadUncertain(root string, m *coremanifest.Manifest) (bool, error) {
	past, err := tailPastManifest(root, m)
	if err != nil || !past {
		return false, err
	}
	held, err := coremanifest.WriterLockHeld(root)
	if err != nil {
		return false, err
	}
	return !held, nil
}

func tailPastManifest(root string, m *coremanifest.Manifest) (bool, error) {
	if m.Head == 0 {
		return false, nil
	}
	headSlice := (m.Head - 1) / 10000
	sliceDir := func(n uint32) string {
		start := n*10000 + 1
		return filepath.Join(root, fmt.Sprintf("history_%010d-%010d", start, start+10000-1))
	}
	if _, err := os.Stat(sliceDir(headSlice + 1)); err == nil {
		return true, nil
	}
	if fi, err := os.Stat(filepath.Join(sliceDir(headSlice), "data.log")); err == nil && uint64(fi.Size()) > m.SliceDataEnd {
		return true, nil
	}
	f, err := os.Open(filepath.Join(sliceDir(headSlice), "blocks.index"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}
	if (fi.Size()-4)/32 > int64(m.SliceIndexLen) {
		return true, nil
	}
	var count [4]byte
	if _, err := f.ReadAt(count[:], 0); err == nil && binary.LittleEndian.Uint32(count[:]) > m.SliceIndexLen {
		return true, nil
	}
	return false, nil
}

func (s *Store) GetBlock(blockNum uint32) ([]byte, error) {
	r := s.currentReader()
	if r == nil || blockNum == 0 || blockNum > s.served.Load() {
		return nil, appendlog.ErrNotFound
	}
	data, err := r.GetBlockBytes(blockNum)
	if errors.Is(err, corereader.ErrBeyondHead) || errors.Is(err, corereader.ErrNotReady) {
		return nil, appendlog.ErrNotFound
	}
	return data, err
}

func (s *Store) GetBlockByGlob(glob uint64) (uint32, error) {
	r := s.currentReader()
	if r == nil {
		return 0, appendlog.ErrNotFound
	}
	blockNum, err := r.GetBlockByGlob(glob)
	if err != nil || blockNum > s.served.Load() {
		return 0, appendlog.ErrNotFound
	}
	return blockNum, nil
}

func (s *Store) GetLIB() uint32  { return s.served.Load() }
func (s *Store) GetHead() uint32 { return s.served.Load() }

func (s *Store) GetCacheStats() (hits, misses uint64) {
	if r := s.currentReader(); r != nil {
		hits, misses, _, _ = r.GetBlockCacheStats()
	}
	return hits, misses
}

func (s *Store) GetCacheSize() int {
	if r := s.currentReader(); r != nil {
		_, _, _, n := r.GetBlockCacheStats()
		return n
	}
	return 0
}

func (s *Store) AppendBlock(uint32, []byte, uint64, uint64, bool) error { return ErrReadOnly }
func (s *Store) AppendBlockBatch([]appendlog.BlockEntry) error          { return ErrReadOnly }
func (s *Store) BuildGlobIndex() (int, error)                           { return 0, ErrReadOnly }
func (s *Store) Flush() error                                           { return nil }
func (s *Store) Sync() error                                            { return nil }

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	s.reader = nil
	return err
}
