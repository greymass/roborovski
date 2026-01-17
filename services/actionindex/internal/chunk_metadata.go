package internal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

var (
	ErrMetadataCorrupt = errors.New("metadata file corrupt")
)

type ChunkMetadata struct {
	mu sync.RWMutex

	allActions map[uint64][]uint64
}

func NewChunkMetadata() *ChunkMetadata {
	return &ChunkMetadata{
		allActions: make(map[uint64][]uint64),
	}
}

func findChunk(bases []uint64, targetSeq uint64) (chunkID uint32, found bool) {
	if len(bases) == 0 {
		return 0, false
	}
	idx := sort.Search(len(bases), func(i int) bool {
		return bases[i] > targetSeq
	})
	if idx == 0 {
		return 0, true
	}
	return uint32(idx - 1), true
}

func (m *ChunkMetadata) AddAllActionsChunk(account uint64, baseSeq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allActions[account] = append(m.allActions[account], baseSeq)
}

func (m *ChunkMetadata) FindAllActionsChunk(account uint64, targetSeq uint64) (chunkID uint32, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return findChunk(m.allActions[account], targetSeq)
}

func (m *ChunkMetadata) GetAllActionsChunkCount(account uint64) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.allActions[account])
}

func (m *ChunkMetadata) GetAllActionsSeqRange(account uint64) (minBaseSeq, maxBaseSeq uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bases := m.allActions[account]
	if len(bases) == 0 {
		return 0, 0
	}
	return bases[0], bases[len(bases)-1]
}

func (m *ChunkMetadata) GetChunkBaseSeqsNear(account uint64, targetSeq uint64) (prevBase, nextBase uint64, chunkID int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bases := m.allActions[account]
	if len(bases) == 0 {
		return 0, 0, -1
	}
	idx := sort.Search(len(bases), func(i int) bool {
		return bases[i] > targetSeq
	})
	if idx == 0 {
		return 0, bases[0], 0
	}
	if idx >= len(bases) {
		return bases[len(bases)-1], 0, len(bases) - 1
	}
	return bases[idx-1], bases[idx], idx - 1
}

func (m *ChunkMetadata) GetAllActionsChunkCountNoLock(account uint64) int {
	return len(m.allActions[account])
}

func (m *ChunkMetadata) Stats() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.allActions)
}

const metadataFileVersion = 2

func (m *ChunkMetadata) SaveToFile(path string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], metadataFileVersion)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(m.allActions)))
	if _, err := w.Write(header); err != nil {
		f.Close()
		os.Remove(path + ".tmp")
		return err
	}

	buf := make([]byte, 8)
	for account, bases := range m.allActions {
		binary.LittleEndian.PutUint64(buf, account)
		if _, err := w.Write(buf); err != nil {
			f.Close()
			os.Remove(path + ".tmp")
			return err
		}

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(bases)))
		if _, err := w.Write(buf[0:4]); err != nil {
			f.Close()
			os.Remove(path + ".tmp")
			return err
		}

		for _, baseSeq := range bases {
			binary.LittleEndian.PutUint64(buf, baseSeq)
			if _, err := w.Write(buf); err != nil {
				f.Close()
				os.Remove(path + ".tmp")
				return err
			}
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(path + ".tmp")
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path + ".tmp")
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(path + ".tmp")
		return err
	}

	return os.Rename(path+".tmp", path)
}

func LoadChunkMetadataFromFile(path string) (*ChunkMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	version := binary.LittleEndian.Uint32(header[0:4])
	if version != metadataFileVersion {
		return nil, ErrMetadataCorrupt
	}

	allActionsCount := binary.LittleEndian.Uint32(header[4:8])

	m := &ChunkMetadata{
		allActions: make(map[uint64][]uint64, allActionsCount),
	}

	buf := make([]byte, 8)
	for i := uint32(0); i < allActionsCount; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		account := binary.LittleEndian.Uint64(buf)

		if _, err := io.ReadFull(r, buf[0:4]); err != nil {
			return nil, err
		}
		baseCount := binary.LittleEndian.Uint32(buf[0:4])

		bases := make([]uint64, baseCount)
		for j := uint32(0); j < baseCount; j++ {
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			bases[j] = binary.LittleEndian.Uint64(buf)
		}
		m.allActions[account] = bases
	}

	for account, bases := range m.allActions {
		if !sort.SliceIsSorted(bases, func(i, j int) bool {
			return bases[i] < bases[j]
		}) {
			sort.Slice(bases, func(i, j int) bool {
				return bases[i] < bases[j]
			})
			m.allActions[account] = bases
		}
	}

	return m, nil
}

func RebuildChunkMetadataFromDB(db *pebble.DB) (*ChunkMetadata, error) {
	m := NewChunkMetadata()

	if err := m.rebuildAllActions(db); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *ChunkMetadata) rebuildAllActions(db *pebble.DB) error {
	prefix := []byte{PrefixLegacyAccountActions}
	upperBound := []byte{PrefixLegacyAccountActions + 1}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		account, _, ok := parseLegacyAccountActionsKey(iter.Key())
		if !ok {
			continue
		}

		baseSeq, err := DecodeChunkBaseSeq(iter.Value())
		if err != nil {
			continue
		}

		m.allActions[account] = append(m.allActions[account], baseSeq)
	}

	if err := iter.Error(); err != nil {
		return err
	}

	for account, bases := range m.allActions {
		if !sort.SliceIsSorted(bases, func(i, j int) bool {
			return bases[i] < bases[j]
		}) {
			sort.Slice(bases, func(i, j int) bool {
				return bases[i] < bases[j]
			})
			m.allActions[account] = bases
		}
	}

	return nil
}
