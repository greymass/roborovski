package abicache

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	goeosio "github.com/greymass/go-eosio/pkg/chain"
)

type Reader struct {
	basePath  string
	dataFile  *os.File
	indexFile *os.File
	indexMmap []byte
	indexLen  int
	indexSize int64
	indexIno  uint64
	limit     func() (uint32, bool)
	limitLen  uint32
	limited   bool

	parsedABIs   map[int64]*goeosio.Abi
	parsedABIsMu sync.RWMutex

	mu sync.RWMutex
}

func NewReader(basePath string) (*Reader, error) {
	r := &Reader{
		basePath:   basePath,
		parsedABIs: make(map[int64]*goeosio.Abi),
	}
	if err := r.openFiles(); err != nil {
		return nil, err
	}
	if err := r.loadIndex(); err != nil {
		r.closeFiles()
		return nil, err
	}
	return r, nil
}

// SetLimit bounds the reader to the first n index entries whenever fn reports ok, as a manifest's ABI entry count does.
func (r *Reader) SetLimit(fn func() (n uint32, ok bool)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.limit = fn
	return r.loadIndex()
}

func (r *Reader) openFiles() error {
	dataFile, err := os.OpenFile(filepath.Join(r.basePath, "abis.dat"), os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	indexFile, err := os.OpenFile(filepath.Join(r.basePath, "abis.index"), os.O_RDONLY, 0644)
	if err != nil {
		dataFile.Close()
		return fmt.Errorf("failed to open index file: %w", err)
	}
	st, err := indexFile.Stat()
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return fmt.Errorf("failed to stat index file: %w", err)
	}
	r.dataFile = dataFile
	r.indexFile = indexFile
	r.indexIno = inode(st)
	return nil
}

func (r *Reader) closeFiles() {
	if r.indexMmap != nil {
		syscall.Munmap(r.indexMmap)
		r.indexMmap = nil
	}
	if r.indexFile != nil {
		r.indexFile.Close()
		r.indexFile = nil
	}
	if r.dataFile != nil {
		r.dataFile.Close()
		r.dataFile = nil
	}
}

func inode(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}

func (r *Reader) currentLimit() (uint32, bool) {
	if r.limit == nil {
		return 0, false
	}
	return r.limit()
}

func (r *Reader) loadIndex() error {
	stat, err := r.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file: %w", err)
	}

	indexSize := stat.Size()
	entries := indexSize / indexEntrySize
	lim, limited := r.currentLimit()
	if limited && int64(lim) < entries {
		entries = int64(lim)
	}
	r.indexLen = int(entries)
	r.indexSize = indexSize
	r.limitLen, r.limited = lim, limited

	if r.indexMmap != nil {
		syscall.Munmap(r.indexMmap)
		r.indexMmap = nil
	}

	if entries > 0 {
		indexMmap, err := syscall.Mmap(
			int(r.indexFile.Fd()),
			0,
			int(entries*indexEntrySize),
			syscall.PROT_READ,
			syscall.MAP_SHARED,
		)
		if err != nil {
			return fmt.Errorf("failed to mmap index file: %w", err)
		}
		r.indexMmap = indexMmap
	}

	return nil
}

func (r *Reader) checkIndexGrowth() error {
	if st, err := os.Stat(filepath.Join(r.basePath, "abis.index")); err == nil && inode(st) != r.indexIno {
		return r.reopen()
	}
	stat, err := r.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file: %w", err)
	}
	if stat.Size() < r.indexSize {
		return r.reopen()
	}
	lim, limited := r.currentLimit()
	if stat.Size() > r.indexSize || lim != r.limitLen || limited != r.limited {
		return r.loadIndex()
	}
	return nil
}

// reopen follows a replaced or shortened index; parsed ABIs are keyed by data offset, which a reset reuses.
func (r *Reader) reopen() error {
	r.closeFiles()
	r.parsedABIsMu.Lock()
	r.parsedABIs = make(map[int64]*goeosio.Abi)
	r.parsedABIsMu.Unlock()
	if err := r.openFiles(); err != nil {
		return err
	}
	return r.loadIndex()
}

func (r *Reader) Get(contract uint64, atBlock uint32) ([]byte, error) {
	data, _, err := r.getWithOffset(contract, atBlock)
	return data, err
}

func (r *Reader) getWithOffset(contract uint64, atBlock uint32) ([]byte, int64, error) {
	r.mu.Lock()
	if err := r.checkIndexGrowth(); err != nil {
		r.mu.Unlock()
		return nil, -1, fmt.Errorf("failed to check index growth: %w", err)
	}

	if r.indexLen == 0 {
		r.mu.Unlock()
		return nil, -1, fmt.Errorf("no ABI found for contract %d at block %d", contract, atBlock)
	}

	left, right := 0, r.indexLen
	for left < right {
		mid := left + (right-left)/2
		midOffset := mid * 20
		midBlock := binary.LittleEndian.Uint32(r.indexMmap[midOffset : midOffset+4])

		if midBlock <= atBlock {
			left = mid + 1
		} else {
			right = mid
		}
	}

	dataOffset := int64(-1)
	for i := left - 1; i >= 0; i-- {
		entryOffset := i * 20
		entryContract := binary.LittleEndian.Uint64(r.indexMmap[entryOffset+4 : entryOffset+12])

		if entryContract == contract {
			dataOffset = int64(binary.LittleEndian.Uint64(r.indexMmap[entryOffset+12 : entryOffset+20]))
			break
		}
	}
	r.mu.Unlock()

	if dataOffset < 0 {
		return nil, -1, fmt.Errorf("no ABI found for contract %d at block %d", contract, atBlock)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	header := make([]byte, 16)
	n, err := r.dataFile.ReadAt(header, dataOffset)
	if err != nil || n < 16 {
		return nil, -1, fmt.Errorf("failed to read ABI header: %w", err)
	}

	abiLen := binary.LittleEndian.Uint32(header[12:16])

	if abiLen == 0 {
		return nil, -1, fmt.Errorf("no ABI found for contract %d at block %d (ABI was cleared)", contract, atBlock)
	}

	abiData := make([]byte, abiLen)
	n, err = r.dataFile.ReadAt(abiData, dataOffset+16)
	if err != nil || n < int(abiLen) {
		return nil, -1, fmt.Errorf("failed to read ABI data: %w", err)
	}

	return abiData, dataOffset, nil
}

func (r *Reader) Decode(contract, action uint64, data []byte, atBlock uint32) (map[string]interface{}, error) {
	abiBytes, offset, err := r.getWithOffset(contract, atBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to get ABI: %w", err)
	}

	r.parsedABIsMu.RLock()
	cachedABI, found := r.parsedABIs[offset]
	r.parsedABIsMu.RUnlock()

	if !found {
		var abi goeosio.Abi
		if err := json.Unmarshal(abiBytes, &abi); err != nil {
			return nil, fmt.Errorf("failed to parse ABI: %w", err)
		}

		r.parsedABIsMu.Lock()
		r.parsedABIs[offset] = &abi
		r.parsedABIsMu.Unlock()

		cachedABI = &abi
	}

	reader := bytes.NewReader(data)
	decoded, err := cachedABI.DecodeAction(reader, goeosio.Name(action))
	if err != nil {
		return nil, fmt.Errorf("failed to decode action: %w", err)
	}

	decodedMap, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded action is not a map")
	}

	return decodedMap, nil
}

func (r *Reader) DecodeHex(contract, action uint64, hexData string, atBlock uint32) (map[string]interface{}, error) {
	data, err := hex.DecodeString(hexData)
	if err != nil {
		return nil, fmt.Errorf("invalid hex data: %w", err)
	}
	return r.Decode(contract, action, data, atBlock)
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error

	if r.indexMmap != nil {
		if err := syscall.Munmap(r.indexMmap); err != nil {
			errs = append(errs, fmt.Errorf("munmap index: %w", err))
		}
	}

	if r.indexFile != nil {
		if err := r.indexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close index: %w", err))
		}
	}

	if r.dataFile != nil {
		if err := r.dataFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close data: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
