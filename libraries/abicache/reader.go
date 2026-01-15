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
	"github.com/greymass/roborovski/libraries/chain"
)

type Reader struct {
	dataFile  *os.File
	indexFile *os.File
	indexMmap []byte
	indexLen  int
	indexSize int64

	parsedABIs   map[int64]*goeosio.Abi
	parsedABIsMu sync.RWMutex

	mu sync.RWMutex
}

func NewReader(basePath string) (*Reader, error) {
	r := &Reader{
		parsedABIs: make(map[int64]*goeosio.Abi),
	}

	dataPath := filepath.Join(basePath, "abis.dat")
	dataFile, err := os.OpenFile(dataPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	r.dataFile = dataFile

	indexPath := filepath.Join(basePath, "abis.index")
	indexFile, err := os.OpenFile(indexPath, os.O_RDONLY, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	r.indexFile = indexFile

	if err := r.loadIndex(); err != nil {
		indexFile.Close()
		dataFile.Close()
		return nil, err
	}

	return r, nil
}

func (r *Reader) loadIndex() error {
	stat, err := r.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file: %w", err)
	}

	indexSize := stat.Size()
	if indexSize%20 != 0 {
		return fmt.Errorf("index file corrupted (size not multiple of 20)")
	}
	r.indexLen = int(indexSize / 20)
	r.indexSize = indexSize

	if r.indexMmap != nil {
		syscall.Munmap(r.indexMmap)
		r.indexMmap = nil
	}

	if indexSize > 0 {
		indexMmap, err := syscall.Mmap(
			int(r.indexFile.Fd()),
			0,
			int(indexSize),
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
	stat, err := r.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file: %w", err)
	}

	if stat.Size() > r.indexSize {
		return r.loadIndex()
	}

	return nil
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

	actionName := chain.NameToString(action)
	reader := bytes.NewReader(data)
	decoded, err := cachedABI.Decode(reader, actionName)
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
