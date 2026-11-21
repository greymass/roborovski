package corereader

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/logger"
)

var (
	// ErrNotReady means the store has no published view yet: no manifest, or a manifest with an empty head.
	ErrNotReady = errors.New("not ready: no published view")
	// ErrBeyondHead means a block above the published head was requested.
	ErrBeyondHead = errors.New("beyond published head")
)

type publishedView struct {
	m     *coremanifest.Manifest
	ino   uint64
	mtime int64
	size  int64
}

func sliceNumForBlock(blockNum uint32) uint32 {
	if blockNum == 0 {
		return 0
	}
	return (blockNum - 1) / 10000
}

func slicePathFor(basePath string, sliceNum uint32) string {
	start := sliceNum*10000 + 1
	return filepath.Join(basePath, fmt.Sprintf("history_%010d-%010d", start, start+10000-1))
}

// Manifest returns the published view this reader honours, or nil for a store written before the manifest protocol.
func (sr *SliceReader) Manifest() *coremanifest.Manifest {
	if v := sr.view.Load(); v != nil {
		return v.m
	}
	return nil
}

func (sr *SliceReader) headLimit() (uint32, bool) {
	if v := sr.view.Load(); v != nil {
		return v.m.Head, true
	}
	return 0, sr.manifestSeen.Load()
}

func (sr *SliceReader) checkBlockBound(blockNum uint32) error {
	lim, ok := sr.headLimit()
	if !ok {
		return nil
	}
	if lim == 0 {
		return ErrNotReady
	}
	if blockNum > lim {
		return fmt.Errorf("block %d: %w %d", blockNum, ErrBeyondHead, lim)
	}
	return nil
}

// RefreshView re-reads the manifest when its inode, mtime or size changed, and reports an epoch change.
func (sr *SliceReader) RefreshView() (epochChanged bool, err error) {
	sr.viewMu.Lock()
	defer sr.viewMu.Unlock()

	path := coremanifest.ManifestPath(sr.basePath)
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if sr.view.Load() != nil {
			logger.Printf("warning", "Manifest %s disappeared; reader is not ready until it returns", path)
			sr.view.Store(nil)
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ino, mtime := fileIdentity(fi)
	old := sr.view.Load()
	if old != nil && old.ino == ino && old.mtime == mtime && old.size == fi.Size() {
		return false, nil
	}
	m, err := coremanifest.ReadManifest(sr.basePath)
	if err != nil {
		if errors.Is(err, coremanifest.ErrNoManifest) {
			return false, nil
		}
		return false, err
	}
	next := &publishedView{m: m, ino: ino, mtime: mtime, size: fi.Size()}
	sr.manifestSeen.Store(true)
	if old != nil && old.m.Epoch != m.Epoch {
		epochChanged = true
		logger.Printf("sync", "Manifest epoch %d -> %d: head %d -> %d, dropping state above the new head",
			old.m.Epoch, m.Epoch, old.m.Head, m.Head)
		sr.view.Store(next)
		sr.dropAbove(m.Head)
	} else {
		sr.view.Store(next)
	}
	sr.applyViewToMetadata(m)
	return epochChanged, nil
}

func fileIdentity(fi os.FileInfo) (uint64, int64) {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino), fi.ModTime().UnixNano()
	}
	return 0, fi.ModTime().UnixNano()
}

// applyViewToMetadata drops slices above the head slice and sets the head slice's end to the head.
func (sr *SliceReader) applyViewToMetadata(m *coremanifest.Manifest) {
	if m.Head == 0 {
		sr.sharedMetadata.truncateFrom(0)
		sr.rebuildSliceMap()
		return
	}
	headSlice := sliceNumForBlock(m.Head)
	if sr.sharedMetadata.truncateFrom(headSlice + 1) {
		sr.rebuildSliceMap()
	}
	idx, ok := sr.sharedMetadata.getSliceIndex(headSlice)
	if !ok {
		return
	}
	info := sr.sharedMetadata.getSlice(idx)
	if info.EndBlock == m.Head && (info.GlobMax != 0 || info.GlobMin != 0) {
		return
	}
	end, gmin, gmax, err := readIndexBounds(filepath.Join(slicePathFor(sr.basePath, headSlice), "blocks.index"), int(m.SliceIndexLen))
	if err != nil {
		logger.Printf("warning", "Head slice %d index unreadable under manifest head %d: %v", headSlice, m.Head, err)
		return
	}
	info.EndBlock = end
	info.GlobMin = gmin
	info.GlobMax = gmax
	info.Finalized = m.Head >= info.MaxBlock && info.Finalized
	sr.sharedMetadata.updateSlice(idx, info)
	if sr.blockIndexCache != nil {
		_ = sr.blockIndexCache.LoadSliceFromBlocksIndexBounded(slicePathFor(sr.basePath, headSlice), headSlice, info.StartBlock, false, m.Head)
	}
	sr.lastAccessedSliceIdx.Store(-1)
}

func (sr *SliceReader) rebuildSliceMap() {
	sr.sliceMapMu.Lock()
	sr.rebuildSliceMapByBlockLocked()
	sr.sliceMapMu.Unlock()
	sr.lastAccessedSliceIdx.Store(-1)
}

func (sr *SliceReader) dropAbove(head uint32) {
	from := sliceNumForBlock(head)
	if head == 0 {
		from = 0
	}

	sr.sharedMetadata.truncateFrom(from + 1)
	if idx, ok := sr.sharedMetadata.getSliceIndex(from); ok && head > 0 {
		info := sr.sharedMetadata.getSlice(idx)
		info.Finalized = false
		info.EndBlock = head
		sr.sharedMetadata.updateSlice(idx, info)
	} else if head == 0 {
		sr.sharedMetadata.truncateFrom(0)
	}
	sr.rebuildSliceMap()

	sr.sliceReadersMu.Lock()
	for num, r := range sr.sliceReaders {
		if num < from {
			continue
		}
		delete(sr.sliceReaders, num)
		r.retired.Store(true)
		if r.refCount.Load() == 0 {
			r.close()
		}
	}
	kept := sr.cacheOrder[:0]
	for _, num := range sr.cacheOrder {
		if num < from {
			kept = append(kept, num)
		}
	}
	sr.cacheOrder = kept
	sr.sliceReadersMu.Unlock()

	sr.pinnedSlicesMu.Lock()
	for num, r := range sr.pinnedSlices {
		if num < from {
			continue
		}
		delete(sr.pinnedSlices, num)
		r.retired.Store(true)
		if r.refCount.Load() == 0 {
			r.close()
		}
	}
	sr.pinnedSlicesMu.Unlock()

	if sr.blockCache != nil {
		sr.blockCache.DropAbove(head)
	}
	if sr.bufferPool != nil {
		sr.bufferPool.DropFrom(from)
	}
	if sr.globRangeIndex != nil {
		sr.globRangeIndex.dropFrom(from)
	}
	if sr.blockIndexCache != nil {
		sr.blockIndexCache.DropFrom(from)
		if v := sr.view.Load(); v != nil {
			sr.blockIndexCache.SetEpoch(v.m.Epoch)
		}
	}
}

func readIndexBounds(indexPath string, maxEntries int) (uint32, uint64, uint64, error) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(data) < 4 {
		return 0, 0, 0, fmt.Errorf("index file too small")
	}
	const entrySize = 32
	count := int(binary.LittleEndian.Uint32(data[0:4]))
	if physical := (len(data) - 4) / entrySize; physical < count {
		count = physical
	}
	if maxEntries > 0 && count > maxEntries {
		count = maxEntries
	}
	if count == 0 {
		return 0, 0, 0, fmt.Errorf("index is empty")
	}
	last := data[4+(count-1)*entrySize : 4+count*entrySize]
	return binary.LittleEndian.Uint32(last[0:4]),
		binary.LittleEndian.Uint64(data[4+16 : 4+24]),
		binary.LittleEndian.Uint64(last[24:32]), nil
}

// boundedLastBlock is findLastBlockInIndex limited to the published view.
func (sr *SliceReader) boundedLastBlock(sliceNum uint32, indexPath string) (uint32, uint64, uint64, error) {
	v := sr.view.Load()
	if v == nil {
		if sr.manifestSeen.Load() {
			return 0, 0, 0, ErrNotReady
		}
		return findLastBlockInIndex(indexPath)
	}
	headSlice := sliceNumForBlock(v.m.Head)
	switch {
	case v.m.Head == 0 || sliceNum > headSlice:
		return 0, 0, 0, fmt.Errorf("slice %d: %w %d", sliceNum, ErrBeyondHead, v.m.Head)
	case sliceNum == headSlice:
		return readIndexBounds(indexPath, int(v.m.SliceIndexLen))
	}
	return findLastBlockInIndex(indexPath)
}

func (sr *SliceReader) indexLimit() func() (uint32, bool) {
	return sr.headLimit
}

func boundIndex(idx map[uint32]blockIndexEntry, limit func() (uint32, bool)) map[uint32]blockIndexEntry {
	if limit == nil {
		return idx
	}
	lim, ok := limit()
	if !ok {
		return idx
	}
	for bn := range idx {
		if bn > lim {
			delete(idx, bn)
		}
	}
	return idx
}

func (sm *SharedSliceMetadata) truncateFrom(sliceNum uint32) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	n := len(sm.slices)
	for n > 0 && sm.slices[n-1].SliceNum >= sliceNum {
		delete(sm.sliceIndex, sm.slices[n-1].SliceNum)
		n--
	}
	if n == len(sm.slices) {
		return false
	}
	sm.slices = sm.slices[:n]
	return true
}

func (c *BlockCache) DropAbove(head uint32) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := c.order[:0]
	for _, bn := range c.order {
		if bn > head {
			if blob, ok := c.cache[bn]; ok {
				c.curBytes -= estimateBlobSize(blob)
				delete(c.cache, bn)
			}
			continue
		}
		kept = append(kept, bn)
	}
	c.order = kept
	if c.curBytes < 0 {
		c.curBytes = 0
	}
}

func (g *GlobRangeIndex) dropFrom(sliceNum uint32) {
	g.updateMu.Lock()
	defer g.updateMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	n := len(g.ranges)
	for n > 0 && g.ranges[n-1].SliceNum >= sliceNum {
		n--
	}
	g.ranges = g.ranges[:n]
	g.totalSlices = n
	var highest uint32
	if n > 0 {
		highest = g.ranges[n-1].SliceNum
	}
	if g.highestIndexedSlice.Load() > highest {
		g.highestIndexedSlice.Store(highest)
	}
}

func (sr *SliceReader) discoverSlicesThrough(head uint32) {
	headSlice := sliceNumForBlock(head)
	var next uint32
	if n := sr.sharedMetadata.getSliceCount(); n > 0 {
		next = sr.sharedMetadata.getSlice(n-1).SliceNum + 1
	} else if scanned, err := scanSlicesFromFilesystem(sr.basePath); err == nil {
		for _, s := range scanned {
			if s.SliceNum < headSlice {
				sr.sharedMetadata.addSlice(s)
			}
		}
		if n := sr.sharedMetadata.getSliceCount(); n > 0 {
			next = sr.sharedMetadata.getSlice(n-1).SliceNum + 1
		} else {
			next = headSlice
		}
	}
	added := false
	for ; next <= headSlice; next++ {
		path := slicePathFor(sr.basePath, next)
		if _, err := os.Stat(path); err != nil {
			break
		}
		end, gmin, gmax, err := sr.boundedLastBlock(next, filepath.Join(path, "blocks.index"))
		if err != nil {
			logger.Printf("debug", "Slice %d exists but its index is not readable: %v", next, err)
			break
		}
		if n := sr.sharedMetadata.getSliceCount(); n > 0 {
			prev := sr.sharedMetadata.getSlice(n - 1)
			if !prev.Finalized {
				prev.Finalized = true
				sr.sharedMetadata.updateSlice(n-1, prev)
				if sr.blockIndexCache != nil {
					sr.blockIndexCache.FinalizeSlice(prev.SliceNum)
				}
			}
		}
		start := next*10000 + 1
		sr.sharedMetadata.addSlice(SliceInfo{
			SliceNum:       next,
			StartBlock:     start,
			EndBlock:       end,
			MaxBlock:       start + 10000 - 1,
			BlocksPerSlice: 10000,
			GlobMin:        gmin,
			GlobMax:        gmax,
		})
		if sr.blockIndexCache != nil {
			_ = sr.loadBlockIndexCacheSlice(path, next, start, false)
		}
		added = true
	}
	if added {
		sr.rebuildSliceMap()
	}
	if v := sr.view.Load(); v != nil {
		sr.applyViewToMetadata(v.m)
	}
}

func (sr *SliceReader) loadBlockIndexCacheSlice(slicePath string, sliceNum, startBlock uint32, finalized bool) error {
	lim, ok := sr.headLimit()
	if !ok {
		lim = ^uint32(0)
	}
	return sr.blockIndexCache.LoadSliceFromBlocksIndexBounded(slicePath, sliceNum, startBlock, finalized, lim)
}

func isEmptyStore(basePath string) bool {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) > 8 && e.Name()[:8] == "history_" {
			return false
		}
	}
	return true
}

// GetBlockBytes returns the decompressed record for blockNum, within the published view.
func (sr *SliceReader) GetBlockBytes(blockNum uint32) ([]byte, error) {
	info, err := sr.findSliceForBlock(blockNum)
	if err != nil {
		return nil, err
	}
	reader, err := sr.getSliceReader(info.SliceNum)
	if err != nil {
		return nil, err
	}
	defer sr.releaseSliceReader(reader)
	return reader.readBlockData(blockNum)
}

// GetBlockByGlob returns the block holding global sequence glob, within the published view.
func (sr *SliceReader) GetBlockByGlob(glob uint64) (uint32, error) {
	info, err := sr.findSliceForGlob(glob)
	if err != nil {
		return 0, err
	}
	reader, err := sr.getSliceReader(info.SliceNum)
	if err != nil {
		return 0, err
	}
	defer sr.releaseSliceReader(reader)
	blockNum, err := reader.binarySearchGlob(glob)
	if err != nil {
		return 0, err
	}
	if err := sr.checkBlockBound(blockNum); err != nil {
		return 0, err
	}
	return blockNum, nil
}
