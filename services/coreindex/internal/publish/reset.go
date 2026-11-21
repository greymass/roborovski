package publish

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

type ResetStep int

const (
	ResetManifestWritten ResetStep = iota + 1
	ResetSlicesRemoved
	ResetABIIndexReplaced
)

type ResetResult struct {
	Boundary      uint32
	Epoch         uint64
	Resumed       bool // the manifest already named the boundary, so this run finished an earlier reset
	RemovedSlices []string
	ABIEntriesCut uint32
}

// Recover takes both writer locks and runs the truncation every writer runs at start, adopting a store that has no manifest.
func Recover(cfg Config) (m coremanifest.Manifest, adopted bool, err error) {
	p, err := Open(cfg)
	if err != nil {
		return m, false, err
	}
	defer p.Close()
	adopted = p.Adopting()
	if err := p.Prepare(); err != nil {
		return m, adopted, err
	}
	return p.Manifest(), adopted, nil
}

// ResetAfter removes everything above slice boundary n: epoch-bumped manifest, slices, ABI index by rename, ABI data.
func (p *Publisher) ResetAfter(n uint32, hook func(ResetStep) error) (*ResetResult, error) {
	if p.adopt {
		return nil, errors.New("the store has no manifest; run --recover or start a writer once to adopt it before a reset")
	}
	span := p.cfg.SliceSpan
	m := *p.manifest
	switch {
	case n == 0 || n%span != 0:
		return nil, fmt.Errorf("block %d is not the last block of a %d-block slice", n, span)
	case n > m.Head:
		return nil, fmt.Errorf("block %d is above the committed head %d; there is nothing to remove", n, m.Head)
	case m.ABIFloor > 0 && n < m.ABIFloor:
		return nil, fmt.Errorf("block %d is below the ABI cache's first block %d, so no ABI history at the boundary would remain", n, m.ABIFloor)
	}
	step := func(s ResetStep) error {
		if hook == nil {
			return nil
		}
		return hook(s)
	}
	res := &ResetResult{Boundary: n, Epoch: m.Epoch, Resumed: m.Head == n}

	if m.Head != n {
		b, err := appendlog.BoundsAt(p.cfg.CoreRoot, span, n)
		if err != nil {
			return nil, fmt.Errorf("retained boundary: %w", err)
		}
		abiEnd, abiLen, err := abicache.CommittedBounds(p.cfg.ABIRoot, n)
		if err != nil {
			return nil, fmt.Errorf("retained ABI boundary: %w", err)
		}
		if abiLen > m.ABIIndexLen || abiEnd > m.ABIDataEnd {
			return nil, fmt.Errorf("ABI cache through block %d holds %d entries, more than the %d committed", n, abiLen, m.ABIIndexLen)
		}
		next := m
		next.Epoch++
		next.Head, next.SliceDataEnd, next.SliceIndexLen = n, b.SliceDataEnd, b.SliceIndexLen
		next.ABIDataEnd, next.ABIIndexLen = abiEnd, abiLen
		if next.HeadID, err = p.cfg.HeadID(b.HeadData); err != nil {
			return nil, fmt.Errorf("block ID of block %d: %w", n, err)
		}
		if err := coremanifest.WriteManifest(p.cfg.CoreRoot, &next); err != nil {
			return nil, fmt.Errorf("manifest: %w", err)
		}
		p.manifest = &next
		res.Epoch = next.Epoch
		res.ABIEntriesCut = m.ABIIndexLen - abiLen
	}
	if err := step(ResetManifestWritten); err != nil {
		return res, err
	}
	m = *p.manifest

	report, err := appendlog.TruncateToCommitted(p.cfg.CoreRoot, span, appendlog.CoreBounds{
		Head: m.Head, SliceDataEnd: m.SliceDataEnd, SliceIndexLen: m.SliceIndexLen,
	})
	res.RemovedSlices = report.RemovedSlices
	if err != nil {
		return res, fmt.Errorf("remove slices above block %d: %w", n, err)
	}
	if err := step(ResetSlicesRemoved); err != nil {
		return res, err
	}

	// abis.index is memory-mapped by readers, so it is replaced by rename and never truncated in place.
	indexPath := filepath.Join(p.cfg.ABIRoot, "abis.index")
	index, err := os.ReadFile(indexPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return res, err
	}
	keep := int(m.ABIIndexLen) * 20
	if len(index) < keep {
		return res, fmt.Errorf("abis.index holds %d bytes, manifest commits %d", len(index), keep)
	}
	if len(index) > keep {
		if err := coremanifest.WriteFileAtomic(indexPath, index[:keep]); err != nil {
			return res, fmt.Errorf("replace abis.index: %w", err)
		}
	}
	if err := step(ResetABIIndexReplaced); err != nil {
		return res, err
	}
	if _, err := abicache.TruncateTo(p.cfg.ABIRoot, m.ABIDataEnd, m.ABIIndexLen); err != nil {
		return res, fmt.Errorf("truncate abis.dat: %w", err)
	}
	return res, nil
}
