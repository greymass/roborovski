package publish

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

type CoreStore interface {
	SyncForCommit() (appendlog.CommitState, error)
	MarkPublished(head uint32)
}

type ABIStore interface {
	SyncBounds() (dataEnd uint64, indexLen uint32, err error)
}

type Step int

const (
	StepABISynced Step = iota + 1
	StepCoreSynced
	StepManifestWritten
)

type Config struct {
	CoreRoot  string
	ABIRoot   string
	SliceSpan uint32
	ChainID   [32]byte // all zero when unknown
	HeadID    func(block []byte) ([32]byte, error)
	Erase     bool // remove all history under both roots after taking the locks
}

// Publisher owns both storage roots for the life of the process and publishes commits through the manifest.
type Publisher struct {
	cfg      Config
	coreLock *coremanifest.WriterLock
	abiLock  *coremanifest.WriterLock
	core     *coremanifest.Binding
	abi      *coremanifest.Binding
	manifest *coremanifest.Manifest
	adopt    bool

	FaultHook func(Step) error
}

func Open(cfg Config) (*Publisher, error) {
	var err error
	if cfg.CoreRoot, err = filepath.Abs(cfg.CoreRoot); err != nil {
		return nil, err
	}
	if cfg.ABIRoot, err = filepath.Abs(cfg.ABIRoot); err != nil {
		return nil, err
	}
	if cfg.CoreRoot == cfg.ABIRoot {
		return nil, fmt.Errorf("core storage and the ABI cache need separate directories (both are %s)", cfg.CoreRoot)
	}
	if cfg.SliceSpan == 0 {
		return nil, errors.New("slice span is zero")
	}
	p := &Publisher{cfg: cfg}
	if p.coreLock, err = coremanifest.AcquireWriterLock(cfg.CoreRoot); err != nil {
		return nil, fmt.Errorf("core storage: %w", err)
	}
	if p.abiLock, err = coremanifest.AcquireWriterLock(cfg.ABIRoot); err != nil {
		p.coreLock.Release()
		return nil, fmt.Errorf("ABI cache: %w", err)
	}
	if err := p.load(); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Publisher) Close() {
	p.abiLock.Release()
	p.coreLock.Release()
}

// ReleaseLocks drops both locks without any other cleanup, as a killed process would.
func (p *Publisher) ReleaseLocks() { p.Close() }

func (p *Publisher) Manifest() coremanifest.Manifest { return *p.manifest }

func (p *Publisher) Binding() coremanifest.Binding { return *p.core }

// ABIBinding is the binding to stamp into a rebuilt ABI cache.
func (p *Publisher) ABIBinding() coremanifest.Binding { return *p.abi }

func (p *Publisher) Adopting() bool { return p.adopt }

func (p *Publisher) load() error {
	if p.cfg.Erase {
		if err := eraseRoot(p.cfg.CoreRoot, p.cfg.ABIRoot); err != nil {
			return err
		}
		if err := eraseRoot(p.cfg.ABIRoot, p.cfg.CoreRoot); err != nil {
			return err
		}
	}
	removeStrayTemps(filepath.Join(p.cfg.CoreRoot, coremanifest.ControlDirName))

	core, coreErr := coremanifest.ReadBinding(p.cfg.CoreRoot)
	abi, abiErr := coremanifest.ReadBinding(p.cfg.ABIRoot)
	for _, e := range []error{coreErr, abiErr} {
		if e != nil && !errors.Is(e, coremanifest.ErrNoBinding) {
			return e
		}
	}
	m, err := coremanifest.ReadManifest(p.cfg.CoreRoot)
	if err != nil && !errors.Is(err, coremanifest.ErrNoManifest) {
		return err
	}
	haveBindings := coreErr == nil && abiErr == nil
	if (coreErr == nil) != (abiErr == nil) {
		return fmt.Errorf("only one of the core (%v) and ABI (%v) roots has a binding; they belong to different stores", coreErr, abiErr)
	}
	if m != nil && !haveBindings {
		return errors.New("a manifest exists but the storage roots have no binding")
	}

	if haveBindings {
		if err := coremanifest.CheckPair(core, abi); err != nil {
			return err
		}
		if core.SliceSpan != p.cfg.SliceSpan {
			return fmt.Errorf("store is bound to %d-block slices, configured span is %d", core.SliceSpan, p.cfg.SliceSpan)
		}
		if p.cfg.ChainID != ([32]byte{}) && core.ChainKnown() && core.ChainID != p.cfg.ChainID {
			return fmt.Errorf("%w: store is bound to chain %x, configured chain is %x", coremanifest.ErrBindingMismatch, core.ChainID, p.cfg.ChainID)
		}
	} else {
		coreUUID, err := coremanifest.NewUUID()
		if err != nil {
			return err
		}
		abiUUID, err := coremanifest.NewUUID()
		if err != nil {
			return err
		}
		core = &coremanifest.Binding{Role: coremanifest.RoleCore, ChainID: p.cfg.ChainID, CoreUUID: coreUUID, ABIUUID: abiUUID, SliceSpan: p.cfg.SliceSpan}
		pair := *core
		pair.Role = coremanifest.RoleABI
		abi = &pair
	}
	if m != nil {
		if err := coremanifest.CheckManifest(core, m); err != nil {
			return err
		}
	}
	p.core, p.abi, p.manifest, p.adopt = core, abi, m, m == nil
	return nil
}

// Prepare adopts a store that has no manifest, and otherwise truncates both roots to the manifest.
func (p *Publisher) Prepare() error {
	if p.adopt {
		return p.adoptStore()
	}
	m := p.manifest
	report, err := appendlog.TruncateToCommitted(p.cfg.CoreRoot, p.cfg.SliceSpan, appendlog.CoreBounds{
		Head: m.Head, SliceDataEnd: m.SliceDataEnd, SliceIndexLen: m.SliceIndexLen,
	})
	if err != nil {
		return fmt.Errorf("recover core storage to committed block %d: %w", m.Head, err)
	}
	if report.Changed() {
		logger.Printf("store", "Recovered to committed block %d: removed %d uncommitted slices, %d data bytes, %d index entries",
			m.Head, len(report.RemovedSlices), report.DataBytesCut, report.IndexEntriesCut)
	}
	cut, err := abicache.TruncateTo(p.cfg.ABIRoot, m.ABIDataEnd, m.ABIIndexLen)
	if err != nil {
		return fmt.Errorf("recover ABI cache to committed block %d: %w", m.Head, err)
	}
	if cut {
		logger.Printf("store", "Recovered ABI cache to %d committed entries", m.ABIIndexLen)
	}
	if p.cfg.ChainID != ([32]byte{}) && !p.core.ChainKnown() {
		return p.recordChainID()
	}
	return nil
}

func (p *Publisher) adoptStore() error {
	a, err := appendlog.FindAdoptionBoundary(p.cfg.CoreRoot, p.cfg.SliceSpan)
	if err != nil {
		return fmt.Errorf("adopt existing store: %w", err)
	}
	if _, err := appendlog.TruncateToCommitted(p.cfg.CoreRoot, p.cfg.SliceSpan, a.CoreBounds); err != nil {
		return fmt.Errorf("adopt existing store at block %d: %w", a.Head, err)
	}
	abiEnd, abiLen, err := abicache.CommittedBounds(p.cfg.ABIRoot, a.Head)
	if err != nil {
		return fmt.Errorf("adopt existing ABI cache: %w", err)
	}
	if _, err := abicache.TruncateTo(p.cfg.ABIRoot, abiEnd, abiLen); err != nil {
		return fmt.Errorf("adopt existing ABI cache: %w", err)
	}
	m := &coremanifest.Manifest{
		CoreUUID: p.core.CoreUUID, ABIUUID: p.core.ABIUUID, Epoch: 1,
		Head: a.Head, SliceDataEnd: a.SliceDataEnd, SliceIndexLen: a.SliceIndexLen,
		ABIIndexLen: abiLen, ABIDataEnd: abiEnd,
	}
	if a.Head > 0 {
		if m.HeadID, err = p.cfg.HeadID(a.HeadData); err != nil {
			return fmt.Errorf("adopt existing store: block ID of block %d: %w", a.Head, err)
		}
	}
	if err := coremanifest.WriteBinding(p.cfg.CoreRoot, p.core); err != nil {
		return err
	}
	if err := coremanifest.WriteBinding(p.cfg.ABIRoot, p.abi); err != nil {
		return err
	}
	if err := coremanifest.WriteManifest(p.cfg.CoreRoot, m); err != nil {
		return err
	}
	p.manifest, p.adopt = m, false
	if a.Head > 0 {
		logger.Printf("store", "Adopted existing store at block %d (store %s)", a.Head, coremanifest.FormatUUID(p.core.CoreUUID))
	} else {
		logger.Printf("store", "Initialized empty store %s", coremanifest.FormatUUID(p.core.CoreUUID))
	}
	return nil
}

func (p *Publisher) recordChainID() error {
	p.core.ChainID = p.cfg.ChainID
	p.abi.ChainID = p.cfg.ChainID
	if err := coremanifest.WriteBinding(p.cfg.CoreRoot, p.core); err != nil {
		return err
	}
	if err := coremanifest.WriteBinding(p.cfg.ABIRoot, p.abi); err != nil {
		return err
	}
	logger.Printf("store", "Recorded chain ID %x in the store binding", p.cfg.ChainID)
	return nil
}

func (p *Publisher) fault(s Step) error {
	if p.FaultHook == nil {
		return nil
	}
	return p.FaultHook(s)
}

// Commit syncs the ABI cache, then core storage, then publishes the manifest; false means nothing was new.
func (p *Publisher) Commit(core CoreStore, abi ABIStore) (bool, error) {
	abiEnd, abiLen, err := abi.SyncBounds()
	if err != nil {
		return false, fmt.Errorf("commit: ABI cache: %w", err)
	}
	if err := p.fault(StepABISynced); err != nil {
		return false, err
	}
	cs, err := core.SyncForCommit()
	if err != nil {
		return false, fmt.Errorf("commit: core storage: %w", err)
	}
	if err := p.fault(StepCoreSynced); err != nil {
		return false, err
	}

	cur := p.manifest
	if cs.Head < cur.Head {
		return false, fmt.Errorf("commit: core head %d is below committed head %d", cs.Head, cur.Head)
	}
	if abiLen < cur.ABIIndexLen || abiEnd < cur.ABIDataEnd {
		return false, fmt.Errorf("commit: ABI cache shrank below its committed extent")
	}
	if cs.Head == cur.Head && cs.SliceDataEnd == cur.SliceDataEnd && abiEnd == cur.ABIDataEnd && abiLen == cur.ABIIndexLen {
		return false, nil
	}
	next := *cur
	next.Head, next.SliceDataEnd, next.SliceIndexLen = cs.Head, cs.SliceDataEnd, cs.SliceIndexLen
	next.ABIDataEnd, next.ABIIndexLen = abiEnd, abiLen
	if cs.Head != cur.Head {
		if cs.HeadData == nil {
			return false, fmt.Errorf("commit: head moved to %d without its block bytes", cs.Head)
		}
		if next.HeadID, err = p.cfg.HeadID(cs.HeadData); err != nil {
			return false, fmt.Errorf("commit: block ID of block %d: %w", cs.Head, err)
		}
	}
	if err := coremanifest.WriteManifest(p.cfg.CoreRoot, &next); err != nil {
		return false, fmt.Errorf("commit: manifest: %w", err)
	}
	p.manifest = &next
	if err := p.fault(StepManifestWritten); err != nil {
		return true, err
	}
	core.MarkPublished(next.Head)
	return true, nil
}

func removeStrayTemps(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "."+coremanifest.ManifestName+".tmp-") {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// eraseRoot removes everything under root except its lock file and the other root when nested inside it.
func eraseRoot(root, keep string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		path := filepath.Join(root, e.Name())
		if e.Name() == coremanifest.LockName || path == keep || strings.HasPrefix(keep, path+string(filepath.Separator)) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return coremanifest.SyncDir(root)
}
