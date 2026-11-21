package preflight

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

const DirectSliceSpan = 10000

type Target string

const (
	TargetDirect Target = "direct"
	TargetTrace  Target = "trace"
)

type Status string

const (
	Pass    Status = "pass"
	Fail    Status = "fail"
	Unknown Status = "unknown"
)

type Result string

const (
	ResultReady        Result = "ready"
	ResultBlocked      Result = "blocked"
	ResultInconclusive Result = "inconclusive"
)

// SnapshotInfo is the JSON that `spring-util snapshot info` prints.
type SnapshotInfo struct {
	Version       uint32 `json:"version"`
	ChainID       string `json:"chain_id"`
	HeadBlockID   string `json:"head_block_id"`
	HeadBlockNum  uint32 `json:"head_block_num"`
	HeadBlockTime string `json:"head_block_time"`
}

func ReadSnapshotInfo(path string) (*SnapshotInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s SnapshotInfo
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &s, nil
}

type Options struct {
	Target    Target
	CoreRoot  string
	ABIRoot   string
	SliceSpan uint32
	ChainID   [32]byte // all zero when not configured
	HeadID    func(block []byte) ([32]byte, error)

	Snapshot     *SnapshotInfo // direct ingestion's starting snapshot, nil when none was given
	TraceCovers  func(block uint32) (bool, error)
	Attempts     int
	RetryBackoff time.Duration
}

type Check struct {
	Name   string `json:"name"`
	Status Status `json:"status"`
	Detail string `json:"detail"`
}

type Boundary struct {
	Head     uint32 `json:"head"`
	HeadID   string `json:"head_id,omitempty"`
	Epoch    uint64 `json:"epoch,omitempty"`
	Store    string `json:"store,omitempty"`
	ChainID  string `json:"chain_id,omitempty"`
	Adoption bool   `json:"adoption,omitempty"`
}

type Report struct {
	Result        Result    `json:"result"`
	Target        Target    `json:"target"`
	CoreRoot      string    `json:"core_root"`
	ABIRoot       string    `json:"abi_root"`
	Attempts      int       `json:"attempts"`
	WriterActive  bool      `json:"writer_active"`
	Boundary      *Boundary `json:"boundary,omitempty"`
	Checks        []Check   `json:"checks"`
	Blockers      []string  `json:"blockers"`
	Unestablished []string  `json:"unestablished"`
}

func (r *Report) add(name string, s Status, format string, args ...any) {
	r.Checks = append(r.Checks, Check{Name: name, Status: s, Detail: fmt.Sprintf(format, args...)})
}

// Run inspects storage without changing it, and repeats the inspection while the manifest moves underneath it.
func Run(o Options) *Report {
	if o.Attempts <= 0 {
		o.Attempts = 5
	}
	if o.RetryBackoff == 0 {
		o.RetryBackoff = 200 * time.Millisecond
	}
	var r *Report
	for attempt := 1; attempt <= o.Attempts; attempt++ {
		before, _ := os.ReadFile(coremanifest.ManifestPath(o.CoreRoot))
		r = inspect(o)
		after, _ := os.ReadFile(coremanifest.ManifestPath(o.CoreRoot))
		r.Attempts = attempt
		if string(before) == string(after) {
			r.finish()
			return r
		}
		time.Sleep(o.RetryBackoff)
	}
	r.finish()
	r.Result = ResultInconclusive
	r.Unestablished = append(r.Unestablished, fmt.Sprintf("the manifest changed during each of %d inspections", o.Attempts))
	return r
}

func (r *Report) finish() {
	r.Blockers, r.Unestablished = []string{}, []string{}
	for _, c := range r.Checks {
		switch c.Status {
		case Fail:
			r.Blockers = append(r.Blockers, c.Name+": "+c.Detail)
		case Unknown:
			r.Unestablished = append(r.Unestablished, c.Name+": "+c.Detail)
		}
	}
	r.Result = ResultReady
	if len(r.Blockers) > 0 {
		r.Result = ResultBlocked
	}
}

func inspect(o Options) *Report {
	r := &Report{Target: o.Target, CoreRoot: o.CoreRoot, ABIRoot: o.ABIRoot}
	if !checkPaths(r, o) {
		return r
	}
	checkGeometry(r, o)

	core, ok := checkBindings(r, o)
	m, err := coremanifest.ReadManifest(o.CoreRoot)
	switch {
	case errors.Is(err, coremanifest.ErrNoManifest):
		if core != nil {
			r.add("manifest", Fail, "the roots are bound but control/manifest is missing")
			return r
		}
		a, err := appendlog.FindAdoptionBoundary(o.CoreRoot, o.SliceSpan)
		if err != nil {
			r.add("manifest", Fail, "no manifest, and the store cannot be adopted: %v", err)
			return r
		}
		r.add("manifest", Pass, "no manifest; the writer adopts the store at block %d", a.Head)
		r.Boundary = &Boundary{Head: a.Head, Adoption: true}
		if a.Head > 0 {
			if id, err := o.HeadID(a.HeadData); err == nil {
				r.Boundary.HeadID = hex.EncodeToString(id[:])
			}
		}
	case err != nil:
		r.add("manifest", Fail, "%v", err)
		return r
	default:
		if ok {
			if err := coremanifest.CheckManifest(core, m); err != nil {
				r.add("manifest", Fail, "%v", err)
				return r
			}
		}
		r.add("manifest", Pass, "committed head %d, epoch %d", m.Head, m.Epoch)
		r.Boundary = &Boundary{Head: m.Head, HeadID: hex.EncodeToString(m.HeadID[:]), Epoch: m.Epoch, Store: coremanifest.FormatUUID(m.CoreUUID)}
		if core != nil && core.ChainKnown() {
			r.Boundary.ChainID = hex.EncodeToString(core.ChainID[:])
		}
		checkHead(r, o, m)
		checkABIExtent(r, o, m)
	}
	held, err := coremanifest.WriterLockHeld(o.CoreRoot)
	if err != nil {
		r.add("writer", Unknown, "cannot test the core writer lock: %v", err)
	} else {
		r.WriterActive = held
		if held {
			r.add("writer", Pass, "a writer is running; stop it and take ownership before the handoff")
		} else {
			r.add("writer", Pass, "no writer holds the core writer lock")
		}
	}

	checkChain(r, o, core)
	switch o.Target {
	case TargetDirect:
		checkSnapshot(r, o)
	case TargetTrace:
		checkTrace(r, o)
	}
	return r
}

func checkPaths(r *Report, o Options) bool {
	ok := true
	for _, p := range []struct{ name, path string }{{"core path", o.CoreRoot}, {"ABI path", o.ABIRoot}} {
		st, err := os.Stat(p.path)
		switch {
		case err != nil:
			r.add("paths", Fail, "%s %s: %v", p.name, p.path, err)
			ok = false
		case !st.IsDir():
			r.add("paths", Fail, "%s %s is not a directory", p.name, p.path)
			ok = false
		}
	}
	if ok && filepath.Clean(o.CoreRoot) == filepath.Clean(o.ABIRoot) {
		r.add("paths", Fail, "core storage and the ABI cache need separate directories")
		return false
	}
	if ok {
		r.add("paths", Pass, "core %s, ABI %s", o.CoreRoot, o.ABIRoot)
	}
	return ok
}

func checkGeometry(r *Report, o Options) {
	if o.Target == TargetDirect && o.SliceSpan != DirectSliceSpan {
		r.add("geometry", Fail, "direct ingestion requires %d-block slices, configured span is %d", DirectSliceSpan, o.SliceSpan)
		return
	}
	x, err := appendlog.ScanSlices(o.CoreRoot, o.SliceSpan)
	if err != nil {
		r.add("geometry", Fail, "%v", err)
		return
	}
	r.add("geometry", Pass, "%d slice directories of %d blocks", x.Count, o.SliceSpan)
}

func checkBindings(r *Report, o Options) (*coremanifest.Binding, bool) {
	core, coreErr := coremanifest.ReadBinding(o.CoreRoot)
	abi, abiErr := coremanifest.ReadBinding(o.ABIRoot)
	switch {
	case errors.Is(coreErr, coremanifest.ErrNoBinding) && errors.Is(abiErr, coremanifest.ErrNoBinding):
		r.add("binding", Pass, "neither root is bound; the writer binds both when it adopts the store")
		return nil, false
	case coreErr != nil:
		r.add("binding", Fail, "core root: %v", coreErr)
		return nil, false
	case abiErr != nil:
		r.add("binding", Fail, "ABI root: %v", abiErr)
		return core, false
	}
	if err := coremanifest.CheckPair(core, abi); err != nil {
		r.add("binding", Fail, "%v", err)
		return core, false
	}
	if core.SliceSpan != o.SliceSpan {
		r.add("binding", Fail, "store is bound to %d-block slices, configured span is %d", core.SliceSpan, o.SliceSpan)
		return core, false
	}
	r.add("binding", Pass, "store %s, ABI %s", coremanifest.FormatUUID(core.CoreUUID), coremanifest.FormatUUID(core.ABIUUID))
	return core, true
}

func checkHead(r *Report, o Options, m *coremanifest.Manifest) {
	if m.Head == 0 {
		r.add("head", Pass, "empty store")
		return
	}
	b, err := appendlog.BoundsAt(o.CoreRoot, o.SliceSpan, m.Head)
	if err != nil {
		r.add("head", Fail, "%v", err)
		return
	}
	if b.SliceIndexLen != m.SliceIndexLen || b.SliceDataEnd != m.SliceDataEnd {
		r.add("head", Fail, "block %d is index entry %d ending at byte %d; manifest commits %d entries and %d bytes",
			m.Head, b.SliceIndexLen-1, b.SliceDataEnd, m.SliceIndexLen, m.SliceDataEnd)
		return
	}
	id, err := o.HeadID(b.HeadData)
	if err != nil {
		r.add("head", Fail, "block %d: %v", m.Head, err)
		return
	}
	if id != m.HeadID {
		r.add("head", Fail, "stored block %d has ID %x; manifest records %x", m.Head, id, m.HeadID)
		return
	}
	r.add("head", Pass, "stored block %d passes its check and matches the manifest's block ID", m.Head)
}

func checkABIExtent(r *Report, o Options, m *coremanifest.Manifest) {
	sizes := map[string]int64{}
	for _, name := range []string{"abis.dat", "abis.index"} {
		st, err := os.Stat(filepath.Join(o.ABIRoot, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			r.add("abi cache", Fail, "%v", err)
			return
		}
		sizes[name] = st.Size()
	}
	if sizes["abis.dat"] < int64(m.ABIDataEnd) || sizes["abis.index"] < int64(m.ABIIndexLen)*20 {
		r.add("abi cache", Fail, "ABI cache holds %d data and %d index bytes; manifest commits %d and %d",
			sizes["abis.dat"], sizes["abis.index"], m.ABIDataEnd, int64(m.ABIIndexLen)*20)
		return
	}
	floor := "full history"
	if m.ABIFloor > 0 {
		floor = fmt.Sprintf("from block %d", m.ABIFloor)
	}
	r.add("abi cache", Pass, "%d committed entries, %s", m.ABIIndexLen, floor)
}

func checkChain(r *Report, o Options, core *coremanifest.Binding) {
	var stored [32]byte
	if core != nil {
		stored = core.ChainID
	}
	if o.ChainID != ([32]byte{}) && stored != ([32]byte{}) && o.ChainID != stored {
		r.add("chain", Fail, "store is bound to chain %x, configured chain is %x", stored, o.ChainID)
		return
	}
	if o.Snapshot != nil {
		snap, err := coremanifest.ParseChainID(o.Snapshot.ChainID)
		if err != nil {
			r.add("chain", Fail, "snapshot info chain_id: %v", err)
			return
		}
		if stored != ([32]byte{}) && snap != stored {
			r.add("chain", Fail, "snapshot is of chain %x, store is bound to chain %x", snap, stored)
			return
		}
		if o.ChainID != ([32]byte{}) && snap != o.ChainID {
			r.add("chain", Fail, "snapshot is of chain %x, configured chain is %x", snap, o.ChainID)
			return
		}
	}
	if stored == ([32]byte{}) && o.ChainID == ([32]byte{}) && o.Snapshot == nil {
		r.add("chain", Unknown, "the store records no chain ID and none was supplied")
		return
	}
	r.add("chain", Pass, "chain IDs agree")
}

func checkSnapshot(r *Report, o Options) {
	if o.Snapshot == nil {
		r.add("snapshot", Unknown, "no snapshot info given (--snapshot-info); cannot check the snapshot nodeos starts from")
		return
	}
	if r.Boundary == nil {
		return
	}
	s, head := o.Snapshot.HeadBlockNum, r.Boundary.Head
	if s > head {
		r.add("snapshot", Fail, "snapshot is at block %d, above the retained head %d; history %d to %d would be missing", s, head, head+1, s)
		return
	}
	if head == 0 {
		r.add("snapshot", Pass, "empty store; direct ingestion starts after snapshot block %d", s)
		return
	}
	b, err := appendlog.BoundsAt(o.CoreRoot, o.SliceSpan, s)
	if err != nil {
		r.add("snapshot", Unknown, "snapshot block %d is not readable in core storage (%v); its identity is unchecked", s, err)
	} else if id, err := o.HeadID(b.HeadData); err != nil {
		r.add("snapshot", Unknown, "block %d: %v", s, err)
	} else if hex.EncodeToString(id[:]) != o.Snapshot.HeadBlockID {
		r.add("snapshot", Fail, "snapshot block %d has ID %s; core storage holds %x", s, o.Snapshot.HeadBlockID, id)
		return
	} else {
		r.add("snapshot", Pass, "snapshot block %d matches core storage; nodeos verifies and skips %d to %d and appends from %d", s, s+1, head, head+1)
	}
	if s < head {
		r.add("block source", Unknown, "nodeos needs blocks %d to %d from its block log or peers; this is not checked", s+1, head)
	}
}

func checkTrace(r *Report, o Options) {
	if r.Boundary == nil || o.TraceCovers == nil {
		return
	}
	next := r.Boundary.Head + 1
	if r.Boundary.Head == 0 {
		next = 2
	}
	ok, err := o.TraceCovers(next)
	switch {
	case err != nil:
		r.add("trace input", Unknown, "cannot read trace files for block %d: %v", next, err)
	case !ok:
		r.add("trace input", Fail, "trace files do not hold block %d, where trace ingestion resumes", next)
	default:
		r.add("trace input", Pass, "trace files hold block %d, where trace ingestion resumes", next)
	}
}
