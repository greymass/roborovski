package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/corestream"
)

const (
	PathLocal  = "local"
	PathHTTP   = "http"
	PathABI    = "abi"
	PathStream = "stream"

	ClassSteady       = "steady"
	ClassBehind       = "behind"
	ClassAfterRestart = "after_restart"

	StatusOnTime  = "on_time"
	StatusLate    = "late"
	StatusMissing = "missing"
	StatusNoT0    = "no_writer_record"

	targetFraction = 0.99
)

var probePaths = []string{PathLocal, PathHTTP, PathABI, PathStream}

type ProbeConfig struct {
	WriterURL     string
	NodeosURL     string
	DataPath      string
	CoreindexURL  string
	StreamAddress string
	RecordsPath   string
	Duration      time.Duration
	PollInterval  time.Duration
	LateAfter     time.Duration
	MissingAfter  time.Duration
	BehindAfter   time.Duration
	RestartGrace  time.Duration
	OutputJSON    bool
}

type ProbeRecord struct {
	Block         uint32  `json:"block"`
	Path          string  `json:"path"`
	Class         string  `json:"class"`
	Status        string  `json:"status"`
	NotifiedAtUs  int64   `json:"notified_at_us,omitempty"`
	CommittedAtUs int64   `json:"committed_at_us,omitempty"`
	ObservedAtUs  int64   `json:"observed_at_us,omitempty"`
	LatencyMs     float64 `json:"latency_ms,omitempty"`
	Actions       int     `json:"actions"`
	Transactions  int     `json:"transactions"`
}

type ClassStats struct {
	Count        int     `json:"count"`
	Missing      int     `json:"missing"`
	Over1s       int     `json:"over_1s"`
	Over5s       int     `json:"over_5s"`
	Over30s      int     `json:"over_30s"`
	WithinTarget float64 `json:"within_target"`
	P50Ms        float64 `json:"p50_ms"`
	P90Ms        float64 `json:"p90_ms"`
	P99Ms        float64 `json:"p99_ms"`
	P999Ms       float64 `json:"p999_ms"`
	MaxMs        float64 `json:"max_ms"`
	latencies    []float64
}

type HourStats struct {
	Hour      int        `json:"hour"`
	StartedAt string     `json:"started_at"`
	Stats     ClassStats `json:"stats"`
}

type PathReport struct {
	Classes    map[string]*ClassStats `json:"classes"`
	WorstHour  *HourStats             `json:"worst_hour,omitempty"`
	PollErrors int                    `json:"poll_errors"`
	Late       []ProbeRecord          `json:"late"`
	hours      map[int]*ClassStats
}

type ProbeReport struct {
	Status          string                 `json:"status"`
	StartedAt       string                 `json:"started_at"`
	EndedAt         string                 `json:"ended_at"`
	DurationSeconds float64                `json:"duration_seconds"`
	StartBlock      uint32                 `json:"start_block"`
	EndBlock        uint32                 `json:"end_block"`
	Blocks          int                    `json:"blocks"`
	Unresolved      int                    `json:"unresolved"`
	Actions         int                    `json:"actions"`
	Transactions    int                    `json:"transactions"`
	SetabiBlocks    int                    `json:"setabi_blocks"`
	Restarts        []string               `json:"restarts"`
	Paths           map[string]*PathReport `json:"paths"`
	Failures        []string               `json:"failures"`
}

type probeBlock struct {
	num          uint32
	notifiedUs   int64
	committedUs  int64
	firstKnownUs int64
	decoded      bool
	actions      int
	trxs         int
	abiExpect    map[uint64][]byte
	abiSeen      map[uint64]bool
	seen         map[string]int64
}

type lagSample struct {
	atUs  int64
	lagUs int64
}

type endpointState struct {
	everUp    bool
	downSince int64
}

type restartWindow struct {
	fromUs int64
	toUs   int64
}

type LiveProbe struct {
	cfg    ProbeConfig
	client *http.Client
	paths  []string

	mu         sync.Mutex
	start      uint32
	maxKnown   uint32
	blocks     map[uint32]*probeBlock
	httpHead   uint32
	lags       []lagSample
	restarts   []restartWindow
	endpoints  map[string]*endpointState
	pollErrors map[string]int
	records    *bufio.Writer
	recordsF   *os.File
	report     *ProbeReport
	startedAt  time.Time
	nowUs      func() int64
}

func NewLiveProbe(cfg ProbeConfig) *LiveProbe {
	if cfg.NodeosURL == "" {
		cfg.NodeosURL = cfg.WriterURL
	}
	p := &LiveProbe{
		cfg:        cfg,
		client:     &http.Client{Timeout: 2 * time.Second},
		blocks:     map[uint32]*probeBlock{},
		endpoints:  map[string]*endpointState{},
		pollErrors: map[string]int{},
		nowUs:      func() int64 { return time.Now().UnixMicro() },
	}
	if cfg.DataPath != "" {
		p.paths = append(p.paths, PathLocal)
	}
	if cfg.CoreindexURL != "" {
		p.paths = append(p.paths, PathHTTP)
	}
	if cfg.DataPath != "" && cfg.CoreindexURL != "" {
		p.paths = append(p.paths, PathABI)
	}
	if cfg.StreamAddress != "" {
		p.paths = append(p.paths, PathStream)
	}
	return p
}

func (p *LiveProbe) probes(path string) bool {
	for _, q := range p.paths {
		if q == path {
			return true
		}
	}
	return false
}

type fcInt64 int64

func (v *fcInt64) UnmarshalJSON(b []byte) error {
	n, err := strconv.ParseInt(strings.Trim(string(b), `"`), 10, 64)
	*v = fcInt64(n)
	return err
}

type recentRow struct {
	Block         uint32  `json:"block"`
	NotifiedAtUs  fcInt64 `json:"notified_at_us"`
	CommittedAtUs fcInt64 `json:"committed_at_us"`
}

func (p *LiveProbe) fetchRecent() ([]recentRow, error) {
	var rows []recentRow
	err := p.getJSON(p.cfg.WriterURL+"/v1/roborovski_writer/recent", &rows)
	return rows, err
}

func (p *LiveProbe) getJSON(url string, out any) error {
	resp, err := p.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (p *LiveProbe) Run(ctx context.Context) (*ProbeReport, error) {
	if len(p.paths) == 0 {
		return nil, fmt.Errorf("no serving path to probe: give --data-path, --coreindex-url or --stream-address")
	}
	if p.cfg.RecordsPath != "" {
		f, err := os.Create(p.cfg.RecordsPath)
		if err != nil {
			return nil, err
		}
		p.recordsF = f
		p.records = bufio.NewWriter(f)
		defer func() {
			p.records.Flush()
			f.Close()
		}()
	}

	start, err := p.waitForStart(ctx)
	if err != nil {
		return nil, err
	}
	p.start = start
	p.maxKnown = start - 1
	p.startedAt = time.Now()
	p.report = &ProbeReport{StartBlock: start, StartedAt: p.startedAt.UTC().Format(time.RFC3339), Paths: map[string]*PathReport{}}
	for _, path := range p.paths {
		p.report.Paths[path] = &PathReport{Classes: map[string]*ClassStats{}, hours: map[int]*ClassStats{}}
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	spawn := func(fn func(context.Context)) {
		wg.Add(1)
		go func() { defer wg.Done(); fn(runCtx) }()
	}
	spawn(p.pollWriter)
	spawn(p.pollNodeos)
	if p.probes(PathLocal) {
		reader, err := corereader.Open(p.cfg.DataPath, corereader.DefaultOpenConfig())
		if err != nil {
			cancelRun()
			return nil, fmt.Errorf("open %s: %w", p.cfg.DataPath, err)
		}
		defer reader.Close()
		spawn(func(c context.Context) { p.pollLocal(c, reader) })
	}
	if p.probes(PathABI) {
		spawn(p.pollABI)
	}
	if p.probes(PathHTTP) {
		spawn(p.pollHTTP)
	}
	if p.probes(PathStream) {
		spawn(p.followStream)
	}

	if p.cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.Duration)
		defer cancel()
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for running := true; running; {
		select {
		case <-ctx.Done():
			running = false
		case <-tick.C:
			p.finalize(false)
		}
	}

	p.mu.Lock()
	drainUntil := p.nowUs() + p.cfg.MissingAfter.Microseconds()
	p.mu.Unlock()
	for p.pending() > 0 && p.nowUs() < drainUntil {
		time.Sleep(250 * time.Millisecond)
		p.finalize(false)
	}
	cancelRun()
	wg.Wait()
	p.finalize(true)
	return p.buildReport(), nil
}

func (p *LiveProbe) waitForStart(ctx context.Context) (uint32, error) {
	for {
		rows, err := p.fetchRecent()
		if err == nil && len(rows) > 0 {
			var max uint32
			for _, r := range rows {
				if r.Block > max {
					max = r.Block
				}
			}
			return max + 1, nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return 0, fmt.Errorf("writer ring unavailable: %w", err)
			}
			return 0, fmt.Errorf("writer ring stayed empty")
		case <-time.After(p.cfg.PollInterval):
		}
	}
}

func (p *LiveProbe) endpoint(name string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.endpoints[name]
	if st == nil {
		st = &endpointState{}
		p.endpoints[name] = st
	}
	if err != nil {
		if st.downSince == 0 {
			st.downSince = p.nowUs()
		}
		p.pollErrors[name]++
		return
	}
	if st.downSince != 0 && st.everUp && name != "local" {
		p.restarts = append(p.restarts, restartWindow{fromUs: st.downSince, toUs: p.nowUs() + p.cfg.RestartGrace.Microseconds()})
	}
	st.downSince = 0
	st.everUp = true
}

func (p *LiveProbe) blockLocked(n uint32) *probeBlock {
	if n < p.start {
		return nil
	}
	nowUs := p.nowUs()
	for m := p.maxKnown + 1; m <= n; m++ {
		p.blocks[m] = &probeBlock{num: m, firstKnownUs: nowUs, seen: map[string]int64{}}
		p.maxKnown = m
	}
	return p.blocks[n]
}

func (p *LiveProbe) observe(path string, n uint32, atUs int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b := p.blockLocked(n); b != nil {
		if _, ok := b.seen[path]; !ok {
			b.seen[path] = atUs
		}
	}
}

func (p *LiveProbe) every(ctx context.Context, interval time.Duration, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		fn()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *LiveProbe) pollWriter(ctx context.Context) {
	p.every(ctx, p.cfg.PollInterval, func() {
		rows, err := p.fetchRecent()
		p.endpoint("writer", err)
		if err != nil {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, r := range rows {
			if b := p.blockLocked(r.Block); b != nil && b.notifiedUs == 0 {
				b.notifiedUs = int64(r.NotifiedAtUs)
				b.committedUs = int64(r.CommittedAtUs)
			}
		}
	})
}

type nodeosInfo struct {
	LIB           uint32 `json:"last_irreversible_block_num"`
	HeadBlockTime string `json:"head_block_time"`
}

func (p *LiveProbe) pollNodeos(ctx context.Context) {
	p.every(ctx, 500*time.Millisecond, func() {
		var info nodeosInfo
		err := p.getJSON(p.cfg.NodeosURL+"/v1/chain/get_info", &info)
		p.endpoint("nodeos", err)
		if err != nil {
			return
		}
		headTime, err := time.Parse("2006-01-02T15:04:05.000", info.HeadBlockTime)
		p.mu.Lock()
		defer p.mu.Unlock()
		nowUs := p.nowUs()
		if err == nil {
			p.lags = append(p.lags, lagSample{atUs: nowUs, lagUs: nowUs - headTime.UnixMicro()})
		}
		p.blockLocked(info.LIB)
	})
}

func (p *LiveProbe) pollLocal(ctx context.Context, reader corereader.Reader) {
	next := p.start
	p.every(ctx, p.cfg.PollInterval, func() {
		head, _, err := reader.GetStateProps(true)
		if err != nil {
			if err != corereader.ErrNotReady {
				p.endpoint("local", err)
			}
			return
		}
		p.endpoint("local", nil)
		for next <= head {
			end := head
			if end-next >= 100 {
				end = next + 99
			}
			raws, err := reader.GetRawBlockBatch(next, end)
			if err != nil {
				p.endpoint("local", err)
				return
			}
			atUs := p.nowUs()
			for i := range raws {
				p.decoded(&raws[i], reader)
				p.observe(PathLocal, raws[i].BlockNum, atUs)
			}
			next = end + 1
		}
	})
}

func (p *LiveProbe) decoded(raw *corereader.RawBlock, reader corereader.Reader) {
	var trxs [][32]byte
	if reader != nil {
		trxs, _ = reader.GetTransactionIDsRaw(raw.BlockNum)
	}
	var expect map[uint64][]byte
	for _, a := range raw.Actions {
		if a.ReceiverUint64 != a.ContractUint64 || !abicache.IsSetabi(a.ContractUint64, a.ActionUint64) {
			continue
		}
		account, abiJSON, err := abicache.ParseSetabi(raw.GetActionData(a.DataIndex))
		if err != nil {
			continue
		}
		if expect == nil {
			expect = map[uint64][]byte{}
		}
		expect[account] = canonicalABI(abiJSON)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.blockLocked(raw.BlockNum)
	if b == nil || b.decoded {
		return
	}
	b.decoded = true
	b.actions = len(raw.Actions)
	b.trxs = len(trxs)
	b.abiExpect = expect
	b.abiSeen = map[uint64]bool{}
}

type coreindexInfo struct {
	Head uint32 `json:"head_block_num"`
}

func (p *LiveProbe) pollHTTP(ctx context.Context) {
	next := p.start
	var retry []uint32
	p.every(ctx, p.cfg.PollInterval, func() {
		var info coreindexInfo
		err := p.getJSON(p.cfg.CoreindexURL+"/v1/history/get_info", &info)
		p.endpoint("coreindex", err)
		if err != nil {
			return
		}
		p.mu.Lock()
		p.httpHead = info.Head
		p.mu.Unlock()
		for ; next <= info.Head; next++ {
			retry = append(retry, next)
		}
		kept := retry[:0]
		for i, n := range retry {
			served, err := p.fetchBlockBinary(n)
			if err != nil {
				p.endpoint("coreindex", err)
				kept = append(kept, retry[i:]...)
				break
			}
			if served {
				p.observe(PathHTTP, n, p.nowUs())
			} else {
				kept = append(kept, n)
			}
		}
		retry = kept
	})
}

func (p *LiveProbe) fetchBlockBinary(n uint32) (bool, error) {
	body := fmt.Sprintf(`{"block_num":%d}`, n)
	resp, err := p.client.Post(p.cfg.CoreindexURL+"/v1/history/get_block_binary", "application/json", strings.NewReader(body))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("get_block_binary %d: HTTP %d", n, resp.StatusCode)
	}
	got, err := corestream.DecodeGetBlockResponse(payload)
	if err != nil {
		return false, err
	}
	return got.BlockNum == n && len(got.Data) > 0, nil
}

type abiCheck struct {
	block   uint32
	account uint64
	expect  []byte
}

func (p *LiveProbe) pollABI(ctx context.Context) {
	p.every(ctx, p.cfg.PollInterval, func() {
		p.mu.Lock()
		var checks []abiCheck
		for _, b := range p.blocks {
			if !b.decoded || b.num > p.httpHead {
				continue
			}
			for account, expect := range b.abiExpect {
				if !b.abiSeen[account] {
					checks = append(checks, abiCheck{b.num, account, expect})
				}
			}
		}
		p.mu.Unlock()
		sort.Slice(checks, func(i, j int) bool { return checks[i].block < checks[j].block })
		for _, c := range checks {
			ok, err := p.abiServed(c)
			if err != nil {
				p.endpoint("coreindex", err)
				return
			}
			if !ok {
				continue
			}
			atUs := p.nowUs()
			p.mu.Lock()
			if b := p.blocks[c.block]; b != nil {
				b.abiSeen[c.account] = true
				if len(b.abiSeen) == len(b.abiExpect) {
					if _, done := b.seen[PathABI]; !done {
						b.seen[PathABI] = atUs
					}
				}
			}
			p.mu.Unlock()
		}
	})
}

func (p *LiveProbe) abiServed(c abiCheck) (bool, error) {
	url := fmt.Sprintf("%s/abi/%s/%d", p.cfg.CoreindexURL, chain.NameToString(c.account), c.block)
	resp, err := p.client.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return c.expect == nil, nil
	case resp.StatusCode != http.StatusOK:
		return false, nil
	case c.expect == nil:
		trimmed := bytes.TrimSpace(body)
		return len(trimmed) == 0 || string(trimmed) == "null", nil
	default:
		return bytes.Equal(canonicalABI(body), c.expect), nil
	}
}

func (p *LiveProbe) followStream(ctx context.Context) {
	cfg := corestream.DefaultClientConfig()
	cfg.AckInterval = 1
	client := corestream.NewClient(p.cfg.StreamAddress, cfg)
	for {
		err := client.Connect(p.start - 1)
		p.endpoint("stream", err)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	defer client.Close()
	for ctx.Err() == nil {
		n, data, err := client.NextBlockWithTimeout(500 * time.Millisecond)
		if err != nil {
			return
		}
		if data == nil {
			if !client.IsConnected() {
				p.endpoint("stream", fmt.Errorf("stream disconnected"))
			}
			continue
		}
		p.endpoint("stream", nil)
		p.observe(PathStream, n, p.nowUs())
	}
}

func (p *LiveProbe) pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.blocks)
}

func (p *LiveProbe) classify(b *probeBlock) string {
	ref := b.notifiedUs
	if ref == 0 {
		ref = b.firstKnownUs
	}
	for _, w := range p.restarts {
		if ref >= w.fromUs && ref < w.toUs {
			return ClassAfterRestart
		}
	}
	i := sort.Search(len(p.lags), func(i int) bool { return p.lags[i].atUs > ref })
	if i > 0 && p.lags[i-1].lagUs > p.cfg.BehindAfter.Microseconds() {
		return ClassBehind
	}
	return ClassSteady
}

func (p *LiveProbe) applicable(b *probeBlock, path string) (applies, known bool) {
	if path != PathABI {
		return true, true
	}
	if !b.decoded {
		return false, false
	}
	return len(b.abiExpect) > 0, true
}

func (p *LiveProbe) finalize(final bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	nowUs := p.nowUs()
	nums := make([]uint32, 0, len(p.blocks))
	for n := range p.blocks {
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	for _, n := range nums {
		b := p.blocks[n]
		ref := b.notifiedUs
		if ref == 0 {
			ref = b.firstKnownUs
		}
		expired := nowUs >= ref+p.cfg.MissingAfter.Microseconds()
		complete := b.notifiedUs != 0
		for _, path := range p.paths {
			applies, known := p.applicable(b, path)
			if !known || (applies && b.seen[path] == 0) {
				complete = false
			}
		}
		if !complete && !expired {
			if final {
				p.report.Unresolved++
			}
			continue
		}
		p.record(b)
		delete(p.blocks, n)
	}
}

func (p *LiveProbe) record(b *probeBlock) {
	class := p.classify(b)
	r := p.report
	r.Blocks++
	r.Actions += b.actions
	r.Transactions += b.trxs
	if len(b.abiExpect) > 0 {
		r.SetabiBlocks++
	}
	if b.num > r.EndBlock {
		r.EndBlock = b.num
	}
	for _, path := range p.paths {
		applies, known := p.applicable(b, path)
		if known && !applies {
			continue
		}
		rec := ProbeRecord{Block: b.num, Path: path, Class: class, NotifiedAtUs: b.notifiedUs, CommittedAtUs: b.committedUs, Actions: b.actions, Transactions: b.trxs}
		latency := math.Inf(1)
		obs := b.seen[path]
		switch {
		case b.notifiedUs == 0:
			rec.Status = StatusNoT0
			rec.ObservedAtUs = obs
		case obs == 0:
			rec.Status = StatusMissing
		default:
			rec.ObservedAtUs = obs
			latency = float64(obs-b.notifiedUs) / 1000
			rec.LatencyMs = latency
			rec.Status = StatusOnTime
			if latency > float64(p.cfg.LateAfter.Milliseconds()) {
				rec.Status = StatusLate
			}
		}
		pr := r.Paths[path]
		stats := pr.Classes[class]
		if stats == nil {
			stats = &ClassStats{}
			pr.Classes[class] = stats
		}
		stats.add(latency)
		if class == ClassSteady && b.notifiedUs != 0 {
			hour := int(time.UnixMicro(b.notifiedUs).Sub(p.startedAt) / time.Hour)
			hs := pr.hours[hour]
			if hs == nil {
				hs = &ClassStats{}
				pr.hours[hour] = hs
			}
			hs.add(latency)
		}
		if rec.Status != StatusOnTime {
			pr.Late = append(pr.Late, rec)
		}
		if p.records != nil {
			line, _ := json.Marshal(rec)
			p.records.Write(append(line, '\n'))
		}
	}
}

func (s *ClassStats) add(latencyMs float64) {
	s.Count++
	s.latencies = append(s.latencies, latencyMs)
	switch {
	case math.IsInf(latencyMs, 1):
		s.Missing++
	case latencyMs > 30000:
		s.Over30s++
		s.Over5s++
		s.Over1s++
	case latencyMs > 5000:
		s.Over5s++
		s.Over1s++
	case latencyMs > 1000:
		s.Over1s++
	}
}

func (s *ClassStats) summarize(lateMs float64) {
	if s.Count == 0 {
		return
	}
	sort.Float64s(s.latencies)
	within := sort.Search(len(s.latencies), func(i int) bool { return s.latencies[i] > lateMs })
	s.WithinTarget = float64(within) / float64(s.Count)
	at := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(s.latencies)))) - 1
		if i < 0 {
			i = 0
		}
		return s.latencies[i]
	}
	s.P50Ms, s.P90Ms, s.P99Ms, s.P999Ms, s.MaxMs = at(0.50), at(0.90), at(0.99), at(0.999), s.latencies[len(s.latencies)-1]
	for _, v := range []*float64{&s.P50Ms, &s.P90Ms, &s.P99Ms, &s.P999Ms, &s.MaxMs} {
		if math.IsInf(*v, 1) {
			*v = -1
		}
	}
}

func (p *LiveProbe) buildReport() *ProbeReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.report
	ended := time.Now()
	r.EndedAt = ended.UTC().Format(time.RFC3339)
	r.DurationSeconds = ended.Sub(p.startedAt).Seconds()
	r.Restarts = []string{}
	for _, w := range p.restarts {
		back := time.UnixMicro(w.toUs).Add(-p.cfg.RestartGrace)
		r.Restarts = append(r.Restarts, fmt.Sprintf("%s to %s", time.UnixMicro(w.fromUs).UTC().Format(time.RFC3339), back.UTC().Format(time.RFC3339)))
	}
	lateMs := float64(p.cfg.LateAfter.Milliseconds())
	r.Failures = []string{}
	for _, path := range p.paths {
		pr := r.Paths[path]
		pr.PollErrors = p.pollErrors[endpointFor(path)]
		for _, class := range []string{ClassSteady, ClassBehind, ClassAfterRestart} {
			s := pr.Classes[class]
			if s == nil {
				continue
			}
			s.summarize(lateMs)
			if s.Missing > 0 {
				r.Failures = append(r.Failures, fmt.Sprintf("%s: %d %s blocks missing after %s", path, s.Missing, class, p.cfg.MissingAfter))
			}
		}
		if s := pr.Classes[ClassSteady]; s != nil && s.WithinTarget < targetFraction {
			r.Failures = append(r.Failures, fmt.Sprintf("%s: %.3f%% of steady blocks within %s, target %.0f%%", path, s.WithinTarget*100, p.cfg.LateAfter, targetFraction*100))
		}
		for hour, s := range pr.hours {
			s.summarize(lateMs)
			if pr.WorstHour == nil || s.WithinTarget < pr.WorstHour.Stats.WithinTarget ||
				(s.WithinTarget == pr.WorstHour.Stats.WithinTarget && s.P99Ms > pr.WorstHour.Stats.P99Ms) {
				pr.WorstHour = &HourStats{Hour: hour, StartedAt: p.startedAt.Add(time.Duration(hour) * time.Hour).UTC().Format(time.RFC3339), Stats: *s}
			}
		}
		sort.Slice(pr.Late, func(i, j int) bool { return pr.Late[i].Block < pr.Late[j].Block })
		if pr.Late == nil {
			pr.Late = []ProbeRecord{}
		}
	}
	if r.Blocks == 0 {
		r.Failures = append(r.Failures, "no block was resolved")
	}
	r.Status = "passed"
	if len(r.Failures) > 0 {
		r.Status = "failed"
	}
	return r
}

func endpointFor(path string) string {
	switch path {
	case PathHTTP, PathABI:
		return "coreindex"
	}
	return path
}

func (p *LiveProbe) PrintReport(r *ProbeReport) {
	if p.cfg.OutputJSON {
		out, _ := json.MarshalIndent(r, "", "  ")
		fmt.Println(string(out))
		return
	}
	fmt.Printf("Live availability probe: %s\n", strings.ToUpper(r.Status))
	fmt.Printf("Blocks %d to %d (%d resolved, %d unresolved) over %.0fs\n", r.StartBlock, r.EndBlock, r.Blocks, r.Unresolved, r.DurationSeconds)
	if r.Blocks > 0 {
		fmt.Printf("Load: %d transactions, %d actions (%.1f actions per block), %d blocks with setabi\n",
			r.Transactions, r.Actions, float64(r.Actions)/float64(r.Blocks), r.SetabiBlocks)
	}
	if len(r.Restarts) > 0 {
		fmt.Printf("Restarts observed: %s\n", strings.Join(r.Restarts, ", "))
	}
	ms := func(v float64) string {
		if v < 0 {
			return "missing"
		}
		return fmt.Sprintf("%.0fms", v)
	}
	for _, path := range probePaths {
		pr, ok := r.Paths[path]
		if !ok {
			continue
		}
		fmt.Printf("\n%s (poll errors %d)\n", path, pr.PollErrors)
		for _, class := range []string{ClassSteady, ClassBehind, ClassAfterRestart} {
			s := pr.Classes[class]
			if s == nil {
				continue
			}
			fmt.Printf("  %-13s n=%d within=%.3f%% p50=%s p90=%s p99=%s p99.9=%s max=%s >1s=%d >5s=%d >30s=%d missing=%d\n",
				class, s.Count, s.WithinTarget*100, ms(s.P50Ms), ms(s.P90Ms), ms(s.P99Ms), ms(s.P999Ms), ms(s.MaxMs), s.Over1s, s.Over5s, s.Over30s, s.Missing)
		}
		if wh := pr.WorstHour; wh != nil {
			fmt.Printf("  worst hour %d from %s: within=%.3f%% p99=%s\n", wh.Hour, wh.StartedAt, wh.Stats.WithinTarget*100, ms(wh.Stats.P99Ms))
		}
		for i, rec := range pr.Late {
			if i == 50 {
				fmt.Printf("  ... %d more in the records file or --output-json\n", len(pr.Late)-50)
				break
			}
			latency := rec.LatencyMs
			if rec.ObservedAtUs == 0 || rec.NotifiedAtUs == 0 {
				latency = -1
			}
			fmt.Printf("  %s block %d (%s): notified %d committed %d observed %d latency %s\n",
				rec.Status, rec.Block, rec.Class, rec.NotifiedAtUs, rec.CommittedAtUs, rec.ObservedAtUs, ms(latency))
		}
	}
	for _, f := range r.Failures {
		fmt.Printf("\nFAIL %s", f)
	}
	fmt.Println()
}
