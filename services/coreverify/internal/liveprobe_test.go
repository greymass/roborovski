package internal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/corestream"
)

type fakeChain struct {
	mu       sync.Mutex
	rows     []recentRow
	served   uint32
	withhold map[uint32]bool
}

func (f *fakeChain) commit(n uint32, notifiedUs int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, recentRow{Block: n, NotifiedAtUs: fcInt64(notifiedUs), CommittedAtUs: fcInt64(notifiedUs + 1000)})
}

func (f *fakeChain) serve(n uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.served = n
}

func (f *fakeChain) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/roborovski_writer/recent", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var rows []string
		for _, r := range f.rows {
			rows = append(rows, fmt.Sprintf(`{"block":%d,"notified_at_us":"%d","committed_at_us":"%d"}`, r.Block, r.NotifiedAtUs, r.CommittedAtUs))
		}
		fmt.Fprintf(w, "[%s]", strings.Join(rows, ","))
	})
	mux.HandleFunc("/v1/chain/get_info", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		lib := uint32(0)
		if len(f.rows) > 0 {
			lib = f.rows[len(f.rows)-1].Block
		}
		fmt.Fprintf(w, `{"last_irreversible_block_num":%d,"head_block_time":"%s"}`, lib, time.Now().UTC().Format("2006-01-02T15:04:05.000"))
	})
	mux.HandleFunc("/v1/history/get_info", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		fmt.Fprintf(w, `{"head_block_num":%d}`, f.served)
	})
	mux.HandleFunc("/v1/history/get_block_binary", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BlockNum uint32 `json:"block_num"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		defer f.mu.Unlock()
		if req.BlockNum > f.served || f.withhold[req.BlockNum] {
			http.Error(w, "Block not found", http.StatusNotFound)
			return
		}
		w.Write(corestream.EncodeGetBlockResponse(&corestream.GetBlockResponse{BlockNum: req.BlockNum, Data: []byte{1}}))
	})
	return mux
}

func TestLiveProbeHTTPPath(t *testing.T) {
	chain := &fakeChain{withhold: map[uint32]bool{}}
	chain.commit(100, time.Now().UnixMicro())
	chain.serve(100)
	srv := httptest.NewServer(chain.handler())
	defer srv.Close()

	records := filepath.Join(t.TempDir(), "records.jsonl")
	probe := NewLiveProbe(ProbeConfig{
		WriterURL:    srv.URL,
		CoreindexURL: srv.URL,
		RecordsPath:  records,
		PollInterval: 10 * time.Millisecond,
		LateAfter:    200 * time.Millisecond,
		MissingAfter: 600 * time.Millisecond,
		BehindAfter:  5 * time.Second,
		RestartGrace: time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *ProbeReport)
	go func() {
		r, err := probe.Run(ctx)
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()

	time.Sleep(50 * time.Millisecond)
	for n := uint32(101); n <= 110; n++ {
		chain.commit(n, time.Now().UnixMicro())
		if n == 105 {
			chain.mu.Lock()
			chain.withhold[105] = true
			chain.mu.Unlock()
		}
		if n == 107 {
			go func() {
				time.Sleep(350 * time.Millisecond)
				chain.mu.Lock()
				delete(chain.withhold, 105)
				chain.mu.Unlock()
			}()
		}
		chain.serve(n)
		time.Sleep(20 * time.Millisecond)
	}
	chain.commit(111, time.Now().UnixMicro())
	time.Sleep(100 * time.Millisecond)
	cancel()
	r := <-done

	if r.StartBlock != 101 {
		t.Fatalf("start block %d, want 101", r.StartBlock)
	}
	if r.Blocks != 11 {
		t.Fatalf("resolved %d blocks, want 11", r.Blocks)
	}
	s := r.Paths[PathHTTP].Classes[ClassSteady]
	if s.Count != 11 || s.Missing != 1 {
		t.Fatalf("http steady count %d missing %d, want 11 and 1", s.Count, s.Missing)
	}
	var late []uint32
	for _, rec := range r.Paths[PathHTTP].Late {
		late = append(late, rec.Block)
	}
	if fmt.Sprint(late) != "[105 111]" {
		t.Fatalf("late or missing blocks %v, want [105 111]", late)
	}
	if r.Status != "failed" {
		t.Fatalf("status %s, want failed", r.Status)
	}
	data, err := os.ReadFile(records)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(data), "\n"); lines != 11 {
		t.Fatalf("%d records, want 11", lines)
	}
}

func TestClassStatsCountsMissingAgainstPercentile(t *testing.T) {
	var s ClassStats
	for i := 0; i < 98; i++ {
		s.add(100)
	}
	s.add(1500)
	s.add(math.Inf(1))
	s.summarize(1000)
	if s.Count != 100 || s.Missing != 1 || s.Over1s != 1 {
		t.Fatalf("count %d missing %d over1s %d", s.Count, s.Missing, s.Over1s)
	}
	if s.WithinTarget != 0.98 {
		t.Fatalf("within %v, want 0.98", s.WithinTarget)
	}
	if s.P99Ms != 1500 || s.MaxMs != -1 {
		t.Fatalf("p99 %v max %v", s.P99Ms, s.MaxMs)
	}
}

func TestLiveProbeABIPath(t *testing.T) {
	abis := map[string]string{
		"/abi/alice/50": `{"version":"eosio::abi/1.1","types":[],"structs":[],"actions":[],"tables":[]}`,
		"/abi/alice/49": `{"version":"eosio::abi/1.0","types":[],"structs":[],"actions":[],"tables":[]}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := abis[r.URL.Path]
		if !ok {
			http.Error(w, `{"error": "ABI not found"}`, http.StatusNotFound)
			return
		}
		w.Write([]byte(body))
	}))
	defer srv.Close()
	p := NewLiveProbe(ProbeConfig{CoreindexURL: srv.URL})
	alice := chain.StringToName("alice")
	want := canonicalABI([]byte(abis["/abi/alice/50"]))

	for _, c := range []struct {
		check abiCheck
		want  bool
	}{
		{abiCheck{50, alice, want}, true},
		{abiCheck{49, alice, want}, false},
		{abiCheck{51, alice, nil}, true},
		{abiCheck{50, alice, nil}, false},
	} {
		got, err := p.abiServed(c.check)
		if err != nil || got != c.want {
			t.Fatalf("block %d clear=%v: served %v err %v, want %v", c.check.block, c.check.expect == nil, got, err, c.want)
		}
	}
}

func TestLiveProbeReadsSetabiClear(t *testing.T) {
	p := NewLiveProbe(ProbeConfig{})
	p.start = 10
	p.maxKnown = 9
	alice := chain.StringToName("alice")
	eosio := chain.StringToName("eosio")
	data := make([]byte, 9)
	binary.LittleEndian.PutUint64(data, alice)
	raw := corereader.RawBlock{BlockNum: 10, Actions: []corereader.CanonicalAction{
		{ReceiverUint64: eosio, ContractUint64: eosio, ActionUint64: chain.StringToName("setabi"), DataIndex: 0},
		{ReceiverUint64: alice, ContractUint64: eosio, ActionUint64: chain.StringToName("setabi"), DataIndex: 0},
	}}
	raw.SetActionData(data, []uint32{0}, []uint32{uint32(len(data))})
	p.decoded(&raw, nil)
	b := p.blocks[10]
	if !b.decoded || b.actions != 2 || len(b.abiExpect) != 1 {
		t.Fatalf("decoded %v actions %d expectations %d", b.decoded, b.actions, len(b.abiExpect))
	}
	if exp, ok := b.abiExpect[alice]; !ok || exp != nil {
		t.Fatalf("expected a clear for alice, got %q", exp)
	}
}
