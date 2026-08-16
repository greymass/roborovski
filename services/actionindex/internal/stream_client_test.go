package internal

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/chain"
)

type runFixture struct {
	server *StreamServer
	b      *ActionBroadcaster
	idx    *Indexes
	reader *stubBaseReader
}

// newRunFixture indexes the given seqs for eosio.token and enters live mode at maxSeq.
func newRunFixture(t *testing.T, seqs []uint64) *runFixture {
	t.Helper()
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.walCompactor.Stop() })
	f := &runFixture{
		idx:    idx,
		reader: &stubBaseReader{bySeq: map[uint64]chain.ActionTrace{}},
		b:      NewActionBroadcaster(),
	}
	var maxSeq uint64
	for _, s := range seqs {
		f.addIndexed(t, s)
		if s > maxSeq {
			maxSeq = s
		}
	}
	if err := idx.Commit(1, 1); err != nil {
		t.Fatal(err)
	}
	idx.SetBulkMode(false)
	f.b.SetLiveMode(true)
	f.b.SetState(maxSeq, maxSeq)
	f.server = NewStreamServer(f.b, idx, f.reader, nil, 10, 1, 4)
	return f
}

func (f *runFixture) addIndexed(t *testing.T, seq uint64) {
	t.Helper()
	contract := chain.StringToName("eosio.token")
	f.idx.Add(contract, contract, chain.StringToName("transfer"), seq, 1000)
	f.reader.bySeq[seq] = chain.ActionTrace{
		BlockNum: 1, BlockTime: "1970-01-01T00:16:40.000",
		Receiver: "eosio.token",
		Act:      chain.Action{Account: "eosio.token", Name: "transfer"},
	}
}

func tokenFilter() ActionFilter {
	return ActionFilter{Contracts: map[uint64]struct{}{chain.StringToName("eosio.token"): {}}}
}

type runHarness struct {
	mu        sync.Mutex
	delivered []uint64
	caughtUp  chan struct{}
	runErr    chan error
	client    *StreamClient
	onSend    func(StreamedAction)
}

func startRun(t *testing.T, f *runFixture, startSeq uint64, onSend func(StreamedAction)) *runHarness {
	return startRunCtx(t, context.Background(), f, startSeq, onSend)
}

func startRunCtx(t *testing.T, ctx context.Context, f *runFixture, startSeq uint64, onSend func(StreamedAction)) *runHarness {
	t.Helper()
	h := &runHarness{caughtUp: make(chan struct{}), runErr: make(chan error, 1), onSend: onSend}
	h.client = NewStreamClient(1, f.server, tokenFilter(), startSeq, false)
	go func() {
		h.runErr <- h.client.Run(ctx,
			func(a StreamedAction) error {
				h.mu.Lock()
				h.delivered = append(h.delivered, a.GlobalSeq)
				h.mu.Unlock()
				if h.onSend != nil {
					h.onSend(a)
				}
				return nil
			},
			func() error { close(h.caughtUp); return nil },
			func(e StreamError) error { return nil },
			func() error { return nil },
		)
	}()
	return h
}

func (h *runHarness) seqs() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.delivered...)
}

func (h *runHarness) waitFor(t *testing.T, seq uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, s := range h.seqs() {
			if s == seq {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for seq %d, delivered %v", seq, h.seqs())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *runHarness) stop(t *testing.T) {
	t.Helper()
	h.client.Close()
	if err := <-h.runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestRun_StartSeqEqualsHead_DeliversHeadAction(t *testing.T) {
	f := newRunFixture(t, []uint64{100})
	h := startRun(t, f, 100, nil)
	<-h.caughtUp
	h.waitFor(t, 100)
	h.stop(t)
	if got := h.seqs(); len(got) != 1 || got[0] != 100 {
		t.Fatalf("delivered = %v, want [100]", got)
	}
}

func TestRun_BroadcastDuringCatchup_DeliveredOnceInOrder(t *testing.T) {
	seqs := make([]uint64, 0, 100)
	for s := uint64(1); s <= 100; s++ {
		seqs = append(seqs, s)
	}
	f := newRunFixture(t, seqs)
	var injectOnce sync.Once
	h := startRun(t, f, 1, func(a StreamedAction) {
		if a.GlobalSeq == 50 {
			injectOnce.Do(func() {
				f.addIndexed(t, 101)
				if err := f.idx.Commit(2, 2); err != nil {
					t.Error(err)
				}
				f.b.SetState(101, 101)
				act, via := tokenAction(101)
				f.b.Broadcast(act, via)
			})
		}
	})
	<-h.caughtUp
	h.waitFor(t, 101)
	h.stop(t)

	got := h.seqs()
	count101 := 0
	for i, s := range got {
		if s == 101 {
			count101++
		}
		if i > 0 && got[i] <= got[i-1] {
			t.Fatalf("delivery not strictly increasing at %d: %v", i, got)
		}
	}
	if count101 != 1 {
		t.Fatalf("seq 101 delivered %d times, want exactly once; %v", count101, got)
	}
	if len(got) != 101 {
		t.Fatalf("delivered %d actions, want 101", len(got))
	}
}

func TestRunCatchup_BoundedConcurrency(t *testing.T) {
	f := newRunFixture(t, []uint64{1, 2, 3})
	f.server.catchupSem = make(chan struct{}, 2)

	var mu sync.Mutex
	var active, maxActive int
	slow := func(StreamedAction) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
	}

	hs := make([]*runHarness, 6)
	for i := range hs {
		hs[i] = startRun(t, f, 1, slow)
	}
	for _, h := range hs {
		select {
		case <-h.caughtUp:
		case <-time.After(4 * time.Second):
			t.Fatal("timed out waiting for catchup to complete")
		}
	}
	for _, h := range hs {
		h.stop(t)
	}

	mu.Lock()
	defer mu.Unlock()
	if maxActive > 2 {
		t.Fatalf("max concurrent catchups = %d, want <= 2", maxActive)
	}
}

func TestRunCatchup_CtxCancelledWhileWaiting_ReturnsPromptly(t *testing.T) {
	f := newRunFixture(t, []uint64{1})
	f.server.catchupSem = make(chan struct{}, 1)
	f.server.catchupSem <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	h := startRunCtx(t, ctx, f, 1, nil)
	time.Sleep(50 * time.Millisecond)

	cancelled := time.Now()
	cancel()
	select {
	case err := <-h.runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	if waited := time.Since(cancelled); waited > time.Second {
		t.Fatalf("Run returned %v after cancel, want prompt return", waited)
	}
}
