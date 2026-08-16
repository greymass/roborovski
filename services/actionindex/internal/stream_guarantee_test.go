package internal

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/chain"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

const (
	guaranteeSeed    = 1
	guaranteeActions = 2000
	guaranteeBurst   = 1500
	// Wide commit cadence: production commits per block, not per action.
	guaranteeCommitEvery = 50

	guaranteePayloadBytes = 8192
)

// guaranteeConsumer resumes from lastAccepted+1 on any disconnect, mirroring the 0.3.0/0.4.0 client.
type guaranteeConsumer struct {
	url          string
	t            *testing.T
	mu           sync.Mutex
	received     map[uint64]int
	lastAccepted uint64
	subSeqGaps   int
	sessions     int
	drainDelay   time.Duration
	dropAt       map[uint64]bool
	rewindEvery  int
	stallAt      uint64
	stallFor     time.Duration
	stalled      bool
}

func (c *guaranteeConsumer) run(ctx context.Context, done func() bool) {
	for !done() {
		if err := c.session(ctx, done); err != nil && ctx.Err() != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *guaranteeConsumer) session(ctx context.Context, done func() bool) error {
	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	conn, _, err := websocket.Dial(dialCtx, c.url, nil)
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "session over")

	c.mu.Lock()
	start := c.lastAccepted + 1
	c.sessions++
	// Some clients persist their resume point lazily and come back behind the high-water mark.
	if c.rewindEvery > 0 && c.sessions%c.rewindEvery == 0 && start > 25 {
		start -= 25
	}
	c.mu.Unlock()
	sub := map[string]any{
		"type": "subscribe", "contracts": []string{"eosio.token"},
		"start_seq": fmt.Sprintf("%d", start),
	}
	if err := wsjson.Write(ctx, conn, sub); err != nil {
		return err
	}

	expectedSubSeq := uint64(1)
	var lastOnConn uint64
	for !done() {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		var m map[string]any
		err := wsjson.Read(readCtx, conn, &m)
		cancel()
		if err != nil {
			return err
		}
		switch m["type"] {
		case "action":
			seq := uint64(mustFloat(c.t, m["global_seq"]))
			if lastOnConn != 0 && seq <= lastOnConn {
				c.t.Errorf("per-connection ordering violation: got global_seq %d after %d", seq, lastOnConn)
			}
			lastOnConn = seq
			if ss, ok := m["sub_seq"].(float64); ok {
				if uint64(ss) != expectedSubSeq {
					c.mu.Lock()
					c.subSeqGaps++
					c.mu.Unlock()
					return nil
				}
				expectedSubSeq = uint64(ss) + 1
			}
			c.mu.Lock()
			c.received[seq]++
			if seq > c.lastAccepted {
				c.lastAccepted = seq
			}
			induceDrop := c.dropAt[seq]
			c.mu.Unlock()
			if induceDrop {
				return nil
			}
			if c.stallAt > 0 && seq >= c.stallAt && !c.stalled {
				c.stalled = true
				time.Sleep(c.stallFor)
			}
			if c.drainDelay > 0 {
				time.Sleep(c.drainDelay)
			}
			ack := map[string]any{"type": "ack", "seq": fmt.Sprintf("%d", seq)}
			wsjson.Write(ctx, conn, ack)
		case "error":
			return nil
		}
	}
	return nil
}

func mustFloat(t *testing.T, v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			t.Fatalf("unparseable numeric %v", v)
		}
		return f
	}
	t.Fatalf("unexpected numeric type %T", v)
	return 0
}

func TestStreamGuarantee_ChaosReconciliation(t *testing.T) {
	rng := rand.New(rand.NewSource(guaranteeSeed))
	tmpDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tmpDir, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	idx, err := NewIndexes(db, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	reader := &stubBaseReader{bySeq: map[uint64]chain.ActionTrace{}}
	b := NewActionBroadcaster()
	srv := NewStreamServer(b, idx, reader, nil, 10, 1, 4)
	wss := NewStreamWebSocketServer(srv, 10)
	if err := wss.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer wss.Close()

	contract := chain.StringToName("eosio.token")
	// Payload large enough that a lagging consumer backs the socket up into sendCh (mode B).
	payload := make([]byte, guaranteePayloadBytes)
	payloadHex := hex.EncodeToString(payload)
	// Traces are populated up front so the stub reader is never written concurrently.
	for seq := uint64(1); seq <= guaranteeActions; seq++ {
		reader.bySeq[seq] = chain.ActionTrace{
			BlockNum: uint32(seq / 10), BlockTime: "1970-01-01T00:16:40.000",
			Receiver: "eosio.token",
			Act:      chain.Action{Account: "eosio.token", Name: "transfer", Data: payloadHex},
		}
	}
	ingest := func(seq uint64) {
		idx.Add(contract, contract, chain.StringToName("transfer"), seq, 1000)
	}

	// Seed history, enter live mode.
	for seq := uint64(1); seq <= 100; seq++ {
		ingest(seq)
	}
	if err := idx.Commit(10, 10); err != nil {
		t.Fatal(err)
	}
	idx.SetBulkMode(false)
	defer idx.walCompactor.Stop()
	b.SetState(100, 100)
	b.SetLiveMode(true)

	consumer := &guaranteeConsumer{
		url:      "ws://" + wss.Addr().String() + "/stream",
		t:        t,
		received: map[uint64]int{},
		dropAt:   map[uint64]bool{},
	}
	// Chaos: reconnect storm before the burst, then an uninterrupted burst against a slow drain.
	for i := 0; i < 20; i++ {
		consumer.dropAt[uint64(rng.Intn(guaranteeActions-guaranteeBurst))+1] = true
	}

	var ingestDone sync.WaitGroup
	ingestDone.Add(1)
	go func() {
		defer ingestDone.Done()
		// Mirrors the production order: commit first, then publish head, then broadcast.
		pending := make([]uint64, 0, guaranteeCommitEvery)
		publish := func(upTo uint64) bool {
			if err := idx.CommitNoSync(uint32(upTo/10), uint32(upTo/10)); err != nil {
				t.Error(err)
				return false
			}
			for _, s := range pending {
				b.SetState(s, s)
				act, via := tokenAction(s)
				act.ActionData = payload
				b.Broadcast(act, via)
			}
			pending = pending[:0]
			return true
		}
		for seq := uint64(101); seq <= guaranteeActions; seq++ {
			ingest(seq)
			pending = append(pending, seq)
			if len(pending) >= guaranteeCommitEvery || seq == guaranteeActions {
				if !publish(seq) {
					return
				}
			}
			// Mid-run liveMode pause (mode F), before the burst so it cannot mask mode B.
			if seq == (guaranteeActions-guaranteeBurst)/2 {
				b.SetLiveMode(false)
				time.Sleep(20 * time.Millisecond)
				b.SetLiveMode(true)
			}
			// Burst window: 10x the consumer's drain rate, overrunning sendCh (mode B).
			if seq < guaranteeActions-guaranteeBurst {
				time.Sleep(time.Duration(rng.Intn(2)+2) * time.Millisecond)
			} else if seq%10 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
		if err := idx.Commit(guaranteeActions/10, guaranteeActions/10); err != nil {
			t.Error(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.received) >= guaranteeActions
	}
	consumer.drainDelay = time.Millisecond
	consumer.rewindEvery = 3
	consumer.stallAt = guaranteeActions - guaranteeBurst + 20
	consumer.stallFor = time.Second
	consumer.run(ctx, done)
	ingestDone.Wait()

	// Reconciliation against the ground truth.
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	missing := 0
	for seq := uint64(1); seq <= guaranteeActions; seq++ {
		if consumer.received[seq] == 0 {
			missing++
			t.Errorf("seq %d never delivered", seq)
		}
	}
	t.Logf("chaos summary: %d/%d seqs delivered, %d duplicate deliveries, %d sub_seq gap resumes, %d sessions, lastAccepted=%d",
		guaranteeActions-missing, guaranteeActions, dupCount(consumer.received), consumer.subSeqGaps, consumer.sessions, consumer.lastAccepted)
	if missing > 0 {
		t.Fatalf("%d seqs missing: the guarantee does not hold", missing)
	}
}

func dupCount(received map[uint64]int) int {
	d := 0
	for _, n := range received {
		if n > 1 {
			d += n - 1
		}
	}
	return d
}
