package internal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func TestBuildWsActionMessage_CarriesSubSeq(t *testing.T) {
	msg := buildWsActionMessage(StreamedAction{GlobalSeq: 42}, false, nil, 7)
	if msg.SubSeq != 7 {
		t.Fatalf("SubSeq = %d, want 7", msg.SubSeq)
	}
	encoded, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, ok := decoded["sub_seq"].(float64); !ok || got != 7 {
		t.Fatalf("wire sub_seq = %v, want 7", decoded["sub_seq"])
	}
}

func newTestWSServer(t *testing.T) (*StreamWebSocketServer, *ActionBroadcaster, string) {
	t.Helper()
	b := NewActionBroadcaster()
	b.SetLiveMode(true)
	srv := NewStreamServer(b, nil, nil, nil, 10, 30, 4)
	wss := NewStreamWebSocketServer(srv, 10)
	if err := wss.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wss.Close() })
	return wss, b, "ws://" + wss.Addr().String() + "/stream"
}

func dialAndSubscribe(t *testing.T, ctx context.Context, url string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	sub := map[string]any{"type": "subscribe", "contracts": []string{"eosio.token"}}
	if err := wsjson.Write(ctx, conn, sub); err != nil {
		t.Fatal(err)
	}
	return conn
}

func tokenAction(seq uint64) (StreamedAction, []uint64) {
	contract := chain.StringToName("eosio.token")
	return StreamedAction{
		GlobalSeq: seq,
		Contract:  contract,
		Action:    chain.StringToName("transfer"),
		Receiver:  contract,
	}, []uint64{contract}
}

func waitForSubscribers(t *testing.T, b *ActionBroadcaster, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for b.SubscriberCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d subscribers", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readActionSubSeqs(t *testing.T, ctx context.Context, conn *websocket.Conn, n int) []uint64 {
	t.Helper()
	var got []uint64
	for len(got) < n {
		var m map[string]any
		if err := wsjson.Read(ctx, conn, &m); err != nil {
			t.Fatalf("read after %d actions: %v", len(got), err)
		}
		if m["type"] != "action" {
			continue
		}
		ss, ok := m["sub_seq"].(float64)
		if !ok {
			t.Fatalf("action message missing sub_seq: %v", m)
		}
		got = append(got, uint64(ss))
	}
	return got
}

func TestWebSocket_SubSeq_PerConnection(t *testing.T) {
	_, b, url := newTestWSServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := dialAndSubscribe(t, ctx, url)
	defer conn.Close(websocket.StatusNormalClosure, "done")
	waitForSubscribers(t, b, 1)

	for seq := uint64(100); seq <= 102; seq++ {
		act, via := tokenAction(seq)
		b.Broadcast(act, via)
	}
	if got := readActionSubSeqs(t, ctx, conn, 3); got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("sub_seq = %v, want [1 2 3]", got)
	}

	conn2 := dialAndSubscribe(t, ctx, url)
	defer conn2.Close(websocket.StatusNormalClosure, "done")
	waitForSubscribers(t, b, 2)
	act, via := tokenAction(200)
	b.Broadcast(act, via)
	if got := readActionSubSeqs(t, ctx, conn2, 1); got[0] != 1 {
		t.Fatalf("second connection sub_seq = %v, want [1]", got)
	}
}
