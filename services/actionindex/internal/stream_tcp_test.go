package internal

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"testing"
)

const tcpTestTrxID = "5b273364b825dfd58e7ac36e4014a24f1547cb5b1786a586af31c5a83daaa03b"

func TestSendActionLayout(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	ts := &StreamTCPServer{}
	action := StreamedAction{
		GlobalSeq:                              42,
		BlockNum:                               7,
		BlockTime:                              1700000000,
		Contract:                               0x1111,
		Action:                                 0x2222,
		Receiver:                               0x3333,
		CpuUsageUs:                             11,
		NetUsageWords:                          22,
		ActionOrdinal:                          3,
		CreatorActionOrdinal:                   1,
		ClosestUnnotifiedAncestorActionOrdinal: 1,
		TrxID:                                  tcpTestTrxID,
		ActionData:                             []byte("abc"),
	}

	go func() {
		_ = ts.sendAction(serverConn, action, false)
		serverConn.Close()
	}()

	header := make([]byte, 5)
	if _, err := io.ReadFull(clientConn, header); err != nil {
		t.Fatalf("read header: %v", err)
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if got := header[4]; got != MsgTypeActionBatch {
		t.Errorf("message type = %#x, want MsgTypeActionBatch (%#x)", got, MsgTypeActionBatch)
	}
	payload := make([]byte, length-1)
	if _, err := io.ReadFull(clientConn, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}

	if len(payload) != 92+3 {
		t.Fatalf("payload len = %d, want 95", len(payload))
	}
	if got := binary.LittleEndian.Uint32(payload[48:52]); got != 3 {
		t.Errorf("ActionOrdinal = %d, want 3", got)
	}
	if got := binary.LittleEndian.Uint32(payload[52:56]); got != 1 {
		t.Errorf("CreatorActionOrdinal = %d, want 1", got)
	}
	if got := binary.LittleEndian.Uint32(payload[56:60]); got != 1 {
		t.Errorf("ClosestUnnotifiedAncestorActionOrdinal = %d, want 1", got)
	}
	if got := hex.EncodeToString(payload[60:92]); got != tcpTestTrxID {
		t.Errorf("trx_id = %s, want %s", got, tcpTestTrxID)
	}
	if string(payload[92:]) != "abc" {
		t.Errorf("action_data = %q, want \"abc\"", payload[92:])
	}
}

func TestSendActionRejectsMissingTrxID(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go func() {
		_, _ = io.Copy(io.Discard, clientConn)
	}()
	defer serverConn.Close()

	ts := &StreamTCPServer{}
	if err := ts.sendAction(serverConn, StreamedAction{GlobalSeq: 42}, false); err == nil {
		t.Fatal("sendAction should refuse an action without a usable trx id")
	}
}
