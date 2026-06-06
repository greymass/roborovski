package internal

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
)

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
	payload := make([]byte, length-1)
	if _, err := io.ReadFull(clientConn, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}

	if len(payload) != 60+3 {
		t.Fatalf("payload len = %d, want 63", len(payload))
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
	if string(payload[60:]) != "abc" {
		t.Errorf("action_data = %q, want \"abc\"", payload[60:])
	}
}
