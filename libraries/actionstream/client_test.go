package actionstream

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	filter := Filter{
		Contracts: []uint64{123, 456},
		Receivers: []uint64{789},
	}
	client := NewClient("localhost:9411", filter, 100, DefaultClientConfig())

	if client.address != "localhost:9411" {
		t.Errorf("expected address localhost:9411, got %s", client.address)
	}
	if len(client.filter.Contracts) != 2 {
		t.Errorf("expected 2 contracts, got %d", len(client.filter.Contracts))
	}
	if client.startSeq != 100 {
		t.Errorf("expected startSeq 100, got %d", client.startSeq)
	}
}

func TestDecodeAction(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	payload := make([]byte, 40)
	binary.LittleEndian.PutUint64(payload[0:8], 12345)
	binary.LittleEndian.PutUint32(payload[8:12], 1000)
	binary.LittleEndian.PutUint32(payload[12:16], 1700000000)
	binary.LittleEndian.PutUint64(payload[16:24], 0x5530EA033C80A555)
	binary.LittleEndian.PutUint64(payload[24:32], 0x00000000A89C6360)
	binary.LittleEndian.PutUint64(payload[32:40], 0x5530EA033C80A555)

	action, err := client.decodeAction(payload)
	if err != nil {
		t.Fatalf("decodeAction failed: %v", err)
	}

	if action.GlobalSeq != 12345 {
		t.Errorf("expected GlobalSeq 12345, got %d", action.GlobalSeq)
	}
	if action.BlockNum != 1000 {
		t.Errorf("expected BlockNum 1000, got %d", action.BlockNum)
	}
	if action.BlockTime != 1700000000 {
		t.Errorf("expected BlockTime 1700000000, got %d", action.BlockTime)
	}
}

func TestDecodeActionWithData(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	payload := make([]byte, 48)
	binary.LittleEndian.PutUint64(payload[0:8], 100)
	binary.LittleEndian.PutUint32(payload[8:12], 500)
	binary.LittleEndian.PutUint32(payload[12:16], 1600000000)
	binary.LittleEndian.PutUint64(payload[16:24], 0)
	binary.LittleEndian.PutUint64(payload[24:32], 0)
	binary.LittleEndian.PutUint64(payload[32:40], 0)
	copy(payload[40:], []byte("testdata"))

	action, err := client.decodeAction(payload)
	if err != nil {
		t.Fatalf("decodeAction failed: %v", err)
	}

	if string(action.ActionData) != "testdata" {
		t.Errorf("expected ActionData 'testdata', got '%s'", string(action.ActionData))
	}
}

func TestDecodeActionTooShort(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	payload := make([]byte, 30)
	_, err := client.decodeAction(payload)
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestSubscribeMessageEncoding(t *testing.T) {
	filter := Filter{
		Contracts: []uint64{111, 222},
		Receivers: []uint64{333},
	}
	client := NewClient("localhost:9411", filter, 5000, DefaultClientConfig())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	received := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		if _, err := conn.Read(header); err != nil {
			return
		}

		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		if _, err := conn.Read(payload); err != nil {
			return
		}

		received <- payload

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)

		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		time.Sleep(100 * time.Millisecond)
	}()

	client.address = serverAddr
	go func() {
		client.dial()
	}()

	select {
	case payload := <-received:
		if len(payload) < 12 {
			t.Fatalf("payload too short: %d", len(payload))
		}

		startFrom := binary.LittleEndian.Uint64(payload[0:8])
		contractCount := binary.LittleEndian.Uint16(payload[8:10])
		receiverCount := binary.LittleEndian.Uint16(payload[10:12])

		if startFrom != 5000 {
			t.Errorf("expected startFrom 5000, got %d", startFrom)
		}
		if contractCount != 2 {
			t.Errorf("expected 2 contracts, got %d", contractCount)
		}
		if receiverCount != 1 {
			t.Errorf("expected 1 receiver, got %d", receiverCount)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for subscribe message")
	}

	client.Close()
}

func TestClientCloseIdempotent(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	err := client.Close()
	if err != nil {
		t.Errorf("first close should succeed: %v", err)
	}

	err = client.Close()
	if err != ErrAlreadyClosed {
		t.Errorf("second close should return ErrAlreadyClosed, got: %v", err)
	}
}

func TestDefaultClientConfig(t *testing.T) {
	config := DefaultClientConfig()

	if config.ReconnectDelay != 1*time.Second {
		t.Errorf("expected ReconnectDelay 1s, got %v", config.ReconnectDelay)
	}
	if config.ReconnectMaxDelay != 30*time.Second {
		t.Errorf("expected ReconnectMaxDelay 30s, got %v", config.ReconnectMaxDelay)
	}
	if config.AckInterval != 1000 {
		t.Errorf("expected AckInterval 1000, got %d", config.AckInterval)
	}
}

func TestDecodeError(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	tests := []struct {
		name     string
		payload  []byte
		expected string
	}{
		{"empty payload", []byte{}, "unknown error"},
		{"single byte", []byte{0x01}, "unknown error"},
		{"code only", []byte{0x01, 0x00}, "error"},
		{"code with message", []byte{0x01, 0x00, 'f', 'a', 'i', 'l', 'e', 'd'}, "failed"},
		{"different error code", []byte{0x02, 0x00, 't', 'e', 's', 't'}, "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.decodeError(tt.payload)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestGetHeadSeq(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	if client.GetHeadSeq() != 0 {
		t.Errorf("expected initial HeadSeq 0, got %d", client.GetHeadSeq())
	}

	client.headSeq.Store(12345)
	if client.GetHeadSeq() != 12345 {
		t.Errorf("expected HeadSeq 12345, got %d", client.GetHeadSeq())
	}
}

func TestGetLIBSeq(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	if client.GetLIBSeq() != 0 {
		t.Errorf("expected initial LIBSeq 0, got %d", client.GetLIBSeq())
	}

	client.libSeq.Store(99999)
	if client.GetLIBSeq() != 99999 {
		t.Errorf("expected LIBSeq 99999, got %d", client.GetLIBSeq())
	}
}

func TestIsConnected(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	if client.IsConnected() {
		t.Error("expected IsConnected false initially")
	}

	client.connected.Store(true)
	if !client.IsConnected() {
		t.Error("expected IsConnected true after setting")
	}
}

func TestNextReturnsErrorWhenClosed(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())
	client.Close()

	_, err := client.Next()
	if err != ErrAlreadyClosed {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestNextWithTimeoutReturnsErrorWhenClosed(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())
	client.Close()

	_, _, err := client.NextWithTimeout(100 * time.Millisecond)
	if err != ErrAlreadyClosed {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestNextWithTimeoutTimesOut(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())
	defer client.Close()

	start := time.Now()
	_, ok, err := client.NextWithTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("expected no error on timeout, got %v", err)
	}
	if ok {
		t.Error("expected ok=false on timeout")
	}
	if elapsed < 50*time.Millisecond {
		t.Error("timeout returned too quickly")
	}
}

func TestNextReceivesAction(t *testing.T) {
	config := DefaultClientConfig()
	config.AckInterval = 0
	client := NewClient("localhost:9411", Filter{}, 0, config)
	defer client.Close()

	testAction := Action{
		GlobalSeq: 1000,
		BlockNum:  500,
		BlockTime: 1700000000,
		Contract:  "eosio.token",
		Action:    "transfer",
		Receiver:  "eosio.token",
	}

	go func() {
		client.recvChan <- testAction
	}()

	action, err := client.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if action.GlobalSeq != 1000 {
		t.Errorf("expected GlobalSeq 1000, got %d", action.GlobalSeq)
	}
	if action.Contract != "eosio.token" {
		t.Errorf("expected Contract eosio.token, got %s", action.Contract)
	}
}

func TestNextWithTimeoutReceivesAction(t *testing.T) {
	config := DefaultClientConfig()
	config.AckInterval = 0
	client := NewClient("localhost:9411", Filter{}, 0, config)
	defer client.Close()

	testAction := Action{
		GlobalSeq: 2000,
		BlockNum:  600,
	}

	go func() {
		client.recvChan <- testAction
	}()

	action, ok, err := client.NextWithTimeout(1 * time.Second)
	if err != nil {
		t.Fatalf("NextWithTimeout failed: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if action.GlobalSeq != 2000 {
		t.Errorf("expected GlobalSeq 2000, got %d", action.GlobalSeq)
	}
}

func TestNextUpdatesCurrentSeq(t *testing.T) {
	config := DefaultClientConfig()
	config.AckInterval = 0
	client := NewClient("localhost:9411", Filter{}, 0, config)
	defer client.Close()

	go func() {
		client.recvChan <- Action{GlobalSeq: 5000}
	}()

	client.Next()

	client.seqMu.Lock()
	currentSeq := client.currentSeq
	client.seqMu.Unlock()

	if currentSeq != 5001 {
		t.Errorf("expected currentSeq 5001, got %d", currentSeq)
	}
}

func TestConnectWhenAlreadyClosed(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())
	client.Close()

	err := client.Connect()
	if err != ErrAlreadyClosed {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestSendAckWithNilConnection(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())
	defer client.Close()

	client.sendAck(1000)
}

func TestSendAckWithConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	received := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)
		received <- payload
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())
	client.conn = conn
	defer client.Close()

	client.sendAck(12345)

	select {
	case payload := <-received:
		if len(payload) != 8 {
			t.Fatalf("expected 8 byte payload, got %d", len(payload))
		}
		seq := binary.LittleEndian.Uint64(payload)
		if seq != 12345 {
			t.Errorf("expected seq 12345, got %d", seq)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestReadMessageTooLarge(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	header := make([]byte, 5)
	tooLarge := uint32(MaxMessageSize + 1)
	binary.BigEndian.PutUint32(header[0:4], tooLarge)
	header[4] = MsgTypeActionBatch

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write(header)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	_, _, err = client.readMessage(conn)
	if err == nil || err.Error() != "message too large" {
		t.Errorf("expected 'message too large' error, got %v", err)
	}
}

func TestReadMessageEmptyPayload(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], 1)
	header[4] = MsgTypeActionHeartbeat

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write(header)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	msgType, payload, err := client.readMessage(conn)
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}
	if msgType != MsgTypeActionHeartbeat {
		t.Errorf("expected MsgTypeActionHeartbeat, got %d", msgType)
	}
	if payload != nil {
		t.Errorf("expected nil payload, got %v", payload)
	}
}

func TestWriteMessageRoundTrip(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	testPayload := []byte("test payload data")
	testMsgType := MsgTypeActionBatch

	received := make(chan struct {
		msgType uint8
		payload []byte
	}, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		msgType, payload, err := client.readMessage(conn)
		if err != nil {
			return
		}
		received <- struct {
			msgType uint8
			payload []byte
		}{msgType, payload}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	err = client.writeMessage(conn, testMsgType, testPayload)
	if err != nil {
		t.Fatalf("writeMessage failed: %v", err)
	}

	select {
	case result := <-received:
		if result.msgType != testMsgType {
			t.Errorf("expected msgType %d, got %d", testMsgType, result.msgType)
		}
		if string(result.payload) != string(testPayload) {
			t.Errorf("payload mismatch")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestDialErrorResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		errPayload := []byte{0x01, 0x00, 's', 'e', 'r', 'v', 'e', 'r', ' ', 'f', 'u', 'l', 'l'}
		errHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(errHeader[0:4], uint32(len(errPayload)+1))
		errHeader[4] = MsgTypeActionError
		conn.Write(errHeader)
		conn.Write(errPayload)
	}()

	client := NewClient(listener.Addr().String(), Filter{}, 0, DefaultClientConfig())
	defer client.Close()

	err = client.dial()
	if err == nil {
		t.Error("expected error from server")
	}
	if err.Error() != "server full" {
		t.Errorf("expected 'server full' error, got %v", err)
	}
}

func TestDialReceivesBatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		actionPayload := make([]byte, 40)
		binary.LittleEndian.PutUint64(actionPayload[0:8], 999)
		binary.LittleEndian.PutUint32(actionPayload[8:12], 100)
		binary.LittleEndian.PutUint32(actionPayload[12:16], 1700000000)

		batchHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(batchHeader[0:4], uint32(len(actionPayload)+1))
		batchHeader[4] = MsgTypeActionBatch
		conn.Write(batchHeader)
		conn.Write(actionPayload)

		time.Sleep(100 * time.Millisecond)
	}()

	client := NewClient(listener.Addr().String(), Filter{}, 0, DefaultClientConfig())
	defer client.Close()

	err = client.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	if !client.IsConnected() {
		t.Error("expected client to be connected")
	}
}

func TestNextTriggersAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	ackReceived := make(chan uint64, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			header := make([]byte, 5)
			if _, err := conn.Read(header); err != nil {
				return
			}
			msgType := header[4]
			length := binary.BigEndian.Uint32(header[0:4])
			payload := make([]byte, length-1)
			if length > 1 {
				conn.Read(payload)
			}

			if msgType == MsgTypeActionAck {
				seq := binary.LittleEndian.Uint64(payload)
				ackReceived <- seq
				return
			}
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	config := DefaultClientConfig()
	config.AckInterval = 10
	client := NewClient("localhost:9411", Filter{}, 0, config)
	client.conn = conn
	defer client.Close()

	for i := uint64(0); i < 15; i++ {
		client.recvChan <- Action{GlobalSeq: i}
	}

	for i := 0; i < 15; i++ {
		client.Next()
	}

	select {
	case seq := <-ackReceived:
		if seq < 10 {
			t.Errorf("expected ack for seq >= 10, got %d", seq)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestNextWithTimeoutTriggersAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	ackReceived := make(chan uint64, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			header := make([]byte, 5)
			if _, err := conn.Read(header); err != nil {
				return
			}
			msgType := header[4]
			length := binary.BigEndian.Uint32(header[0:4])
			payload := make([]byte, length-1)
			if length > 1 {
				conn.Read(payload)
			}

			if msgType == MsgTypeActionAck {
				seq := binary.LittleEndian.Uint64(payload)
				ackReceived <- seq
				return
			}
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	config := DefaultClientConfig()
	config.AckInterval = 5
	client := NewClient("localhost:9411", Filter{}, 0, config)
	client.conn = conn
	defer client.Close()

	for i := uint64(0); i < 10; i++ {
		client.recvChan <- Action{GlobalSeq: i}
	}

	for i := 0; i < 10; i++ {
		client.NextWithTimeout(100 * time.Millisecond)
	}

	select {
	case seq := <-ackReceived:
		if seq < 5 {
			t.Errorf("expected ack for seq >= 5, got %d", seq)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestRecvLoopReceivesActions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	serverReady := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 50000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 49000)
		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		close(serverReady)

		for i := uint64(0); i < 3; i++ {
			actionPayload := make([]byte, 40)
			binary.LittleEndian.PutUint64(actionPayload[0:8], 1000+i)
			binary.LittleEndian.PutUint32(actionPayload[8:12], uint32(100+i))
			binary.LittleEndian.PutUint32(actionPayload[12:16], 1700000000)
			binary.LittleEndian.PutUint64(actionPayload[16:24], 0x5530EA033C80A555)
			binary.LittleEndian.PutUint64(actionPayload[24:32], 0x00000000A89C6360)
			binary.LittleEndian.PutUint64(actionPayload[32:40], 0x5530EA033C80A555)

			batchHeader := make([]byte, 5)
			binary.BigEndian.PutUint32(batchHeader[0:4], uint32(len(actionPayload)+1))
			batchHeader[4] = MsgTypeActionBatch
			conn.Write(batchHeader)
			conn.Write(actionPayload)
		}

		time.Sleep(200 * time.Millisecond)
	}()

	config := DefaultClientConfig()
	config.AckInterval = 0
	client := NewClient(listener.Addr().String(), Filter{}, 0, config)

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	<-serverReady

	for i := uint64(0); i < 3; i++ {
		action, ok, err := client.NextWithTimeout(1 * time.Second)
		if err != nil {
			t.Fatalf("NextWithTimeout failed: %v", err)
		}
		if !ok {
			t.Fatalf("expected action %d, got timeout", i)
		}
		if action.GlobalSeq != 1000+i {
			t.Errorf("expected GlobalSeq %d, got %d", 1000+i, action.GlobalSeq)
		}
	}

	if client.GetHeadSeq() != 50000 {
		t.Errorf("expected HeadSeq 50000, got %d", client.GetHeadSeq())
	}
	if client.GetLIBSeq() != 49000 {
		t.Errorf("expected LIBSeq 49000, got %d", client.GetLIBSeq())
	}
}

func TestRecvLoopHandlesHeartbeat(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		for i := 0; i < 3; i++ {
			time.Sleep(50 * time.Millisecond)
			hbPayload := make([]byte, 16)
			binary.LittleEndian.PutUint64(hbPayload[0:8], uint64(20000+i*1000))
			binary.LittleEndian.PutUint64(hbPayload[8:16], uint64(19000+i*1000))
			hbHeader := make([]byte, 5)
			binary.BigEndian.PutUint32(hbHeader[0:4], 17)
			hbHeader[4] = MsgTypeActionHeartbeat
			conn.Write(hbHeader)
			conn.Write(hbPayload)
		}

		time.Sleep(100 * time.Millisecond)
	}()

	client := NewClient(listener.Addr().String(), Filter{}, 0, DefaultClientConfig())

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(200 * time.Millisecond)

	headSeq := client.GetHeadSeq()
	if headSeq < 20000 {
		t.Errorf("expected HeadSeq >= 20000, got %d", headSeq)
	}
}

func TestRecvLoopReconnectsOnError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	connectionCount := 0
	connectionCountChan := make(chan int, 5)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connectionCount++
			connectionCountChan <- connectionCount

			header := make([]byte, 5)
			conn.Read(header)
			length := binary.BigEndian.Uint32(header[0:4])
			payload := make([]byte, length-1)
			conn.Read(payload)

			hbPayload := make([]byte, 16)
			binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
			binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
			hbHeader := make([]byte, 5)
			binary.BigEndian.PutUint32(hbHeader[0:4], 17)
			hbHeader[4] = MsgTypeActionHeartbeat
			conn.Write(hbHeader)
			conn.Write(hbPayload)

			if connectionCount == 1 {
				conn.Close()
			} else {
				time.Sleep(500 * time.Millisecond)
				conn.Close()
			}
		}
	}()

	config := DefaultClientConfig()
	config.ReconnectDelay = 10 * time.Millisecond
	config.ReconnectMaxDelay = 50 * time.Millisecond
	client := NewClient(listener.Addr().String(), Filter{}, 0, config)

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	timeout := time.After(1 * time.Second)
	reconnected := false
	for !reconnected {
		select {
		case count := <-connectionCountChan:
			if count >= 2 {
				reconnected = true
			}
		case <-timeout:
			t.Fatal("timeout waiting for reconnection")
		}
	}
}

func TestRecvLoopCloseDuringReconnect(t *testing.T) {
	config := DefaultClientConfig()
	config.ReconnectDelay = 50 * time.Millisecond
	client := NewClient("127.0.0.1:59999", Filter{}, 0, config)

	client.wg.Add(1)
	go client.recvLoop()

	time.Sleep(20 * time.Millisecond)

	client.Close()
}

func TestRecvLoopHandlesNilConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		time.Sleep(200 * time.Millisecond)
	}()

	config := DefaultClientConfig()
	config.ReconnectDelay = 10 * time.Millisecond
	client := NewClient(listener.Addr().String(), Filter{}, 0, config)

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	client.connMu.Lock()
	client.conn = nil
	client.connMu.Unlock()

	time.Sleep(50 * time.Millisecond)
	client.Close()
}

func TestRecvLoopServerError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		errPayload := []byte{0x01, 0x00, 'r', 'a', 't', 'e', ' ', 'l', 'i', 'm', 'i', 't'}
		errHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(errHeader[0:4], uint32(len(errPayload)+1))
		errHeader[4] = MsgTypeActionError
		conn.Write(errHeader)
		conn.Write(errPayload)

		time.Sleep(100 * time.Millisecond)
	}()

	client := NewClient(listener.Addr().String(), Filter{}, 0, DefaultClientConfig())

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestDialUnixSocket(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := tmpDir + "/test.sock"

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to create unix listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		time.Sleep(100 * time.Millisecond)
	}()

	client := NewClient(socketPath, Filter{}, 0, DefaultClientConfig())
	defer client.Close()

	err = client.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	if !client.IsConnected() {
		t.Error("expected client to be connected via unix socket")
	}
}

func TestWriteMessageEmptyPayload(t *testing.T) {
	client := NewClient("localhost:9411", Filter{}, 0, DefaultClientConfig())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	received := make(chan struct {
		msgType uint8
		payload []byte
	}, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		msgType, payload, err := client.readMessage(conn)
		if err != nil {
			return
		}
		received <- struct {
			msgType uint8
			payload []byte
		}{msgType, payload}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	err = client.writeMessage(conn, MsgTypeActionHeartbeat, nil)
	if err != nil {
		t.Fatalf("writeMessage failed: %v", err)
	}

	select {
	case result := <-received:
		if result.msgType != MsgTypeActionHeartbeat {
			t.Errorf("expected MsgTypeActionHeartbeat, got %d", result.msgType)
		}
		if result.payload != nil {
			t.Errorf("expected nil payload, got %v", result.payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestRecvLoopDecodeActionError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 5)
		conn.Read(header)
		length := binary.BigEndian.Uint32(header[0:4])
		payload := make([]byte, length-1)
		conn.Read(payload)

		hbPayload := make([]byte, 16)
		binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
		binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
		hbHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(hbHeader[0:4], 17)
		hbHeader[4] = MsgTypeActionHeartbeat
		conn.Write(hbHeader)
		conn.Write(hbPayload)

		shortPayload := make([]byte, 10)
		batchHeader := make([]byte, 5)
		binary.BigEndian.PutUint32(batchHeader[0:4], uint32(len(shortPayload)+1))
		batchHeader[4] = MsgTypeActionBatch
		conn.Write(batchHeader)
		conn.Write(shortPayload)

		time.Sleep(100 * time.Millisecond)
	}()

	config := DefaultClientConfig()
	config.Debug = true
	client := NewClient(listener.Addr().String(), Filter{}, 0, config)

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)
}

func TestConnectDialFailure(t *testing.T) {
	client := NewClient("127.0.0.1:59998", Filter{}, 0, DefaultClientConfig())
	defer client.Close()

	err := client.Connect()
	if err == nil {
		t.Error("expected dial error")
	}
}

func TestRecvLoopDebugLogging(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	serverReady := make(chan struct{})
	connectionCount := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connectionCount++

			header := make([]byte, 5)
			conn.Read(header)
			length := binary.BigEndian.Uint32(header[0:4])
			payload := make([]byte, length-1)
			conn.Read(payload)

			if connectionCount == 1 {
				hbPayload := make([]byte, 16)
				binary.LittleEndian.PutUint64(hbPayload[0:8], 10000)
				binary.LittleEndian.PutUint64(hbPayload[8:16], 9000)
				hbHeader := make([]byte, 5)
				binary.BigEndian.PutUint32(hbHeader[0:4], 17)
				hbHeader[4] = MsgTypeActionHeartbeat
				conn.Write(hbHeader)
				conn.Write(hbPayload)

				close(serverReady)
				conn.Close()
			} else {
				hbPayload := make([]byte, 16)
				binary.LittleEndian.PutUint64(hbPayload[0:8], 20000)
				binary.LittleEndian.PutUint64(hbPayload[8:16], 19000)
				hbHeader := make([]byte, 5)
				binary.BigEndian.PutUint32(hbHeader[0:4], 17)
				hbHeader[4] = MsgTypeActionHeartbeat
				conn.Write(hbHeader)
				conn.Write(hbPayload)

				time.Sleep(200 * time.Millisecond)
				conn.Close()
			}
		}
	}()

	config := DefaultClientConfig()
	config.Debug = true
	config.ReconnectDelay = 10 * time.Millisecond
	config.ReconnectMaxDelay = 50 * time.Millisecond
	client := NewClient(listener.Addr().String(), Filter{}, 0, config)

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	<-serverReady
	time.Sleep(100 * time.Millisecond)
	client.Close()
	listener.Close()
}

func TestRecvLoopReconnectMaxDelayBackoff(t *testing.T) {
	config := DefaultClientConfig()
	config.Debug = true
	config.ReconnectDelay = 5 * time.Millisecond
	config.ReconnectMaxDelay = 10 * time.Millisecond
	client := NewClient("127.0.0.1:59997", Filter{}, 0, config)

	client.wg.Add(1)
	go client.recvLoop()

	time.Sleep(50 * time.Millisecond)

	client.Close()
}
