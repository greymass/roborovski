package internal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type StreamTCPServer struct {
	server   *StreamServer
	maxConns int

	listener  net.Listener
	clients   map[uint64]*streamTCPClient
	clientsMu sync.RWMutex
	nextID    atomic.Uint64

	closeChan chan struct{}
	closed    atomic.Bool
	wg        sync.WaitGroup
}

type streamTCPClient struct {
	id     uint64
	conn   net.Conn
	client *StreamClient
}

func NewStreamTCPServer(server *StreamServer, maxConns int) *StreamTCPServer {
	return &StreamTCPServer{
		server:    server,
		maxConns:  maxConns,
		clients:   make(map[uint64]*streamTCPClient),
		closeChan: make(chan struct{}),
	}
}

func (ts *StreamTCPServer) Listen(address string) error {
	var listener net.Listener
	var err error

	if strings.HasSuffix(address, ".sock") {
		os.Remove(address)
		listener, err = net.Listen("unix", address)
	} else {
		listener, err = net.Listen("tcp", address)
	}

	if err != nil {
		return err
	}

	ts.listener = listener
	ts.wg.Add(1)
	go ts.acceptLoop()

	return nil
}

func (ts *StreamTCPServer) acceptLoop() {
	defer ts.wg.Done()

	for {
		conn, err := ts.listener.Accept()
		if err != nil {
			if ts.closed.Load() {
				return
			}
			logger.Warning("TCP accept error: %v", err)
			continue
		}

		ts.clientsMu.RLock()
		connCount := len(ts.clients)
		ts.clientsMu.RUnlock()

		if connCount >= ts.maxConns {
			ts.sendError(conn, ActionErrorMaxClients, "max clients reached")
			conn.Close()
			continue
		}

		go ts.handleConnection(conn)
	}
}

func (ts *StreamTCPServer) handleConnection(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	msgType, payload, err := ts.readMessage(conn)
	if err != nil {
		conn.Close()
		return
	}

	if msgType != MsgTypeActionSubscribe {
		ts.sendError(conn, ActionErrorInvalidRequest, "expected subscribe message")
		conn.Close()
		return
	}

	filter, startSeq, decode, err := ts.decodeSubscribe(payload)
	if err != nil {
		ts.sendError(conn, ActionErrorInvalidRequest, err.Error())
		conn.Close()
		return
	}

	if len(filter.Contracts) == 0 && len(filter.Receivers) == 0 {
		ts.sendError(conn, ActionErrorInvalidRequest, "must specify contracts or receivers")
		conn.Close()
		return
	}

	conn.SetReadDeadline(time.Time{})

	clientID := ts.nextID.Add(1)
	client := NewStreamClient(clientID, ts.server, filter, startSeq, decode)

	tc := &streamTCPClient{
		id:     clientID,
		conn:   conn,
		client: client,
	}

	ts.clientsMu.Lock()
	ts.clients[clientID] = tc
	connCount := len(ts.clients)
	ts.clientsMu.Unlock()

	logger.Printf("stream", "TCP client %d connected (%d/%d)", clientID, connCount, ts.maxConns)

	head, lib := ts.server.broadcaster.GetState()
	if err := ts.sendHeartbeat(conn, head, lib); err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()
		ts.recvLoop(ctx, tc, cancel)
	}()

	err = client.Run(ctx,
		func(action StreamedAction) error {
			return ts.sendAction(conn, action, decode)
		},
		func() error {
			head, lib := ts.server.broadcaster.GetState()
			return ts.sendCatchupComplete(conn, head, lib)
		},
		func(streamErr StreamError) error {
			ts.sendError(conn, streamErr.Code, streamErr.Message)
			return fmt.Errorf("stream error [%d]: %s", streamErr.Code, streamErr.Message)
		},
		func() error {
			head, lib := ts.server.broadcaster.GetState()
			return ts.sendHeartbeat(conn, head, lib)
		},
	)

	ts.removeClient(tc)

	if err != nil && err != context.Canceled {
		logger.Printf("stream", "TCP client %d error: %v", clientID, err)
	}
}

func (ts *StreamTCPServer) recvLoop(ctx context.Context, tc *streamTCPClient, cancel context.CancelFunc) {
	heartbeatInterval := ts.server.GetHeartbeatInterval()
	readTimeout := time.Duration(heartbeatInterval*3) * time.Second
	if readTimeout < 120*time.Second {
		readTimeout = 120 * time.Second
	}

	for {
		tc.conn.SetReadDeadline(time.Now().Add(readTimeout))

		msgType, payload, err := ts.readMessage(tc.conn)
		if err != nil {
			cancel()
			return
		}

		switch msgType {
		case MsgTypeActionAck:
			if len(payload) >= 8 && tc.client.sub != nil {
				seq := binary.LittleEndian.Uint64(payload[0:8])
				tc.client.sub.Ack(seq)
			}
		}
	}
}

func (ts *StreamTCPServer) removeClient(tc *streamTCPClient) {
	ts.clientsMu.Lock()
	delete(ts.clients, tc.id)
	connCount := len(ts.clients)
	ts.clientsMu.Unlock()

	tc.client.Close()
	tc.conn.Close()

	actionsSent, uptime := tc.client.Stats()
	logger.Printf("stream", "TCP client %d disconnected (sent %d actions in %v, %d/%d)",
		tc.id, actionsSent, uptime.Round(time.Second), connCount, ts.maxConns)
}

func (ts *StreamTCPServer) decodeSubscribe(payload []byte) (ActionFilter, uint64, bool, error) {
	if len(payload) < 12 {
		return ActionFilter{}, 0, false, errors.New("subscribe message too short")
	}

	startSeq := binary.LittleEndian.Uint64(payload[0:8])
	contractCount := binary.LittleEndian.Uint16(payload[8:10])
	receiverCount := binary.LittleEndian.Uint16(payload[10:12])

	oldMinLen := 12 + int(contractCount)*8 + int(receiverCount)*8
	oldMinLenWithFlags := 13 + int(contractCount)*8 + int(receiverCount)*8

	filter := ActionFilter{
		Contracts: make(map[uint64]struct{}),
		Receivers: make(map[uint64]struct{}),
		Actions:   make(map[uint64]struct{}),
	}

	var actionCount uint16
	decode := true
	offset := 12

	if len(payload) >= 14 {
		actionCount = binary.LittleEndian.Uint16(payload[12:14])
		newMinLen := 15 + int(contractCount)*8 + int(receiverCount)*8 + int(actionCount)*8

		if len(payload) >= newMinLen {
			flags := payload[14]
			decode = (flags & 0x01) != 0
			offset = 15
		} else if len(payload) >= oldMinLenWithFlags {
			actionCount = 0
			flags := payload[12]
			decode = (flags & 0x01) != 0
			offset = 13
		} else if len(payload) < oldMinLen {
			return ActionFilter{}, 0, false, errors.New("subscribe message truncated")
		}
	} else if len(payload) < oldMinLen {
		return ActionFilter{}, 0, false, errors.New("subscribe message truncated")
	}

	for range contractCount {
		contract := binary.LittleEndian.Uint64(payload[offset : offset+8])
		filter.Contracts[contract] = struct{}{}
		offset += 8
	}

	for range receiverCount {
		receiver := binary.LittleEndian.Uint64(payload[offset : offset+8])
		filter.Receivers[receiver] = struct{}{}
		offset += 8
	}

	for range actionCount {
		action := binary.LittleEndian.Uint64(payload[offset : offset+8])
		filter.Actions[action] = struct{}{}
		offset += 8
	}

	return filter, startSeq, decode, nil
}

func (ts *StreamTCPServer) sendAction(conn net.Conn, action StreamedAction, decode bool) error {
	actionData := action.ActionData
	msgType := MsgTypeActionBatch

	if decode && ts.server.abiReader != nil && len(action.ActionData) > 0 {
		decoded, err := ts.server.abiReader.Decode(
			action.Contract, action.Action, action.ActionData, action.BlockNum)
		if err == nil && decoded != nil {
			if jsonBytes, err := json.Marshal(decoded); err == nil {
				actionData = jsonBytes
				msgType = MsgTypeActionDecoded
			}
		}
	}

	payload := make([]byte, 40+len(actionData))

	binary.LittleEndian.PutUint64(payload[0:8], action.GlobalSeq)
	binary.LittleEndian.PutUint32(payload[8:12], action.BlockNum)
	binary.LittleEndian.PutUint32(payload[12:16], action.BlockTime)
	binary.LittleEndian.PutUint64(payload[16:24], action.Contract)
	binary.LittleEndian.PutUint64(payload[24:32], action.Action)
	binary.LittleEndian.PutUint64(payload[32:40], action.Receiver)
	copy(payload[40:], actionData)

	return ts.writeMessage(conn, msgType, payload)
}

func (ts *StreamTCPServer) sendHeartbeat(conn net.Conn, headSeq, libSeq uint64) error {
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload[0:8], headSeq)
	binary.LittleEndian.PutUint64(payload[8:16], libSeq)
	return ts.writeMessage(conn, MsgTypeActionHeartbeat, payload)
}

func (ts *StreamTCPServer) sendCatchupComplete(conn net.Conn, headSeq, libSeq uint64) error {
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload[0:8], headSeq)
	binary.LittleEndian.PutUint64(payload[8:16], libSeq)
	return ts.writeMessage(conn, MsgTypeCatchupComplete, payload)
}

func (ts *StreamTCPServer) sendError(conn net.Conn, code uint16, message string) {
	payload := make([]byte, 2+len(message))
	binary.LittleEndian.PutUint16(payload[0:2], code)
	copy(payload[2:], message)
	ts.writeMessage(conn, MsgTypeActionError, payload)
}

func (ts *StreamTCPServer) readMessage(r io.Reader) (uint8, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	msgType := header[4]

	if length > MaxStreamMessageSize {
		return 0, nil, errors.New("message too large")
	}

	payloadLen := length - 1
	if payloadLen == 0 {
		return msgType, nil, nil
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}

func (ts *StreamTCPServer) writeMessage(w io.Writer, msgType uint8, payload []byte) error {
	length := uint32(len(payload) + 1)

	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], length)
	header[4] = msgType

	if _, err := w.Write(header); err != nil {
		return err
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	return nil
}

func (ts *StreamTCPServer) Close() error {
	if !ts.closed.CompareAndSwap(false, true) {
		return errors.New("already closed")
	}

	close(ts.closeChan)

	if ts.listener != nil {
		ts.listener.Close()
	}

	ts.clientsMu.Lock()
	for _, tc := range ts.clients {
		tc.client.Close()
		tc.conn.Close()
	}
	ts.clientsMu.Unlock()

	ts.wg.Wait()
	return nil
}
