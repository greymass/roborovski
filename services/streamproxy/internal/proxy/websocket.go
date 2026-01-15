package proxy

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/services/streamproxy/internal/backend"
	"github.com/greymass/roborovski/services/streamproxy/internal/metrics"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type SubscribeMessage struct {
	Type      string   `json:"type"`
	Contracts []string `json:"contracts,omitempty"`
	Receivers []string `json:"receivers,omitempty"`
	StartSeq  uint64   `json:"start_seq,omitempty"`
	Decode    *bool    `json:"decode,omitempty"`
}

type ActionMessage struct {
	Type      string                 `json:"type"`
	GlobalSeq uint64                 `json:"global_seq"`
	BlockNum  uint32                 `json:"block_num"`
	BlockTime uint32                 `json:"block_time"`
	Contract  string                 `json:"contract"`
	Action    string                 `json:"action"`
	Receiver  string                 `json:"receiver"`
	HexData   string                 `json:"hex_data,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type AckMessage struct {
	Type string `json:"type"`
	Seq  uint64 `json:"seq"`
}

type HeartbeatMessage struct {
	Type    string `json:"type"`
	HeadSeq uint64 `json:"head_seq"`
	LibSeq  uint64 `json:"lib_seq"`
}

type CatchupCompleteMessage struct {
	Type    string `json:"type"`
	HeadSeq uint64 `json:"head_seq"`
	LibSeq  uint64 `json:"lib_seq"`
}

type ErrorMessage struct {
	Type    string `json:"type"`
	Code    uint16 `json:"code"`
	Message string `json:"message"`
}

type WSConnection struct {
	ID          uint64
	WS          *websocket.Conn
	BackendConn net.Conn
	StartTime   time.Time
	BytesIn     atomic.Uint64
	BytesOut    atomic.Uint64
	cancel      context.CancelFunc
}

type WebSocketProxy struct {
	backend     *backend.Backend
	maxConns    int
	connTimeout time.Duration

	connections map[uint64]*WSConnection
	connMu      sync.RWMutex
	nextID      atomic.Uint64

	server    *http.Server
	closeChan chan struct{}
	closed    atomic.Bool
	wg        sync.WaitGroup
}

type WebSocketProxyConfig struct {
	MaxConnections    int
	ConnectionTimeout time.Duration
}

func NewWebSocketProxy(be *backend.Backend, cfg WebSocketProxyConfig) *WebSocketProxy {
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 10000
	}
	if cfg.ConnectionTimeout == 0 {
		cfg.ConnectionTimeout = 10 * time.Second
	}

	return &WebSocketProxy{
		backend:     be,
		maxConns:    cfg.MaxConnections,
		connTimeout: cfg.ConnectionTimeout,
		connections: make(map[uint64]*WSConnection),
		closeChan:   make(chan struct{}),
	}
}

func (p *WebSocketProxy) Listen(address string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", p.handleWebSocket)
	mux.HandleFunc("/stream", p.handleWebSocket)

	p.server = &http.Server{
		Addr:         address,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warning("WebSocket server error: %v", err)
		}
	}()

	return nil
}

func (p *WebSocketProxy) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return errors.New("proxy already closed")
	}

	close(p.closeChan)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if p.server != nil {
		p.server.Shutdown(ctx)
	}

	p.connMu.Lock()
	for _, conn := range p.connections {
		conn.cancel()
		conn.BackendConn.Close()
		conn.WS.Close(websocket.StatusGoingAway, "server shutdown")
	}
	p.connMu.Unlock()

	p.wg.Wait()
	return nil
}

func (p *WebSocketProxy) GetConnectionCount() int {
	p.connMu.RLock()
	defer p.connMu.RUnlock()
	return len(p.connections)
}

func (p *WebSocketProxy) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if p.closed.Load() {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}

	p.connMu.RLock()
	connCount := len(p.connections)
	p.connMu.RUnlock()

	if connCount >= p.maxConns {
		http.Error(w, "max connections reached", http.StatusServiceUnavailable)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Warning("WebSocket accept error: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	var subMsg SubscribeMessage
	if err := wsjson.Read(ctx, ws, &subMsg); err != nil {
		cancel()
		logger.Warning("Failed to read subscribe message: %v", err)
		ws.Close(websocket.StatusProtocolError, "invalid subscribe")
		return
	}

	if subMsg.Type != "subscribe" {
		cancel()
		logger.Warning("Expected subscribe message, got: %s", subMsg.Type)
		p.sendWSError(ctx, ws, ActionErrorInvalidRequest, "expected subscribe message")
		ws.Close(websocket.StatusProtocolError, "invalid message type")
		return
	}

	if len(subMsg.Contracts) == 0 && len(subMsg.Receivers) == 0 {
		cancel()
		logger.Warning("Subscribe must specify contracts or receivers")
		p.sendWSError(ctx, ws, ActionErrorInvalidRequest, "must specify contracts or receivers")
		ws.Close(websocket.StatusProtocolError, "no filter")
		return
	}

	if !p.backend.IsHealthy() {
		cancel()
		logger.Warning("Backend unavailable")
		p.sendWSError(ctx, ws, ActionErrorBackendUnavail, "backend unavailable")
		ws.Close(websocket.StatusProtocolError, "backend unavailable")
		metrics.BackendErrors.WithLabelValues("backend").Inc()
		return
	}

	logger.Printf("streamproxy", "WebSocket subscribe request: start_seq=%d contracts=%d receivers=%d",
		subMsg.StartSeq, len(subMsg.Contracts), len(subMsg.Receivers))

	backendConn, err := p.backend.Dial(p.connTimeout)
	if err != nil {
		cancel()
		logger.Warning("Failed to connect to backend: %v", err)
		p.sendWSError(ctx, ws, ActionErrorBackendUnavail, "cannot connect to backend")
		ws.Close(websocket.StatusProtocolError, "backend connect failed")
		metrics.BackendErrors.WithLabelValues("backend").Inc()
		return
	}

	binaryPayload := p.buildBinarySubscribe(subMsg)
	logger.Printf("streamproxy", "Sending binary subscribe to backend: %d bytes, startSeq=%d, payload=%x",
		len(binaryPayload), subMsg.StartSeq, binaryPayload)
	if err := writeMessage(backendConn, MsgTypeActionSubscribe, binaryPayload); err != nil {
		cancel()
		logger.Warning("Failed to forward subscribe to backend: %v", err)
		backendConn.Close()
		ws.Close(websocket.StatusProtocolError, "backend error")
		return
	}
	logger.Printf("streamproxy", "Binary subscribe sent successfully")

	conn := &WSConnection{
		ID:          p.nextID.Add(1),
		WS:          ws,
		BackendConn: backendConn,
		StartTime:   time.Now(),
		cancel:      cancel,
	}

	p.connMu.Lock()
	p.connections[conn.ID] = conn
	connCount = len(p.connections)
	p.connMu.Unlock()

	metrics.ConnectionsActive.WithLabelValues("backend", "websocket").Inc()
	metrics.ConnectionsTotal.WithLabelValues("backend", "websocket").Inc()

	logger.Printf("streamproxy", "WebSocket client %d connected (%d/%d connections)",
		conn.ID, connCount, p.maxConns)

	p.wg.Add(2)
	go p.wsToBackend(ctx, conn)
	go p.backendToWS(ctx, conn)
}

func (p *WebSocketProxy) buildBinarySubscribe(msg SubscribeMessage) []byte {
	contracts := make([]uint64, len(msg.Contracts))
	for i, c := range msg.Contracts {
		contracts[i] = stringToName(c)
	}

	receivers := make([]uint64, len(msg.Receivers))
	for i, r := range msg.Receivers {
		receivers[i] = stringToName(r)
	}

	decode := true
	if msg.Decode != nil {
		decode = *msg.Decode
	}

	payloadSize := 8 + 2 + 2 + 1 + len(contracts)*8 + len(receivers)*8
	payload := make([]byte, payloadSize)

	binary.LittleEndian.PutUint64(payload[0:8], msg.StartSeq)
	binary.LittleEndian.PutUint16(payload[8:10], uint16(len(contracts)))
	binary.LittleEndian.PutUint16(payload[10:12], uint16(len(receivers)))

	var flags uint8
	if decode {
		flags |= 0x01
	}
	payload[12] = flags

	offset := 13
	for _, c := range contracts {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], c)
		offset += 8
	}
	for _, r := range receivers {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], r)
		offset += 8
	}

	return payload
}

func (p *WebSocketProxy) wsToBackend(ctx context.Context, conn *WSConnection) {
	defer p.wg.Done()
	defer func() {
		p.removeWSConnection(conn, "client read loop exit")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.closeChan:
			return
		default:
		}

		var msg json.RawMessage
		if err := wsjson.Read(ctx, conn.WS, &msg); err != nil {
			if !p.closed.Load() {
				if strings.Contains(err.Error(), "close") {
					logger.Printf("streamproxy", "WebSocket client %d read error: %v", conn.ID, err)
				}
			}
			return
		}

		var baseMsg struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg, &baseMsg); err != nil {
			continue
		}

		switch baseMsg.Type {
		case "ack":
			var ack AckMessage
			if err := json.Unmarshal(msg, &ack); err == nil {
				ackPayload := make([]byte, 8)
				binary.LittleEndian.PutUint64(ackPayload, ack.Seq)
				writeMessage(conn.BackendConn, MsgTypeActionAck, ackPayload)
			}
		}
	}
}

func (p *WebSocketProxy) backendToWS(ctx context.Context, conn *WSConnection) {
	defer p.wg.Done()
	defer func() {
		p.removeWSConnection(conn, "backend read loop exit")
	}()

	logger.Printf("streamproxy", "Client %d backendToWS started", conn.ID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.closeChan:
			return
		default:
		}

		msgType, payload, err := readMessage(conn.BackendConn)
		if err != nil {
			if !p.closed.Load() {
				logger.Printf("streamproxy", "Client %d backend read error: %v", conn.ID, err)
			}
			return
		}

		logger.Printf("streamproxy", "Client %d received from backend: type=%d payload=%d bytes", conn.ID, msgType, len(payload))

		switch msgType {
		case MsgTypeActionBatch:
			if len(payload) < 40 {
				continue
			}

			action := ActionMessage{
				Type:      "action",
				GlobalSeq: binary.LittleEndian.Uint64(payload[0:8]),
				BlockNum:  binary.LittleEndian.Uint32(payload[8:12]),
				BlockTime: binary.LittleEndian.Uint32(payload[12:16]),
				Contract:  nameToString(binary.LittleEndian.Uint64(payload[16:24])),
				Action:    nameToString(binary.LittleEndian.Uint64(payload[24:32])),
				Receiver:  nameToString(binary.LittleEndian.Uint64(payload[32:40])),
			}
			if len(payload) > 40 {
				action.HexData = hex.EncodeToString(payload[40:])
			}

			if err := wsjson.Write(ctx, conn.WS, action); err != nil {
				logger.Printf("streamproxy", "WebSocket client %d write error: %v", conn.ID, err)
				return
			}

		case MsgTypeActionDecoded:
			if len(payload) < 40 {
				continue
			}

			action := ActionMessage{
				Type:      "action",
				GlobalSeq: binary.LittleEndian.Uint64(payload[0:8]),
				BlockNum:  binary.LittleEndian.Uint32(payload[8:12]),
				BlockTime: binary.LittleEndian.Uint32(payload[12:16]),
				Contract:  nameToString(binary.LittleEndian.Uint64(payload[16:24])),
				Action:    nameToString(binary.LittleEndian.Uint64(payload[24:32])),
				Receiver:  nameToString(binary.LittleEndian.Uint64(payload[32:40])),
			}
			if len(payload) > 40 {
				var decoded map[string]interface{}
				if err := json.Unmarshal(payload[40:], &decoded); err == nil {
					action.Data = decoded
				} else {
					action.HexData = hex.EncodeToString(payload[40:])
				}
			}

			if err := wsjson.Write(ctx, conn.WS, action); err != nil {
				logger.Printf("streamproxy", "WebSocket client %d write error: %v", conn.ID, err)
				return
			}

		case MsgTypeActionHeartbeat:
			if len(payload) < 16 {
				continue
			}
			headSeq := binary.LittleEndian.Uint64(payload[0:8])
			libSeq := binary.LittleEndian.Uint64(payload[8:16])

			ackPayload := make([]byte, 8)
			binary.LittleEndian.PutUint64(ackPayload, headSeq)
			if err := writeMessage(conn.BackendConn, MsgTypeActionAck, ackPayload); err != nil {
				p.removeWSConnection(conn, "backend write error")
				return
			}

			hb := HeartbeatMessage{
				Type:    "heartbeat",
				HeadSeq: headSeq,
				LibSeq:  libSeq,
			}
			if err := wsjson.Write(ctx, conn.WS, hb); err != nil {
				logger.Printf("streamproxy", "WebSocket client %d write error: %v", conn.ID, err)
				return
			}

		case MsgTypeCatchupComplete:
			if len(payload) < 16 {
				continue
			}
			headSeq := binary.LittleEndian.Uint64(payload[0:8])
			libSeq := binary.LittleEndian.Uint64(payload[8:16])

			msg := CatchupCompleteMessage{
				Type:    "catchup_complete",
				HeadSeq: headSeq,
				LibSeq:  libSeq,
			}
			if err := wsjson.Write(ctx, conn.WS, msg); err != nil {
				logger.Printf("streamproxy", "WebSocket client %d write error: %v", conn.ID, err)
				return
			}

		case MsgTypeActionError:
			if len(payload) < 2 {
				continue
			}
			code := binary.LittleEndian.Uint16(payload[0:2])
			message := ""
			if len(payload) > 2 {
				message = string(payload[2:])
			}

			errMsg := ErrorMessage{
				Type:    "error",
				Code:    code,
				Message: message,
			}
			wsjson.Write(ctx, conn.WS, errMsg)
			return
		}
	}
}

func (p *WebSocketProxy) removeWSConnection(conn *WSConnection, reason string) {
	conn.cancel()

	p.connMu.Lock()
	_, exists := p.connections[conn.ID]
	if exists {
		delete(p.connections, conn.ID)
	}
	connCount := len(p.connections)
	p.connMu.Unlock()

	if !exists {
		return
	}

	conn.BackendConn.Close()
	conn.WS.Close(websocket.StatusNormalClosure, reason)

	duration := time.Since(conn.StartTime)
	metrics.ConnectionsActive.WithLabelValues("backend", "websocket").Dec()
	metrics.ConnectionDuration.WithLabelValues("backend", "websocket").Observe(duration.Seconds())

	logger.Printf("streamproxy", "WebSocket client %d disconnected: %s (duration: %v, %d/%d connections)",
		conn.ID, reason, duration.Round(time.Second), connCount, p.maxConns)
}

func (p *WebSocketProxy) sendWSError(ctx context.Context, ws *websocket.Conn, code uint16, message string) {
	errMsg := ErrorMessage{
		Type:    "error",
		Code:    code,
		Message: message,
	}
	wsjson.Write(ctx, ws, errMsg)
}

const charmap = ".12345abcdefghijklmnopqrstuvwxyz"

func stringToName(s string) uint64 {
	var name uint64
	for i := 0; i < len(s) && i < 12; i++ {
		c := s[i]
		var charIndex uint64
		if c >= 'a' && c <= 'z' {
			charIndex = uint64(c-'a') + 6
		} else if c >= '1' && c <= '5' {
			charIndex = uint64(c-'1') + 1
		} else if c == '.' {
			charIndex = 0
		}

		if i < 12 {
			name |= charIndex << (64 - 5*(i+1))
		}
	}
	return name
}

func nameToString(n uint64) string {
	if n == 0 {
		return ""
	}
	var result strings.Builder
	for i := 0; i < 12; i++ {
		shift := 64 - 5*(i+1)
		charIndex := (n >> shift) & 0x1F
		if charIndex < uint64(len(charmap)) {
			result.WriteByte(charmap[charIndex])
		}
	}
	s := strings.TrimRight(result.String(), ".")
	return s
}
