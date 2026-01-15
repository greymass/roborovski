package proxy

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/services/streamproxy/internal/backend"
	"github.com/greymass/roborovski/services/streamproxy/internal/metrics"
)

const (
	MsgTypeActionSubscribe    uint8 = 0x30
	MsgTypeActionAck          uint8 = 0x31
	MsgTypeActionBatch        uint8 = 0x32
	MsgTypeActionHeartbeat    uint8 = 0x33
	MsgTypeActionError        uint8 = 0x34
	MsgTypeActionDecoded      uint8 = 0x35
	MsgTypeCatchupComplete    uint8 = 0x36

	MaxMessageSize = 10 * 1024 * 1024

	ActionErrorInvalidRequest uint16 = 1
	ActionErrorBackendUnavail uint16 = 2
	ActionErrorMaxClients     uint16 = 3
	ActionErrorNoActions      uint16 = 4
)

type Connection struct {
	ID          uint64
	ClientConn  net.Conn
	BackendConn net.Conn
	StartTime   time.Time
	BytesIn     atomic.Uint64
	BytesOut    atomic.Uint64
}

type TCPProxy struct {
	backend     *backend.Backend
	maxConns    int
	connTimeout time.Duration

	connections map[uint64]*Connection
	connMu      sync.RWMutex
	nextID      atomic.Uint64

	listener  net.Listener
	closeChan chan struct{}
	closed    atomic.Bool
	wg        sync.WaitGroup
}

type TCPProxyConfig struct {
	MaxConnections    int
	ConnectionTimeout time.Duration
}

func NewTCPProxy(be *backend.Backend, cfg TCPProxyConfig) *TCPProxy {
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 10000
	}
	if cfg.ConnectionTimeout == 0 {
		cfg.ConnectionTimeout = 10 * time.Second
	}

	return &TCPProxy{
		backend:     be,
		maxConns:    cfg.MaxConnections,
		connTimeout: cfg.ConnectionTimeout,
		connections: make(map[uint64]*Connection),
		closeChan:   make(chan struct{}),
	}
}

func (p *TCPProxy) Listen(address string) error {
	var listener net.Listener
	var err error

	if strings.HasSuffix(address, ".sock") {
		listener, err = net.Listen("unix", address)
	} else {
		listener, err = net.Listen("tcp", address)
	}

	if err != nil {
		return err
	}

	p.listener = listener
	p.wg.Add(1)
	go p.acceptLoop()

	return nil
}

func (p *TCPProxy) acceptLoop() {
	defer p.wg.Done()

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if p.closed.Load() {
				return
			}
			logger.Warning("TCP accept error: %v", err)
			continue
		}

		p.connMu.RLock()
		connCount := len(p.connections)
		p.connMu.RUnlock()

		if connCount >= p.maxConns {
			logger.Warning("Max connections reached (%d)", p.maxConns)
			p.sendError(conn, ActionErrorMaxClients, "max connections reached")
			conn.Close()
			metrics.ConnectionsRejected.Inc()
			continue
		}

		go p.handleConnection(conn)
	}
}

func (p *TCPProxy) handleConnection(clientConn net.Conn) {
	clientConn.SetReadDeadline(time.Now().Add(p.connTimeout))

	msgType, payload, err := readMessage(clientConn)
	if err != nil {
		logger.Warning("Failed to read subscribe: %v", err)
		clientConn.Close()
		return
	}

	if msgType != MsgTypeActionSubscribe {
		logger.Warning("Expected subscribe, got type %d", msgType)
		p.sendError(clientConn, ActionErrorInvalidRequest, "expected subscribe message")
		clientConn.Close()
		return
	}

	subInfo, err := p.parseSubscribe(payload)
	if err != nil {
		logger.Warning("Failed to parse subscribe: %v", err)
		p.sendError(clientConn, ActionErrorInvalidRequest, err.Error())
		clientConn.Close()
		return
	}

	logger.Printf("streamproxy", "Subscribe request: start_seq=%d contracts=%d receivers=%d",
		subInfo.startSeq, subInfo.contractCount, subInfo.receiverCount)

	if !p.backend.IsHealthy() {
		logger.Warning("Backend unavailable")
		p.sendError(clientConn, ActionErrorBackendUnavail, "backend unavailable")
		clientConn.Close()
		metrics.BackendErrors.WithLabelValues("backend").Inc()
		return
	}

	backendConn, err := p.backend.Dial(p.connTimeout)
	if err != nil {
		logger.Warning("Failed to connect to backend: %v", err)
		p.sendError(clientConn, ActionErrorBackendUnavail, "cannot connect to backend")
		clientConn.Close()
		metrics.BackendErrors.WithLabelValues("backend").Inc()
		return
	}

	if err := writeMessage(backendConn, MsgTypeActionSubscribe, payload); err != nil {
		logger.Warning("Failed to forward subscribe: %v", err)
		clientConn.Close()
		backendConn.Close()
		return
	}

	clientConn.SetReadDeadline(time.Time{})

	conn := &Connection{
		ID:          p.nextID.Add(1),
		ClientConn:  clientConn,
		BackendConn: backendConn,
		StartTime:   time.Now(),
	}

	p.connMu.Lock()
	p.connections[conn.ID] = conn
	connCount := len(p.connections)
	p.connMu.Unlock()

	metrics.ConnectionsActive.WithLabelValues("backend", "tcp").Inc()
	metrics.ConnectionsTotal.WithLabelValues("backend", "tcp").Inc()

	logger.Printf("streamproxy", "Client %d connected (%d/%d connections)",
		conn.ID, connCount, p.maxConns)

	p.wg.Add(2)
	go p.proxyClientToBackend(conn)
	go p.proxyBackendToClient(conn)
}

func (p *TCPProxy) proxyClientToBackend(conn *Connection) {
	defer p.wg.Done()

	buf := make([]byte, 32*1024)
	for {
		select {
		case <-p.closeChan:
			return
		default:
		}

		conn.ClientConn.SetReadDeadline(time.Now().Add(60 * time.Second))
		n, err := conn.ClientConn.Read(buf)
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed") {
				if !strings.Contains(err.Error(), "timeout") {
					logger.Printf("streamproxy", "Client %d read error: %v", conn.ID, err)
				}
			}
			p.removeConnection(conn, "client disconnected")
			return
		}

		conn.BytesIn.Add(uint64(n))
		metrics.BytesTotal.WithLabelValues("backend", "in").Add(float64(n))

		if _, err := conn.BackendConn.Write(buf[:n]); err != nil {
			logger.Printf("streamproxy", "Client %d backend write error: %v", conn.ID, err)
			p.removeConnection(conn, "backend write error")
			return
		}
	}
}

func (p *TCPProxy) proxyBackendToClient(conn *Connection) {
	defer p.wg.Done()

	buf := make([]byte, 32*1024)
	for {
		select {
		case <-p.closeChan:
			return
		default:
		}

		conn.BackendConn.SetReadDeadline(time.Now().Add(60 * time.Second))
		n, err := conn.BackendConn.Read(buf)
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed") {
				logger.Printf("streamproxy", "Client %d backend read error: %v", conn.ID, err)
			}
			p.removeConnection(conn, "backend disconnected")
			return
		}

		conn.BytesOut.Add(uint64(n))
		metrics.BytesTotal.WithLabelValues("backend", "out").Add(float64(n))

		if _, err := conn.ClientConn.Write(buf[:n]); err != nil {
			logger.Printf("streamproxy", "Client %d client write error: %v", conn.ID, err)
			p.removeConnection(conn, "client write error")
			return
		}
	}
}

func (p *TCPProxy) removeConnection(conn *Connection, reason string) {
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

	conn.ClientConn.Close()
	conn.BackendConn.Close()

	duration := time.Since(conn.StartTime)
	metrics.ConnectionsActive.WithLabelValues("backend", "tcp").Dec()
	metrics.ConnectionDuration.WithLabelValues("backend", "tcp").Observe(duration.Seconds())

	logger.Printf("streamproxy", "Client %d disconnected: %s (duration: %v, in: %d, out: %d, %d/%d connections)",
		conn.ID, reason, duration.Round(time.Second),
		conn.BytesIn.Load(), conn.BytesOut.Load(),
		connCount, p.maxConns)
}

type subscribeInfo struct {
	startSeq      uint64
	contractCount int
	receiverCount int
}

func (p *TCPProxy) parseSubscribe(payload []byte) (subscribeInfo, error) {
	if len(payload) < 12 {
		return subscribeInfo{}, errors.New("subscribe message too short")
	}

	info := subscribeInfo{
		startSeq:      binary.LittleEndian.Uint64(payload[0:8]),
		contractCount: int(binary.LittleEndian.Uint16(payload[8:10])),
		receiverCount: int(binary.LittleEndian.Uint16(payload[10:12])),
	}

	expectedLen := 12 + info.contractCount*8 + info.receiverCount*8
	if len(payload) < expectedLen {
		return subscribeInfo{}, errors.New("subscribe message truncated")
	}

	return info, nil
}

func (p *TCPProxy) sendError(conn net.Conn, code uint16, message string) {
	payload := make([]byte, 2+len(message))
	binary.LittleEndian.PutUint16(payload[0:2], code)
	copy(payload[2:], message)
	writeMessage(conn, MsgTypeActionError, payload)
}

func (p *TCPProxy) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return errors.New("proxy already closed")
	}

	close(p.closeChan)

	if p.listener != nil {
		p.listener.Close()
	}

	p.connMu.Lock()
	for _, conn := range p.connections {
		conn.ClientConn.Close()
		conn.BackendConn.Close()
	}
	p.connMu.Unlock()

	p.wg.Wait()
	return nil
}

func (p *TCPProxy) GetConnectionCount() int {
	p.connMu.RLock()
	defer p.connMu.RUnlock()
	return len(p.connections)
}

func readMessage(r io.Reader) (uint8, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	msgType := header[4]

	if length > MaxMessageSize {
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

func writeMessage(w io.Writer, msgType uint8, payload []byte) error {
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
