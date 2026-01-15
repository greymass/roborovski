package corestream

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type BlockProvider interface {
	GetBlock(blockNum uint32) ([]byte, error)
	GetBlockByGlob(glob uint64) (uint32, error)
	GetActionsByGlobs(globalSeqs []uint64) ([]ActionData, error)
	GetLIB() uint32
	GetHead() uint32
}

type ServerConfig struct {
	MaxClients        int
	HeartbeatInterval time.Duration
	Debug             bool
}

type streamClient struct {
	id               uint64
	conn             net.Conn
	startBlock       uint32
	lastAck          uint32
	sendChan         chan *BlockMessage
	closeChan        chan struct{}
	closed           atomic.Bool
	mu               sync.Mutex
	blocksSent       atomic.Uint64
	connectTime      time.Time
	disconnectReason string
}

type Server struct {
	config    ServerConfig
	provider  BlockProvider
	listeners []net.Listener

	clients   map[uint64]*streamClient
	clientsMu sync.RWMutex
	nextID    uint64

	broadcastChan   chan *BlockMessage
	closeChan       chan struct{}
	closed          atomic.Bool
	liveMode        atomic.Bool
	wg              sync.WaitGroup
	totalBroadcasts atomic.Uint64
}

func NewServer(provider BlockProvider, config ServerConfig) *Server {
	return &Server{
		config:        config,
		provider:      provider,
		clients:       make(map[uint64]*streamClient),
		listeners:     make([]net.Listener, 0),
		broadcastChan: make(chan *BlockMessage, 1000),
		closeChan:     make(chan struct{}),
	}
}

func (s *Server) Listen(address string) error {
	var listener net.Listener
	var err error

	if strings.HasSuffix(address, ".sock") {
		os.Remove(address)
		listener, err = net.Listen("unix", address)
		if err != nil {
			return err
		}
		os.Chmod(address, 0777)
	} else {
		listener, err = net.Listen("tcp", address)
		if err != nil {
			return err
		}
	}

	s.listeners = append(s.listeners, listener)

	if len(s.listeners) == 1 {
		s.wg.Add(1)
		go s.broadcastLoop()
	}

	s.wg.Add(1)
	go s.acceptLoop(listener)

	return nil
}

func (s *Server) acceptLoop(listener net.Listener) {
	defer s.wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			logger.Warning("Accept error: %v", err)
			continue
		}

		if !s.liveMode.Load() {
			logger.Printf("stream", "Rejecting connection: server is syncing")
			errMsg := &ErrorMessage{Code: ErrorCodeServerSyncing, Message: "server is syncing, streaming not available"}
			WriteMessage(conn, MsgTypeError, EncodeErrorMessage(errMsg))
			conn.Close()
			continue
		}

		s.clientsMu.RLock()
		clientCount := len(s.clients)
		s.clientsMu.RUnlock()

		if clientCount >= s.config.MaxClients {
			logger.Warning("Max clients reached (%d), rejecting connection", s.config.MaxClients)
			errMsg := &ErrorMessage{Code: ErrorCodeMaxClientsReach, Message: "max clients reached"}
			WriteMessage(conn, MsgTypeError, EncodeErrorMessage(errMsg))
			conn.Close()
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	msgType, payload, err := ReadMessage(conn)
	if err != nil {
		logger.Warning("Failed to read initial message: %v", err)
		conn.Close()
		return
	}

	if msgType != MsgTypeRequest {
		logger.Warning("Expected request message, got type %d", msgType)
		errMsg := &ErrorMessage{Code: ErrorCodeInvalidRequest, Message: "expected request message"}
		WriteMessage(conn, MsgTypeError, EncodeErrorMessage(errMsg))
		conn.Close()
		return
	}

	req, err := DecodeRequestMessage(payload)
	if err != nil {
		logger.Warning("Failed to decode request: %v", err)
		errMsg := &ErrorMessage{Code: ErrorCodeInvalidRequest, Message: err.Error()}
		WriteMessage(conn, MsgTypeError, EncodeErrorMessage(errMsg))
		conn.Close()
		return
	}

	lib := s.provider.GetLIB()
	if req.StartBlock > lib {
		logger.Warning("Requested start block %d is ahead of LIB %d", req.StartBlock, lib)
		errMsg := &ErrorMessage{Code: ErrorCodeBlockNotFound, Message: "start block ahead of LIB"}
		WriteMessage(conn, MsgTypeError, EncodeErrorMessage(errMsg))
		conn.Close()
		return
	}

	_, err = s.provider.GetBlock(req.StartBlock)
	if err != nil {
		logger.Warning("Requested start block %d not found: %v", req.StartBlock, err)
		errMsg := &ErrorMessage{Code: ErrorCodeBlockNotFound, Message: "start block not found in storage"}
		WriteMessage(conn, MsgTypeError, EncodeErrorMessage(errMsg))
		conn.Close()
		return
	}

	conn.SetReadDeadline(time.Time{})

	client := &streamClient{
		id:          atomic.AddUint64(&s.nextID, 1),
		conn:        conn,
		startBlock:  req.StartBlock,
		lastAck:     req.StartBlock - 1,
		sendChan:    make(chan *BlockMessage, 100),
		closeChan:   make(chan struct{}),
		connectTime: time.Now(),
	}

	s.clientsMu.Lock()
	s.clients[client.id] = client
	clientCount := len(s.clients)
	s.clientsMu.Unlock()

	logger.Printf("stream", "Client %d connected from block %d (%d/%d clients)",
		client.id, req.StartBlock, clientCount, s.config.MaxClients)

	s.wg.Add(3)
	go s.clientSendLoop(client)
	go s.clientRecvLoop(client)
	go s.clientCatchupLoop(client)
}

func (s *Server) clientSendLoop(client *streamClient) {
	defer s.wg.Done()
	defer func() {
		s.removeClient(client, "send loop exit")
	}()

	heartbeatTicker := time.NewTicker(s.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-s.closeChan:
			client.disconnectReason = "server shutdown"
			return
		case <-client.closeChan:
			return
		case <-heartbeatTicker.C:
			hb := &HeartbeatMessage{
				LIB:  s.provider.GetLIB(),
				HEAD: s.provider.GetHead(),
			}
			err := WriteMessage(client.conn, MsgTypeHeartbeat, EncodeHeartbeatMessage(hb))
			if err != nil {
				client.disconnectReason = fmt.Sprintf("heartbeat write error: %v", err)
				return
			}
		case msg := <-client.sendChan:
			err := WriteMessage(client.conn, MsgTypeBlock, EncodeBlockMessage(msg))
			if err != nil {
				client.disconnectReason = fmt.Sprintf("block write error: %v", err)
				return
			}
			client.blocksSent.Add(1)
		}
	}
}

func (s *Server) clientRecvLoop(client *streamClient) {
	defer s.wg.Done()

	for {
		client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		msgType, payload, err := ReadMessage(client.conn)
		if err != nil {
			if !client.closed.Load() {
				if strings.Contains(err.Error(), "use of closed") {
					client.disconnectReason = "connection closed"
				} else if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
					client.disconnectReason = "read timeout (60s)"
				} else if strings.Contains(err.Error(), "EOF") {
					client.disconnectReason = "client closed connection"
				} else {
					client.disconnectReason = fmt.Sprintf("read error: %v", err)
				}
			}
			client.closeOnce()
			return
		}

		switch msgType {
		case MsgTypeAck:
			ack, err := DecodeAckMessage(payload)
			if err != nil {
				continue
			}
			client.mu.Lock()
			if ack.BlockNum > client.lastAck {
				client.lastAck = ack.BlockNum
			}
			client.mu.Unlock()

		case MsgTypeQueryState:
			s.handleQueryState(client, payload)

		case MsgTypeQueryBlock:
			s.handleQueryBlock(client, payload)

		case MsgTypeQueryBlockBatch:
			s.handleQueryBlockBatch(client, payload)

		case MsgTypeQueryGlobs:
			s.handleQueryGlobs(client, payload)
		}
	}
}

func (s *Server) clientCatchupLoop(client *streamClient) {
	defer s.wg.Done()

	currentBlock := client.startBlock
	lib := s.provider.GetLIB()
	catchupStart := time.Now()

	for currentBlock <= lib {
		select {
		case <-s.closeChan:
			return
		case <-client.closeChan:
			return
		default:
		}

		data, err := s.provider.GetBlock(currentBlock)
		if err != nil {
			if s.config.Debug {
				logger.Printf("stream", "Client %d catchup: block %d error: %v", client.id, currentBlock, err)
			}
			time.Sleep(100 * time.Millisecond)
			lib = s.provider.GetLIB()
			continue
		}

		msg := &BlockMessage{
			BlockNum: currentBlock,
			LIB:      s.provider.GetLIB(),
			HEAD:     s.provider.GetHead(),
			Data:     data,
		}

		select {
		case <-s.closeChan:
			return
		case <-client.closeChan:
			return
		case client.sendChan <- msg:
			currentBlock++
			if currentBlock%10000 == 0 {
				logger.Printf("stream", "Client %d catchup progress: block %d / %d", client.id, currentBlock, lib)
			}
		}

		lib = s.provider.GetLIB()
	}

	catchupDuration := time.Since(catchupStart).Round(time.Second)
	blocksCaughtUp := currentBlock - client.startBlock
	var bps float64
	if catchupDuration.Seconds() > 0 {
		bps = float64(blocksCaughtUp) / catchupDuration.Seconds()
	}
	logger.Printf("stream", "Client %d catchup complete: %d blocks in %v (%.0f BPS), now in live mode",
		client.id, blocksCaughtUp, catchupDuration, bps)
}

func (s *Server) broadcastLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.closeChan:
			return
		case msg := <-s.broadcastChan:
			s.clientsMu.RLock()
			clientCount := len(s.clients)
			if clientCount == 0 {
				s.clientsMu.RUnlock()
				continue
			}
			sentCount := 0
			for _, client := range s.clients {
				client.mu.Lock()
				lastAck := client.lastAck
				startBlock := client.startBlock
				shouldSend := msg.BlockNum > lastAck && msg.BlockNum > startBlock-1
				client.mu.Unlock()

				if shouldSend {
					select {
					case client.sendChan <- msg:
						sentCount++
					default:
						logger.Printf("stream", "Client %d send buffer full, skipping block %d", client.id, msg.BlockNum)
					}
				} else if s.config.Debug {
					logger.Printf("stream", "Client %d: block %d skipped (lastAck=%d, startBlock=%d)", client.id, msg.BlockNum, lastAck, startBlock)
				}
			}
			s.clientsMu.RUnlock()
			if s.config.Debug && sentCount > 0 {
				logger.Printf("stream", "Broadcast block %d to %d/%d clients", msg.BlockNum, sentCount, clientCount)
			}
		}
	}
}

func (s *Server) Broadcast(blockNum uint32, data []byte) {
	if s.closed.Load() {
		return
	}
	if !s.liveMode.Load() {
		return
	}

	msg := &BlockMessage{
		BlockNum: blockNum,
		LIB:      s.provider.GetLIB(),
		HEAD:     s.provider.GetHead(),
		Data:     data,
	}

	select {
	case s.broadcastChan <- msg:
		s.totalBroadcasts.Add(1)
	default:
		logger.Printf("stream", "Broadcast dropped: channel full for block %d", blockNum)
	}
}

func (s *Server) removeClient(client *streamClient, reason string) {
	client.closeOnce()

	s.clientsMu.Lock()
	delete(s.clients, client.id)
	clientCount := len(s.clients)
	s.clientsMu.Unlock()

	client.conn.Close()

	finalReason := client.disconnectReason
	if finalReason == "" {
		finalReason = reason
	}

	uptime := time.Since(client.connectTime).Round(time.Second)
	blocksSent := client.blocksSent.Load()
	var bps float64
	if uptime.Seconds() > 0 {
		bps = float64(blocksSent) / uptime.Seconds()
	}

	logger.Printf("stream", "Client %d disconnected: %s (sent %d blocks in %v, %.0f BPS, %d/%d clients remain)",
		client.id, finalReason, blocksSent, uptime, bps, clientCount, s.config.MaxClients)
}

func (c *streamClient) closeOnce() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeChan)
	}
}

func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return errors.New("server already closed")
	}

	close(s.closeChan)

	for _, listener := range s.listeners {
		listener.Close()
	}

	s.clientsMu.Lock()
	for _, client := range s.clients {
		client.closeOnce()
		client.conn.Close()
	}
	s.clientsMu.Unlock()

	s.wg.Wait()
	return nil
}

func (s *Server) SetLiveMode(live bool) {
	wasLive := s.liveMode.Swap(live)
	if live && !wasLive {
		logger.Printf("stream", "Streaming enabled (live mode)")
	} else if !live && wasLive {
		logger.Printf("stream", "Streaming disabled (syncing)")
	}
}

func (s *Server) IsLiveMode() bool {
	return s.liveMode.Load()
}

type ClientStats struct {
	ID          uint64
	StartBlock  uint32
	LastAck     uint32
	BlocksSent  uint64
	ConnectTime time.Time
	Uptime      time.Duration
}

type ServerStats struct {
	ClientCount     int
	TotalBroadcasts uint64
	Clients         []ClientStats
}

func (s *Server) GetStats() ServerStats {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	stats := ServerStats{
		ClientCount:     len(s.clients),
		TotalBroadcasts: s.totalBroadcasts.Load(),
		Clients:         make([]ClientStats, 0, len(s.clients)),
	}

	for _, client := range s.clients {
		client.mu.Lock()
		cs := ClientStats{
			ID:          client.id,
			StartBlock:  client.startBlock,
			LastAck:     client.lastAck,
			BlocksSent:  client.blocksSent.Load(),
			ConnectTime: client.connectTime,
			Uptime:      time.Since(client.connectTime),
		}
		client.mu.Unlock()
		stats.Clients = append(stats.Clients, cs)
	}

	return stats
}

func (s *Server) LogStats() {
	stats := s.GetStats()
	if stats.ClientCount == 0 {
		return
	}

	logger.Printf("stream", "Stream server: %d clients, %d total broadcasts",
		stats.ClientCount, stats.TotalBroadcasts)

	for _, client := range stats.Clients {
		bps := float64(client.BlocksSent) / client.Uptime.Seconds()
		behind := int64(client.LastAck) - int64(client.StartBlock)
		logger.Printf("stream", "  Client %d: sent=%d | lastAck=%d | behind=%d | %.1f BPS | uptime=%v",
			client.ID, client.BlocksSent, client.LastAck, behind, bps, client.Uptime.Round(time.Second))
	}
}

func (s *Server) handleQueryState(client *streamClient, payload []byte) {
	req, err := DecodeQueryStateMessage(payload)
	if err != nil {
		s.sendQueryError(client, 0, ErrorCodeInvalidRequest, "invalid query state message")
		return
	}

	resp := &QueryStateResponseMessage{
		RequestID: req.RequestID,
		HEAD:      s.provider.GetHead(),
		LIB:       s.provider.GetLIB(),
	}

	WriteMessage(client.conn, MsgTypeQueryStateResponse, EncodeQueryStateResponseMessage(resp))
}

func (s *Server) handleQueryBlock(client *streamClient, payload []byte) {
	req, err := DecodeQueryBlockMessage(payload)
	if err != nil {
		s.sendQueryError(client, 0, ErrorCodeInvalidRequest, "invalid query block message")
		return
	}

	blockData, err := s.provider.GetBlock(req.BlockNum)
	if err != nil {
		s.sendQueryError(client, req.RequestID, ErrorCodeBlockNotFound, fmt.Sprintf("block %d not found", req.BlockNum))
		return
	}

	resp := &QueryBlockResponseMessage{
		RequestID: req.RequestID,
		BlockNum:  req.BlockNum,
		Data:      blockData,
	}

	WriteMessage(client.conn, MsgTypeQueryBlockResponse, EncodeQueryBlockResponseMessage(resp))
}

func (s *Server) handleQueryBlockBatch(client *streamClient, payload []byte) {
	req, err := DecodeQueryBlockBatchMessage(payload)
	if err != nil {
		s.sendQueryError(client, 0, ErrorCodeInvalidRequest, "invalid query block batch message")
		return
	}

	if req.EndBlock < req.StartBlock {
		s.sendQueryError(client, req.RequestID, ErrorCodeInvalidRequest, "end block must be >= start block")
		return
	}

	totalBlocks := req.EndBlock - req.StartBlock + 1

	startResp := &QueryBlockBatchStartMessage{
		RequestID:   req.RequestID,
		TotalBlocks: totalBlocks,
	}
	if err := WriteMessage(client.conn, MsgTypeQueryBlockBatchStart, EncodeQueryBlockBatchStartMessage(startResp)); err != nil {
		return
	}

	for blockNum := req.StartBlock; blockNum <= req.EndBlock; blockNum++ {
		blockData, err := s.provider.GetBlock(blockNum)
		if err != nil {
			s.sendQueryError(client, req.RequestID, ErrorCodeBlockNotFound, fmt.Sprintf("block %d not found", blockNum))
			return
		}

		blockResp := &QueryBlockBatchBlockMessage{
			RequestID: req.RequestID,
			BlockNum:  blockNum,
			Data:      blockData,
		}
		if err := WriteMessage(client.conn, MsgTypeQueryBlockBatchBlock, EncodeQueryBlockBatchBlockMessage(blockResp)); err != nil {
			return
		}
	}

	completeResp := &QueryBlockBatchCompleteMessage{
		RequestID: req.RequestID,
	}
	WriteMessage(client.conn, MsgTypeQueryBlockBatchComplete, EncodeQueryBlockBatchCompleteMessage(completeResp))
}

func (s *Server) handleQueryGlobs(client *streamClient, payload []byte) {
	req, err := DecodeQueryGlobsMessage(payload)
	if err != nil {
		s.sendQueryError(client, 0, ErrorCodeInvalidRequest, "invalid query globs message")
		return
	}

	if len(req.GlobalSeqs) > 1000 {
		s.sendQueryError(client, req.RequestID, ErrorCodeTooManyGlobs, "max 1000 global seqs per query")
		return
	}

	if len(req.GlobalSeqs) == 0 {
		resp := &QueryGlobsResponseMessage{
			RequestID: req.RequestID,
			LIB:       s.provider.GetLIB(),
			HEAD:      s.provider.GetHead(),
			Actions:   []ActionData{},
		}
		WriteMessage(client.conn, MsgTypeQueryGlobsResponse, EncodeQueryGlobsResponseMessage(resp))
		return
	}

	actions, err := s.provider.GetActionsByGlobs(req.GlobalSeqs)
	if err != nil {
		s.sendQueryError(client, req.RequestID, ErrorCodeBlockNotFound, err.Error())
		return
	}

	resp := &QueryGlobsResponseMessage{
		RequestID: req.RequestID,
		LIB:       s.provider.GetLIB(),
		HEAD:      s.provider.GetHead(),
		Actions:   actions,
	}

	WriteMessage(client.conn, MsgTypeQueryGlobsResponse, EncodeQueryGlobsResponseMessage(resp))
}

func (s *Server) sendQueryError(client *streamClient, requestID uint64, code uint16, message string) {
	errMsg := &QueryErrorMessage{
		RequestID: requestID,
		Code:      code,
		Message:   message,
	}
	WriteMessage(client.conn, MsgTypeQueryError, EncodeQueryErrorMessage(errMsg))
}
