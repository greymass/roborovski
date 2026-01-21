package internal

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

type StreamServer struct {
	broadcaster *ActionBroadcaster
	indexes     *Indexes
	reader      corereader.BaseReader
	abiReader   *abicache.Reader

	tcpServer *StreamTCPServer
	wsServer  *StreamWebSocketServer

	maxClients        int
	heartbeatInterval int

	closeChan chan struct{}
	closed    atomic.Bool
	wg        sync.WaitGroup
}

func NewStreamServer(
	broadcaster *ActionBroadcaster,
	indexes *Indexes,
	reader corereader.BaseReader,
	abiReader *abicache.Reader,
	maxClients int,
	heartbeatInterval int,
) *StreamServer {
	return &StreamServer{
		broadcaster:       broadcaster,
		indexes:           indexes,
		reader:            reader,
		abiReader:         abiReader,
		maxClients:        maxClients,
		heartbeatInterval: heartbeatInterval,
		closeChan:         make(chan struct{}),
	}
}

func (s *StreamServer) StartTCP(address string) error {
	s.tcpServer = NewStreamTCPServer(s, s.maxClients)
	if err := s.tcpServer.Listen(address); err != nil {
		return err
	}
	logger.Printf("stream", "TCP server listening on %s", address)
	return nil
}

func (s *StreamServer) StartWebSocket(address string) error {
	s.wsServer = NewStreamWebSocketServer(s, s.maxClients)
	if err := s.wsServer.Listen(address); err != nil {
		return err
	}
	logger.Printf("stream", "WebSocket server listening on %s", address)
	return nil
}

func (s *StreamServer) GetHeartbeatInterval() int {
	return s.heartbeatInterval
}

func (s *StreamServer) StartStatsLogger(interval time.Duration) {
	s.wg.Add(1)
	go s.statsLoop(interval)
}

func (s *StreamServer) statsLoop(interval time.Duration) {
	defer s.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastTotal uint64

	for {
		select {
		case <-s.closeChan:
			return
		case <-ticker.C:
			tcpClients, tcpActions := s.getTCPStats()
			wsClients, wsActions := s.getWSStats()

			totalClients := tcpClients + wsClients
			totalActions := tcpActions + wsActions

			if totalClients > 0 {
				rate := totalActions - lastTotal
				logger.Printf("stream", "Active streams: %d clients (tcp=%d, ws=%d), %d actions sent (+%d since last)",
					totalClients, tcpClients, wsClients, totalActions, rate)
			}

			lastTotal = totalActions
		}
	}
}

func (s *StreamServer) getTCPStats() (clients int, actionsSent uint64) {
	if s.tcpServer == nil {
		return 0, 0
	}

	s.tcpServer.clientsMu.RLock()
	defer s.tcpServer.clientsMu.RUnlock()

	for _, tc := range s.tcpServer.clients {
		clients++
		sent, _ := tc.client.Stats()
		actionsSent += sent
	}

	return clients, actionsSent
}

func (s *StreamServer) getWSStats() (clients int, actionsSent uint64) {
	if s.wsServer == nil {
		return 0, 0
	}

	s.wsServer.clientsMu.RLock()
	defer s.wsServer.clientsMu.RUnlock()

	for _, wc := range s.wsServer.clients {
		clients++
		sent, _ := wc.client.Stats()
		actionsSent += sent
	}

	return clients, actionsSent
}

func (s *StreamServer) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(s.closeChan)

	if s.tcpServer != nil {
		s.tcpServer.Close()
	}

	if s.wsServer != nil {
		s.wsServer.Close()
	}

	s.wg.Wait()
	logger.Printf("stream", "Stream server closed")
	return nil
}
