package internal

import (
	"sync"
	"sync/atomic"

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
