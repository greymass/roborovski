package corestream

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

var (
	ErrNotConnected  = errors.New("not connected")
	ErrAlreadyClosed = errors.New("client already closed")
)

type ClientConfig struct {
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	AckInterval       uint32
	Debug             bool
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		ReconnectDelay:    1 * time.Second,
		ReconnectMaxDelay: 30 * time.Second,
		AckInterval:       1000,
		Debug:             false,
	}
}

type Client struct {
	config  ClientConfig
	address string

	conn         net.Conn
	connMu       sync.Mutex
	connected    atomic.Bool
	currentBlock uint32
	lastAcked    uint32

	lib  atomic.Uint32
	head atomic.Uint32

	recvChan  chan *BlockMessage
	closeChan chan struct{}
	closed    atomic.Bool
	wg        sync.WaitGroup

	queryID        atomic.Uint64
	pendingQueries sync.Map
	pendingBatches sync.Map
}

type batchCollector struct {
	blocks   [][]byte
	expected uint32
	received uint32
	doneChan chan error
	mu       sync.Mutex
}

func NewClient(address string, config ClientConfig) *Client {
	return &Client{
		config:    config,
		address:   address,
		recvChan:  make(chan *BlockMessage, 100),
		closeChan: make(chan struct{}),
	}
}

func (c *Client) Connect(startBlock uint32) error {
	if c.closed.Load() {
		return ErrAlreadyClosed
	}

	c.currentBlock = startBlock
	c.lastAcked = startBlock - 1

	if err := c.dial(); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.recvLoop()

	return nil
}

func (c *Client) dial() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	var conn net.Conn
	var err error

	if strings.HasSuffix(c.address, ".sock") {
		conn, err = net.Dial("unix", c.address)
	} else {
		conn, err = net.DialTimeout("tcp", c.address, 10*time.Second)
	}

	if err != nil {
		return err
	}

	req := &RequestMessage{StartBlock: c.currentBlock}
	if err := WriteMessage(conn, MsgTypeRequest, EncodeRequestMessage(req)); err != nil {
		conn.Close()
		return err
	}

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	msgType, payload, err := ReadMessage(conn)
	if err != nil {
		conn.Close()
		return err
	}

	if msgType == MsgTypeError {
		errMsg, _ := DecodeErrorMessage(payload)
		conn.Close()
		if errMsg != nil {
			return errors.New(errMsg.Message)
		}
		return errors.New("server returned error")
	}

	if msgType == MsgTypeBlock {
		block, err := DecodeBlockMessage(payload)
		if err == nil {
			c.lib.Store(block.LIB)
			c.head.Store(block.HEAD)

			select {
			case c.recvChan <- block:
			default:
			}
		}
	} else if msgType == MsgTypeHeartbeat {
		hb, err := DecodeHeartbeatMessage(payload)
		if err == nil {
			c.lib.Store(hb.LIB)
			c.head.Store(hb.HEAD)
		}
	}

	conn.SetReadDeadline(time.Time{})

	c.conn = conn
	c.connected.Store(true)

	if c.config.Debug {
		logger.Printf("stream", "Connected to %s, starting from block %d", c.address, c.currentBlock)
	}

	return nil
}

func (c *Client) recvLoop() {
	defer c.wg.Done()

	reconnectDelay := c.config.ReconnectDelay

	for {
		select {
		case <-c.closeChan:
			return
		default:
		}

		if !c.connected.Load() {
			logger.Printf("stream", "Client not connected, attempting reconnect in %v", reconnectDelay)
			time.Sleep(reconnectDelay)
			if err := c.dial(); err != nil {
				logger.Printf("stream", "Reconnect failed: %v", err)
				reconnectDelay = reconnectDelay * 2
				if reconnectDelay > c.config.ReconnectMaxDelay {
					reconnectDelay = c.config.ReconnectMaxDelay
				}
				continue
			}
			logger.Printf("stream", "Reconnected successfully")
			reconnectDelay = c.config.ReconnectDelay
		}

		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()

		if conn == nil {
			c.connected.Store(false)
			continue
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msgType, payload, err := ReadMessage(conn)
		if err != nil {
			logger.Printf("stream", "Client read error: %v, will reconnect", err)
			c.connected.Store(false)
			conn.Close()
			continue
		}

		switch msgType {
		case MsgTypeBlock:
			block, err := DecodeBlockMessage(payload)
			if err != nil {
				continue
			}

			c.lib.Store(block.LIB)
			c.head.Store(block.HEAD)

			select {
			case c.recvChan <- block:
			case <-c.closeChan:
				return
			}

		case MsgTypeHeartbeat:
			hb, err := DecodeHeartbeatMessage(payload)
			if err != nil {
				continue
			}
			c.lib.Store(hb.LIB)
			c.head.Store(hb.HEAD)

		case MsgTypeError:
			errMsg, _ := DecodeErrorMessage(payload)
			if errMsg != nil {
				logger.Warning("Server error: %s", errMsg.Message)
			}

		case MsgTypeQueryStateResponse:
			resp, err := DecodeQueryStateResponseMessage(payload)
			if err != nil {
				continue
			}
			if ch, ok := c.pendingQueries.Load(resp.RequestID); ok {
				ch.(chan *QueryStateResponseMessage) <- resp
			}

		case MsgTypeQueryBlockResponse:
			resp, err := DecodeQueryBlockResponseMessage(payload)
			if err != nil {
				continue
			}
			if ch, ok := c.pendingQueries.Load(resp.RequestID); ok {
				ch.(chan *QueryBlockResponseMessage) <- resp
			}

		case MsgTypeQueryBlockBatchStart:
			resp, err := DecodeQueryBlockBatchStartMessage(payload)
			if err != nil {
				continue
			}
			if collector, ok := c.pendingBatches.Load(resp.RequestID); ok {
				bc := collector.(*batchCollector)
				bc.mu.Lock()
				bc.expected = resp.TotalBlocks
				bc.mu.Unlock()
			}

		case MsgTypeQueryBlockBatchBlock:
			resp, err := DecodeQueryBlockBatchBlockMessage(payload)
			if err != nil {
				continue
			}
			if collector, ok := c.pendingBatches.Load(resp.RequestID); ok {
				bc := collector.(*batchCollector)
				bc.mu.Lock()
				bc.blocks = append(bc.blocks, resp.Data)
				bc.received++
				bc.mu.Unlock()
			}

		case MsgTypeQueryBlockBatchComplete:
			resp, err := DecodeQueryBlockBatchCompleteMessage(payload)
			if err != nil {
				continue
			}
			if collector, ok := c.pendingBatches.Load(resp.RequestID); ok {
				bc := collector.(*batchCollector)
				bc.doneChan <- nil
			}

		case MsgTypeQueryGlobsResponse:
			resp, err := DecodeQueryGlobsResponseMessage(payload)
			if err != nil {
				continue
			}
			if ch, ok := c.pendingQueries.Load(resp.RequestID); ok {
				ch.(chan *QueryGlobsResponseMessage) <- resp
			}

		case MsgTypeQueryError:
			errMsg, err := DecodeQueryErrorMessage(payload)
			if err != nil {
				continue
			}
			if errMsg.RequestID == 0 {
				logger.Warning("Stream error: %s", errMsg.Message)
			} else {
				if collector, ok := c.pendingBatches.Load(errMsg.RequestID); ok {
					bc := collector.(*batchCollector)
					bc.doneChan <- errors.New(errMsg.Message)
				}
			}
		}
	}
}

func (c *Client) NextBlock() (uint32, []byte, error) {
	select {
	case <-c.closeChan:
		return 0, nil, ErrAlreadyClosed
	case msg := <-c.recvChan:
		c.currentBlock = msg.BlockNum + 1

		if c.config.AckInterval > 0 && msg.BlockNum-c.lastAcked >= c.config.AckInterval {
			c.sendAck(msg.BlockNum)
			c.lastAcked = msg.BlockNum
		}

		return msg.BlockNum, msg.Data, nil
	}
}

func (c *Client) NextBlockWithTimeout(timeout time.Duration) (uint32, []byte, error) {
	select {
	case <-c.closeChan:
		return 0, nil, ErrAlreadyClosed
	case msg := <-c.recvChan:
		c.currentBlock = msg.BlockNum + 1

		if c.config.AckInterval > 0 && msg.BlockNum-c.lastAcked >= c.config.AckInterval {
			c.sendAck(msg.BlockNum)
			c.lastAcked = msg.BlockNum
		}

		return msg.BlockNum, msg.Data, nil
	case <-time.After(timeout):
		return 0, nil, nil
	}
}

func (c *Client) sendAck(blockNum uint32) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return
	}

	ack := &AckMessage{BlockNum: blockNum}
	WriteMessage(conn, MsgTypeAck, EncodeAckMessage(ack))
}

func (c *Client) GetLIB() uint32 {
	return c.lib.Load()
}

func (c *Client) GetHEAD() uint32 {
	return c.head.Load()
}

func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrAlreadyClosed
	}

	close(c.closeChan)

	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()

	c.wg.Wait()
	return nil
}

func (c *Client) QueryState(timeout time.Duration) (head, lib uint32, err error) {
	if !c.connected.Load() {
		return 0, 0, ErrNotConnected
	}

	queryID := c.queryID.Add(1)
	respChan := make(chan *QueryStateResponseMessage, 1)
	c.pendingQueries.Store(queryID, respChan)
	defer c.pendingQueries.Delete(queryID)

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return 0, 0, ErrNotConnected
	}

	req := &QueryStateMessage{RequestID: queryID}
	if err := WriteMessage(conn, MsgTypeQueryState, EncodeQueryStateMessage(req)); err != nil {
		return 0, 0, err
	}

	select {
	case resp := <-respChan:
		return resp.HEAD, resp.LIB, nil
	case <-time.After(timeout):
		return 0, 0, errors.New("query timeout")
	case <-c.closeChan:
		return 0, 0, ErrAlreadyClosed
	}
}

func (c *Client) QueryBlock(blockNum uint32, timeout time.Duration) (*QueryBlockResponseMessage, error) {
	if !c.connected.Load() {
		return nil, ErrNotConnected
	}

	queryID := c.queryID.Add(1)
	respChan := make(chan *QueryBlockResponseMessage, 1)
	c.pendingQueries.Store(queryID, respChan)
	defer c.pendingQueries.Delete(queryID)

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return nil, ErrNotConnected
	}

	req := &QueryBlockMessage{RequestID: queryID, BlockNum: blockNum}
	if err := WriteMessage(conn, MsgTypeQueryBlock, EncodeQueryBlockMessage(req)); err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		return resp, nil
	case <-time.After(timeout):
		return nil, errors.New("query timeout")
	case <-c.closeChan:
		return nil, ErrAlreadyClosed
	}
}

func (c *Client) QueryBlockBatch(startBlock, endBlock uint32, timeout time.Duration) ([][]byte, error) {
	if !c.connected.Load() {
		return nil, ErrNotConnected
	}

	queryID := c.queryID.Add(1)

	collector := &batchCollector{
		blocks:   make([][]byte, 0, endBlock-startBlock+1),
		doneChan: make(chan error, 1),
	}
	c.pendingBatches.Store(queryID, collector)
	defer c.pendingBatches.Delete(queryID)

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return nil, ErrNotConnected
	}

	req := &QueryBlockBatchMessage{
		RequestID:  queryID,
		StartBlock: startBlock,
		EndBlock:   endBlock,
	}
	if err := WriteMessage(conn, MsgTypeQueryBlockBatch, EncodeQueryBlockBatchMessage(req)); err != nil {
		return nil, err
	}

	select {
	case err := <-collector.doneChan:
		if err != nil {
			return nil, err
		}
		return collector.blocks, nil
	case <-time.After(timeout):
		return nil, errors.New("query timeout")
	case <-c.closeChan:
		return nil, ErrAlreadyClosed
	}
}

func (c *Client) QueryGlobs(globalSeqs []uint64, timeout time.Duration) (*QueryGlobsResponseMessage, error) {
	if len(globalSeqs) > 1000 {
		return nil, errors.New("max 1000 global seqs per query")
	}

	if !c.connected.Load() {
		return nil, ErrNotConnected
	}

	queryID := c.queryID.Add(1)
	respChan := make(chan *QueryGlobsResponseMessage, 1)
	c.pendingQueries.Store(queryID, respChan)
	defer c.pendingQueries.Delete(queryID)

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return nil, ErrNotConnected
	}

	req := &QueryGlobsMessage{RequestID: queryID, GlobalSeqs: globalSeqs}
	if err := WriteMessage(conn, MsgTypeQueryGlobs, EncodeQueryGlobsMessage(req)); err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		return resp, nil
	case <-time.After(timeout):
		return nil, errors.New("query timeout")
	case <-c.closeChan:
		return nil, ErrAlreadyClosed
	}
}
