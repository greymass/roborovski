package actionstream

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
)

var (
	ErrNotConnected  = errors.New("not connected")
	ErrAlreadyClosed = errors.New("client already closed")
)

const (
	MsgTypeActionSubscribe   uint8 = 0x30
	MsgTypeActionAck         uint8 = 0x31
	MsgTypeActionBatch       uint8 = 0x32
	MsgTypeActionHeartbeat   uint8 = 0x33
	MsgTypeActionError       uint8 = 0x34
	MsgTypeActionDecoded     uint8 = 0x35
	MsgTypeCatchupComplete   uint8 = 0x36

	MaxMessageSize = 10 * 1024 * 1024
)

type ClientConfig struct {
	ReconnectDelay    time.Duration
	ReconnectMaxDelay time.Duration
	AckInterval       uint64
	Debug             bool
	Decode            bool
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		ReconnectDelay:    1 * time.Second,
		ReconnectMaxDelay: 30 * time.Second,
		AckInterval:       1000,
		Debug:             false,
	}
}

type Action struct {
	GlobalSeq  uint64
	BlockNum   uint32
	BlockTime  uint32
	Contract   string
	Action     string
	Receiver   string
	ActionData []byte
}

type Filter struct {
	Contracts []uint64
	Receivers []uint64
	Actions   []uint64
}

type Client struct {
	config   ClientConfig
	address  string
	filter   Filter
	startSeq uint64

	conn      net.Conn
	connMu    sync.Mutex
	connected atomic.Bool

	currentSeq uint64
	lastAcked  uint64
	seqMu      sync.Mutex

	headSeq atomic.Uint64
	libSeq  atomic.Uint64

	recvChan        chan Action
	closeChan       chan struct{}
	closed          atomic.Bool
	catchupComplete atomic.Bool
	wg              sync.WaitGroup
}

func NewClient(address string, filter Filter, startSeq uint64, config ClientConfig) *Client {
	return &Client{
		config:     config,
		address:    address,
		filter:     filter,
		startSeq:   startSeq,
		currentSeq: startSeq,
		recvChan:   make(chan Action, 1000),
		closeChan:  make(chan struct{}),
	}
}

func (c *Client) Connect() error {
	if c.closed.Load() {
		return ErrAlreadyClosed
	}

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

	c.seqMu.Lock()
	startFrom := c.currentSeq
	c.seqMu.Unlock()

	if err := c.sendSubscribe(conn, startFrom); err != nil {
		conn.Close()
		return err
	}

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	msgType, payload, err := c.readMessage(conn)
	if err != nil {
		conn.Close()
		return err
	}

	switch msgType {
	case MsgTypeActionError:
		errMsg := c.decodeError(payload)
		conn.Close()
		return errors.New(errMsg)
	case MsgTypeActionHeartbeat:
		if len(payload) >= 16 {
			c.headSeq.Store(binary.LittleEndian.Uint64(payload[0:8]))
			c.libSeq.Store(binary.LittleEndian.Uint64(payload[8:16]))
		}
	case MsgTypeCatchupComplete:
		if len(payload) >= 16 {
			c.headSeq.Store(binary.LittleEndian.Uint64(payload[0:8]))
			c.libSeq.Store(binary.LittleEndian.Uint64(payload[8:16]))
		}
		c.catchupComplete.Store(true)
	case MsgTypeActionBatch, MsgTypeActionDecoded:
		action, err := c.decodeAction(payload)
		if err == nil {
			select {
			case c.recvChan <- action:
			default:
			}
		}
	}

	conn.SetReadDeadline(time.Time{})

	c.conn = conn
	c.connected.Store(true)

	if c.config.Debug {
		logger.Printf("actionstream", "Connected to %s, starting from seq %d", c.address, startFrom)
	}

	return nil
}

const (
	FlagDecode uint8 = 0x01
)

func (c *Client) sendSubscribe(conn net.Conn, startFrom uint64) error {
	payloadSize := 15 + len(c.filter.Contracts)*8 + len(c.filter.Receivers)*8 + len(c.filter.Actions)*8
	payload := make([]byte, payloadSize)

	binary.LittleEndian.PutUint64(payload[0:8], startFrom)
	binary.LittleEndian.PutUint16(payload[8:10], uint16(len(c.filter.Contracts)))
	binary.LittleEndian.PutUint16(payload[10:12], uint16(len(c.filter.Receivers)))
	binary.LittleEndian.PutUint16(payload[12:14], uint16(len(c.filter.Actions)))

	var flags uint8
	if c.config.Decode {
		flags |= FlagDecode
	}
	payload[14] = flags

	offset := 15
	for _, contract := range c.filter.Contracts {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], contract)
		offset += 8
	}
	for _, receiver := range c.filter.Receivers {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], receiver)
		offset += 8
	}
	for _, action := range c.filter.Actions {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], action)
		offset += 8
	}

	return c.writeMessage(conn, MsgTypeActionSubscribe, payload)
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
			if c.config.Debug {
				logger.Printf("actionstream", "Not connected, attempting reconnect in %v", reconnectDelay)
			}
			time.Sleep(reconnectDelay)
			if err := c.dial(); err != nil {
				if c.config.Debug {
					logger.Printf("actionstream", "Reconnect failed: %v", err)
				}
				reconnectDelay = reconnectDelay * 2
				if reconnectDelay > c.config.ReconnectMaxDelay {
					reconnectDelay = c.config.ReconnectMaxDelay
				}
				continue
			}
			if c.config.Debug {
				logger.Printf("actionstream", "Reconnected successfully")
			}
			reconnectDelay = c.config.ReconnectDelay
		}

		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()

		if conn == nil {
			c.connected.Store(false)
			continue
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		msgType, payload, err := c.readMessage(conn)
		if err != nil {
			if c.config.Debug {
				logger.Printf("actionstream", "Read error: %v, will reconnect", err)
			}
			c.connected.Store(false)
			conn.Close()
			continue
		}

		switch msgType {
		case MsgTypeActionBatch, MsgTypeActionDecoded:
			action, err := c.decodeAction(payload)
			if err != nil {
				if c.config.Debug {
					logger.Warning("Failed to decode action: %v", err)
				}
				continue
			}

			select {
			case c.recvChan <- action:
			case <-c.closeChan:
				return
			}

		case MsgTypeActionHeartbeat:
			if len(payload) >= 16 {
				c.headSeq.Store(binary.LittleEndian.Uint64(payload[0:8]))
				c.libSeq.Store(binary.LittleEndian.Uint64(payload[8:16]))
			}

		case MsgTypeCatchupComplete:
			if len(payload) >= 16 {
				c.headSeq.Store(binary.LittleEndian.Uint64(payload[0:8]))
				c.libSeq.Store(binary.LittleEndian.Uint64(payload[8:16]))
			}
			c.catchupComplete.Store(true)
			if c.config.Debug {
				logger.Printf("actionstream", "Catchup complete, headSeq=%d, libSeq=%d", c.headSeq.Load(), c.libSeq.Load())
			}

		case MsgTypeActionError:
			errMsg := c.decodeError(payload)
			logger.Warning("Server error: %s", errMsg)
		}
	}
}

func (c *Client) Next() (Action, error) {
	select {
	case <-c.closeChan:
		return Action{}, ErrAlreadyClosed
	case action := <-c.recvChan:
		c.seqMu.Lock()
		c.currentSeq = action.GlobalSeq + 1
		shouldAck := c.config.AckInterval > 0 && action.GlobalSeq-c.lastAcked >= c.config.AckInterval
		if shouldAck {
			c.lastAcked = action.GlobalSeq
		}
		c.seqMu.Unlock()

		if shouldAck {
			c.sendAck(action.GlobalSeq)
		}

		return action, nil
	}
}

func (c *Client) NextWithTimeout(timeout time.Duration) (Action, bool, error) {
	select {
	case <-c.closeChan:
		return Action{}, false, ErrAlreadyClosed
	case action := <-c.recvChan:
		c.seqMu.Lock()
		c.currentSeq = action.GlobalSeq + 1
		shouldAck := c.config.AckInterval > 0 && action.GlobalSeq-c.lastAcked >= c.config.AckInterval
		if shouldAck {
			c.lastAcked = action.GlobalSeq
		}
		c.seqMu.Unlock()

		if shouldAck {
			c.sendAck(action.GlobalSeq)
		}

		return action, true, nil
	case <-time.After(timeout):
		return Action{}, false, nil
	}
}

func (c *Client) sendAck(globalSeq uint64) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return
	}

	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload[0:8], globalSeq)
	c.writeMessage(conn, MsgTypeActionAck, payload)
}

func (c *Client) GetHeadSeq() uint64 {
	return c.headSeq.Load()
}

func (c *Client) GetLIBSeq() uint64 {
	return c.libSeq.Load()
}

func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

func (c *Client) IsCatchupComplete() bool {
	return c.catchupComplete.Load()
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

func (c *Client) readMessage(r io.Reader) (uint8, []byte, error) {
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

func (c *Client) writeMessage(w io.Writer, msgType uint8, payload []byte) error {
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

func (c *Client) decodeAction(payload []byte) (Action, error) {
	if len(payload) < 40 {
		return Action{}, errors.New("action payload too short")
	}

	globalSeq := binary.LittleEndian.Uint64(payload[0:8])
	blockNum := binary.LittleEndian.Uint32(payload[8:12])
	blockTime := binary.LittleEndian.Uint32(payload[12:16])
	contract := binary.LittleEndian.Uint64(payload[16:24])
	actionName := binary.LittleEndian.Uint64(payload[24:32])
	receiver := binary.LittleEndian.Uint64(payload[32:40])

	var actionData []byte
	if len(payload) > 40 {
		actionData = payload[40:]
	}

	return Action{
		GlobalSeq:  globalSeq,
		BlockNum:   blockNum,
		BlockTime:  blockTime,
		Contract:   chain.NameToString(contract),
		Action:     chain.NameToString(actionName),
		Receiver:   chain.NameToString(receiver),
		ActionData: actionData,
	}, nil
}

func (c *Client) decodeError(payload []byte) string {
	if len(payload) < 2 {
		return "unknown error"
	}
	if len(payload) > 2 {
		return string(payload[2:])
	}
	return "error"
}
