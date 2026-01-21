package internal

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

type StreamClient struct {
	id          uint64
	server      *StreamServer
	sub         *Subscription
	filter      ActionFilter
	startSeq    uint64
	decode      bool
	connectTime time.Time
	actionsSent atomic.Uint64
	closeChan   chan struct{}
	closed      atomic.Bool
}

func NewStreamClient(id uint64, server *StreamServer, filter ActionFilter, startSeq uint64, decode bool) *StreamClient {
	return &StreamClient{
		id:          id,
		server:      server,
		filter:      filter,
		startSeq:    startSeq,
		decode:      decode,
		connectTime: time.Now(),
		closeChan:   make(chan struct{}),
	}
}

func (c *StreamClient) Run(ctx context.Context, sendAction func(StreamedAction) error, sendCatchupComplete func() error) error {
	sub := c.server.broadcaster.Subscribe(c.filter)
	c.sub = sub
	defer c.server.broadcaster.Unsubscribe(sub.id)

	headSeq, _ := c.server.broadcaster.GetState()
	needsCatchup := c.startSeq < headSeq

	logger.Printf("stream", "Client %d: startSeq=%d, headSeq=%d, needsCatchup=%v", c.id, c.startSeq, headSeq, needsCatchup)

	if needsCatchup {
		logger.Printf("stream", "Client %d starting catchup from seq %d to %d", c.id, c.startSeq, headSeq)

		catchup := NewStreamCatchup(
			c.server.indexes,
			c.server.reader,
			c.filter,
			c.startSeq,
			headSeq,
		)

		var lastSentSeq uint64
		err := catchup.Run(ctx, func(action StreamedAction) error {
			if err := sendAction(action); err != nil {
				return err
			}
			c.actionsSent.Add(1)
			lastSentSeq = action.GlobalSeq
			return nil
		})

		if err != nil {
			return err
		}

		for {
			select {
			case action, ok := <-sub.sendCh:
				if !ok {
					return nil
				}
				if action.GlobalSeq <= lastSentSeq {
					continue
				}
				if err := sendAction(action); err != nil {
					return err
				}
				c.actionsSent.Add(1)
				lastSentSeq = action.GlobalSeq
			default:
				goto catchupDone
			}
		}
	catchupDone:
		logger.Printf("stream", "Client %d catchup complete", c.id)
	}

	if err := sendCatchupComplete(); err != nil {
		return err
	}

	heartbeatInterval := time.Duration(c.server.GetHeartbeatInterval()) * time.Second
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeChan:
			return nil
		case <-heartbeatTicker.C:
		case action, ok := <-sub.sendCh:
			if !ok {
				return nil
			}
			if err := sendAction(action); err != nil {
				return err
			}
			c.actionsSent.Add(1)
		}
	}
}

func (c *StreamClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeChan)
	}
}

func (c *StreamClient) Stats() (actionsSent uint64, uptime time.Duration) {
	return c.actionsSent.Load(), time.Since(c.connectTime)
}
