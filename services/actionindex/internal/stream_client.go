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

func (c *StreamClient) Run(ctx context.Context, sendAction func(StreamedAction) error, sendCatchupComplete func() error, sendError func(StreamError) error, sendHeartbeat func() error) error {
	sub := c.server.broadcaster.Subscribe(c.filter)
	c.sub = sub
	defer c.server.broadcaster.Unsubscribe(sub.id)

	headSeq, _ := c.server.broadcaster.GetState()
	needsCatchup := c.startSeq < headSeq

	logger.Printf("stream", "Client %d: startSeq=%d, headSeq=%d, needsCatchup=%v", c.id, c.startSeq, headSeq, needsCatchup)

	if needsCatchup {
		sub.SetCatchingUp(true)

		currentStartSeq := c.startSeq
		var lastSentSeq uint64
		totalCatchupStart := time.Now()
		iteration := 0

		for currentStartSeq < headSeq {
			iteration++
			logger.Printf("stream", "Client %d catchup iteration %d: seq %d to %d", c.id, iteration, currentStartSeq, headSeq)

			catchup := NewStreamCatchup(
				c.server.indexes,
				c.server.reader,
				c.filter,
				currentStartSeq,
				headSeq,
			)

			iterStart := time.Now()
			err := catchup.Run(ctx, func(action StreamedAction) error {
				if err := sendAction(action); err != nil {
					return err
				}
				c.actionsSent.Add(1)
				lastSentSeq = action.GlobalSeq
				return nil
			})

			if err != nil {
				sub.SetCatchingUp(false)
				return err
			}

			iterDuration := time.Since(iterStart)
			logger.Printf("stream", "Client %d catchup iteration %d done in %v, lastSeq=%d",
				c.id, iteration, iterDuration.Round(time.Millisecond), lastSentSeq)

			currentStartSeq = headSeq + 1
			headSeq, _ = c.server.broadcaster.GetState()
		}

		sub.SetCatchingUp(false)
		sub.ResetCounters()

		totalDuration := time.Since(totalCatchupStart)
		logger.Printf("stream", "Client %d catchup complete: %d actions in %v (%d iterations), lastSeq=%d",
			c.id, c.actionsSent.Load(), totalDuration.Round(time.Millisecond), iteration, lastSentSeq)
	}

	if err := sendCatchupComplete(); err != nil {
		return err
	}

	headSeqNow, libSeqNow := c.server.broadcaster.GetState()
	logger.Printf("stream", "Client %d entering live mode: headSeq=%d, libSeq=%d, liveMode=%v",
		c.id, headSeqNow, libSeqNow, c.server.broadcaster.IsLiveMode())

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
			if err := sendHeartbeat(); err != nil {
				return err
			}
		case action, ok := <-sub.sendCh:
			if !ok {
				return nil
			}
			if err := sendAction(action); err != nil {
				return err
			}
			c.actionsSent.Add(1)
		case streamErr, ok := <-sub.errorCh:
			if !ok {
				return nil
			}
			return sendError(streamErr)
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
