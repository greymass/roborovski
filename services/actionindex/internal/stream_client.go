package internal

import (
	"context"
	"errors"
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

	var lastDeliveredSeq uint64
	send := func(action StreamedAction) error {
		if action.GlobalSeq <= lastDeliveredSeq {
			return nil
		}
		if err := sendAction(action); err != nil {
			return err
		}
		lastDeliveredSeq = action.GlobalSeq
		c.actionsSent.Add(1)
		return nil
	}

	headSeq, _ := c.server.broadcaster.GetState()
	needsCatchup := c.startSeq <= headSeq && headSeq > 0

	logger.Printf("stream", "Client %d: startSeq=%d, headSeq=%d, needsCatchup=%v", c.id, c.startSeq, headSeq, needsCatchup)

	if needsCatchup {
		if err := c.runCatchup(ctx, sub, send, headSeq); err != nil {
			if errors.Is(err, errClientClosed) {
				return nil
			}
			return err
		}
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
			if err := send(action); err != nil {
				return err
			}
		case streamErr, ok := <-sub.errorCh:
			if !ok {
				return nil
			}
			return sendError(streamErr)
		}
	}
}

var errClientClosed = errors.New("stream client closed")

// runCatchup holds a server-wide admission slot for the whole catch-up phase; the live phase runs without one.
func (c *StreamClient) runCatchup(ctx context.Context, sub *Subscription, send func(StreamedAction) error, headSeq uint64) error {
	if err := c.acquireCatchupSlot(ctx); err != nil {
		return err
	}
	defer c.releaseCatchupSlot()

	currentStartSeq := c.startSeq
	var lastSentSeq uint64
	totalCatchupStart := time.Now()
	iteration := 0

	for currentStartSeq <= headSeq {
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
			if err := send(action); err != nil {
				return err
			}
			lastSentSeq = action.GlobalSeq
			return nil
		})

		if err != nil {
			return err
		}

		iterDuration := time.Since(iterStart)
		logger.Printf("stream", "Client %d catchup iteration %d done in %v, lastSeq=%d",
			c.id, iteration, iterDuration.Round(time.Millisecond), lastSentSeq)

		currentStartSeq = headSeq + 1
		headSeq, _ = c.server.broadcaster.GetState()
	}

	sub.ResetCounters()

	totalDuration := time.Since(totalCatchupStart)
	logger.Printf("stream", "Client %d catchup complete: %d actions in %v (%d iterations), lastSeq=%d",
		c.id, c.actionsSent.Load(), totalDuration.Round(time.Millisecond), iteration, lastSentSeq)
	return nil
}

func (c *StreamClient) acquireCatchupSlot(ctx context.Context) error {
	sem := c.server.catchupSem
	if sem == nil {
		return nil
	}
	waitStart := time.Now()
	select {
	case sem <- struct{}{}:
		if waited := time.Since(waitStart); waited > time.Second {
			logger.Printf("stream", "Client %d waited %v for a catchup slot", c.id, waited.Round(time.Millisecond))
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeChan:
		return errClientClosed
	}
}

func (c *StreamClient) releaseCatchupSlot() {
	if c.server.catchupSem == nil {
		return
	}
	<-c.server.catchupSem
}

func (c *StreamClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeChan)
	}
}

func (c *StreamClient) Stats() (actionsSent uint64, uptime time.Duration) {
	return c.actionsSent.Load(), time.Since(c.connectTime)
}
