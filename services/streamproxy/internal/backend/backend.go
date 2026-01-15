package backend

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

var ErrBackendUnhealthy = errors.New("backend unhealthy")

type Backend struct {
	address string
	healthy atomic.Bool

	healthCheckInterval time.Duration
	healthCheckTimeout  time.Duration

	closeChan chan struct{}
	closed    atomic.Bool
}

type Config struct {
	Address             string
	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
}

func New(cfg Config) *Backend {
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 30 * time.Second
	}
	if cfg.HealthCheckTimeout == 0 {
		cfg.HealthCheckTimeout = 5 * time.Second
	}

	b := &Backend{
		address:             cfg.Address,
		healthCheckInterval: cfg.HealthCheckInterval,
		healthCheckTimeout:  cfg.HealthCheckTimeout,
		closeChan:           make(chan struct{}),
	}
	b.healthy.Store(true)

	return b
}

func (b *Backend) Address() string {
	return b.address
}

func (b *Backend) IsHealthy() bool {
	return b.healthy.Load()
}

func (b *Backend) Dial(timeout time.Duration) (net.Conn, error) {
	if !b.IsHealthy() {
		return nil, ErrBackendUnhealthy
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return b.dialContext(ctx)
}

func (b *Backend) dialContext(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	if strings.HasPrefix(b.address, "unix://") {
		return d.DialContext(ctx, "unix", strings.TrimPrefix(b.address, "unix://"))
	}
	if strings.HasSuffix(b.address, ".sock") {
		return d.DialContext(ctx, "unix", b.address)
	}
	addr := strings.TrimPrefix(b.address, "tcp://")
	return d.DialContext(ctx, "tcp", addr)
}

func (b *Backend) Start() {
	go b.healthCheckLoop()
}

func (b *Backend) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return errors.New("backend already closed")
	}

	close(b.closeChan)
	return nil
}

func (b *Backend) healthCheckLoop() {
	b.runHealthCheck()

	ticker := time.NewTicker(b.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.closeChan:
			return
		case <-ticker.C:
			b.runHealthCheck()
		}
	}
}

func (b *Backend) runHealthCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), b.healthCheckTimeout)
	defer cancel()

	conn, err := b.dialContext(ctx)
	if err != nil {
		if b.IsHealthy() {
			logger.Printf("backend", "Backend became unhealthy: %v", err)
			b.healthy.Store(false)
		}
		return
	}
	conn.Close()

	if !b.IsHealthy() {
		logger.Printf("backend", "Backend became healthy")
		b.healthy.Store(true)
	}
}
