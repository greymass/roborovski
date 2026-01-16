package main

import (
	"fmt"
	"os"
	"time"

	"github.com/greymass/roborovski/libraries/config"
)

type ActionIndexConfig struct {
	Address string `name:"address" required:"true" help:"ActionIndex server address"`
}

type Config struct {
	Debug           bool     `name:"debug" help:"Enable debug logging (all categories)"`
	GOGC            int      `name:"gogc" default:"100" help:"Go GC target percentage"`
	LogFile         string   `name:"log-file" help:"Log output file path (logs to both stdout and file when set)"`
	LogFilter       []string `name:"log-filter" default:"startup,stream,health" help:"Log category filter (comma-separated)"`
	PprofPort       string   `name:"pprof-port" help:"Port for pprof debugging endpoint"`
	Profile         bool     `name:"profile" help:"Enable periodic CPU profiling"`
	ProfileInterval int      `name:"profile-interval" default:"60" help:"Profile logging interval in seconds"`

	TCPListen       string `name:"tcp-listen" alias:"stream-listen" default:":9500" help:"TCP listen address"`
	WebSocketListen string `name:"websocket-listen" default:":9501" help:"WebSocket listen address"`
	MetricsListen   string `name:"metrics-listen" default:"none" help:"Metrics endpoint address (e.g., 'localhost:9502' or '/path/to/metrics.sock')"`

	MaxConnections    int           `name:"max-connections" default:"10000" help:"Maximum connections"`
	ConnectionTimeout time.Duration `name:"connection-timeout" default:"10s" help:"Connection timeout"`

	HealthCheckInterval time.Duration `name:"health-check-interval" default:"30s" help:"Health check interval"`
	HealthCheckTimeout  time.Duration `name:"health-check-timeout" default:"5s" help:"Health check timeout"`

	ActionIndex ActionIndexConfig `section:"actionindex"`
}

func loadConfig(configPath string) (*Config, error) {
	cfg := &Config{}
	args := os.Args[1:]
	if configPath != "" {
		args = []string{"-config", configPath}
	}
	if err := config.Load(cfg, args); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.TCPListen == "" && c.WebSocketListen == "" {
		return fmt.Errorf("at least one of tcp-listen or websocket-listen must be configured")
	}

	if c.ActionIndex.Address == "" {
		return fmt.Errorf("[actionindex] address must be configured")
	}

	if c.MaxConnections <= 0 {
		return fmt.Errorf("max-connections must be > 0")
	}
	if c.ConnectionTimeout <= 0 {
		return fmt.Errorf("connection-timeout must be > 0")
	}

	return nil
}
