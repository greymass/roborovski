package main

import (
	"fmt"
	"os"

	"github.com/greymass/roborovski/libraries/config"
)

type RouteConfig struct {
	Path          string `name:"path"`
	Backend       string `name:"backend"`
	Timeout       int    `name:"timeout"`
	RateLimit     int    `name:"rate-limit"`
	CacheDuration int    `name:"cache-duration"`
}

type Config struct {
	Debug           bool     `name:"debug" help:"Enable debug logging (all categories)"`
	GOGC            int      `name:"gogc" default:"100" help:"Go GC target percentage"`
	HTTPListen      string   `name:"http-listen" alias:"listen" default:":8080" help:"HTTP API TCP address ('none' to disable)"`
	HTTPSocket      string   `name:"http-socket" default:"./apiproxy.sock" help:"HTTP API Unix socket ('none' to disable)"`
	LogFile         string   `name:"log-file" help:"Log output file path (logs to both stdout and file when set)"`
	LogFilter       []string `name:"log-filter" default:"startup,http,proxy" help:"Log category filter (comma-separated)"`
	Profile         bool     `name:"profile" help:"Enable periodic CPU profiling"`
	ProfileInterval int      `name:"profile-interval" default:"60" help:"Profile logging interval in seconds"`
	PprofPort       string   `name:"pprof-port" help:"Port for pprof debugging endpoint"`
	QueryTrace      bool     `name:"query-trace" help:"Enable query tracing to log API performance per request"`
	Workers         int      `name:"workers" default:"16" help:"Number of worker threads"`

	UseCloudflareHeaders    bool `name:"use-cloudflare-headers" help:"Use Cloudflare headers for client IP"`
	RequireCloudflareSource bool `name:"require-cloudflare-source" help:"Require requests from Cloudflare IPs"`

	GlobalRateLimit       int `name:"global-rate-limit" default:"1000" help:"Global rate limit"`
	GlobalCacheDuration   int `name:"global-cache-duration" default:"1" help:"Global cache duration in seconds"`
	GlobalTimeout         int `name:"global-timeout" default:"30" help:"Global timeout in seconds"`
	MaxConcurrentRequests int `name:"max-concurrent-requests" default:"10000" help:"Max concurrent requests"`

	CircuitBreakerThreshold   int `name:"circuit-breaker-threshold" default:"5" help:"Circuit breaker failure threshold"`
	CircuitBreakerTimeout     int `name:"circuit-breaker-timeout" default:"60" help:"Circuit breaker timeout in seconds"`
	CircuitBreakerMaxRequests int `name:"circuit-breaker-max-requests" default:"10" help:"Circuit breaker max requests in half-open state"`

	Routes []RouteConfig `sections:"route"`
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

	for i := range cfg.Routes {
		if cfg.Routes[i].Timeout == 0 {
			cfg.Routes[i].Timeout = cfg.GlobalTimeout
		}
		if cfg.Routes[i].RateLimit == 0 {
			cfg.Routes[i].RateLimit = cfg.GlobalRateLimit
		}
		if cfg.Routes[i].CacheDuration == 0 {
			cfg.Routes[i].CacheDuration = cfg.GlobalCacheDuration
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.HTTPListen == "" && c.HTTPSocket == "" {
		return fmt.Errorf("at least one of http-listen or http-socket must be configured")
	}

	if len(c.Routes) == 0 {
		return fmt.Errorf("at least one route must be configured")
	}

	for i, route := range c.Routes {
		if route.Path == "" {
			return fmt.Errorf("route %d: path cannot be empty", i+1)
		}
		if route.Backend == "" {
			return fmt.Errorf("route %d: backend cannot be empty", i+1)
		}
		if route.Timeout <= 0 {
			return fmt.Errorf("route %d: timeout must be > 0", i+1)
		}
		if route.RateLimit <= 0 {
			return fmt.Errorf("route %d: rate-limit must be > 0", i+1)
		}
	}

	if c.GlobalRateLimit <= 0 {
		return fmt.Errorf("global-rate-limit must be > 0")
	}
	if c.GlobalTimeout <= 0 {
		return fmt.Errorf("global-timeout must be > 0")
	}
	if c.MaxConcurrentRequests <= 0 {
		return fmt.Errorf("max-concurrent-requests must be > 0")
	}

	return nil
}
