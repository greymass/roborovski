package main

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	Debug            bool   `help:"Enable debug logging (all categories)"`
	Timing           bool   `help:"Enable detailed timing output during sync"`
	TracePath        string `name:"trace-dir" default:"./data" help:"Path to nodeos trace directory"`
	Stride           int    `default:"500" help:"Trace file stride (blocks per trace file)"`
	GOGC             int    `name:"gogc" default:"100" help:"Go GC target percentage"`
	Path             string `name:"index-storage" alias:"index-path,path" default:"./coreindex.db" help:"Path to coreindex storage directory"`
	Workers          int    `default:"16" help:"Worker threads for sync"`
	FlushInterval    int    `name:"flush-interval" default:"500" help:"Flush state every N blocks"`
	SliceSize        int    `name:"slice-size" default:"10000" help:"Blocks per slice"`
	MaxCachedSlices  int    `name:"max-cached-slices" default:"2" help:"Max slices to keep in memory"`
	LogSliceInterval int    `name:"log-slice-interval" default:"100" help:"Log slice creation every N slices"`
	StartBlock       int    `name:"start-block" default:"0" help:"Start syncing from this block (default: block 2)"`
	Zstd             bool   `help:"Enable zstd compression"`
	ZstdLevel        int    `name:"zstd-level" default:"3" help:"Zstd compression level (1=fast, 3=balanced, 9=best)"`
	Verify           bool   `help:"Rebuild all glob indexes and report invalid slices, then exit"`
	PprofPort        string `name:"pprof-port" help:"Port for pprof debugging endpoint"`
	Profile          bool   `help:"Enable periodic CPU profiling"`
	ProfileInterval  int    `name:"profile-interval" default:"60" help:"Profile logging interval in seconds"`
	Prefetchers      int    `default:"4" help:"Parallel prefetch goroutines for reading trace files"`
	QueryTrace       bool   `name:"query-trace" help:"Enable query tracing to log API performance per request"`
	ReadOnly         bool   `name:"read-only" help:"Run in read-only mode (no syncing)"`

	HTTPListen    string `name:"http-listen" default:":9400" help:"HTTP API TCP address (use 'none' to disable)"`
	HTTPSocket    string `name:"http-socket" default:"./coreindex.sock" help:"HTTP API unix socket (use 'none' to disable)"`
	MetricsListen string `name:"metrics-listen" default:"none" help:"Metrics endpoint address (e.g., 'localhost:9090' or '/path/to/metrics.sock')"`

	StreamEnabled    bool   `name:"stream-enabled" default:"true" help:"Enable streaming API"`
	StreamListen     string `name:"stream-listen" default:":9401" help:"Streaming API TCP address (use 'none' to disable)"`
	StreamSocket     string `name:"stream-socket" default:"./corestream.sock" help:"Streaming API unix socket (use 'none' to disable)"`
	StreamMaxClients int    `name:"stream-max-clients" default:"10" help:"Maximum concurrent stream clients"`

	TraceCleanupMode        string `name:"trace-cleanup-mode" help:"Trace file cleanup mode: '' (disabled), 'delete', or 'move'"`
	TraceCleanupArchivePath string `name:"trace-cleanup-archive-path" help:"Archive path for 'move' mode (required for move)"`
	TraceCleanupBlockDelay  int    `name:"trace-cleanup-block-delay" default:"10000" help:"Blocks to write AFTER stride before cleanup"`
	TraceCleanupDryRun      bool   `name:"trace-cleanup-dry-run" help:"Dry run mode: log actions without performing them"`

	LogFilter   []string `name:"log-filter" default:"startup,sync,http,stream" help:"Log category filter (comma-separated)"`
	LogFile     string   `name:"log-file" help:"Log output file path (logs to both stdout and file when set)"`
	LogInterval string   `name:"log-interval" default:"3" help:"Sync progress log interval. Supports duration syntax (500ms, 1s, 3s)."`

	SliceDownloadURL      string `name:"slice-download-url" help:"Base URL for downloading pre-built slices (disabled if empty)"`
	SliceDownloadParallel int    `name:"slice-download-parallel" default:"4" help:"Number of parallel slice download workers"`
	SliceDownloadTimeout  int    `name:"slice-download-timeout" default:"30" help:"Slice download timeout in minutes"`

	Replay      bool `help:"DESTRUCTIVE: Erase all data and restart sync from block 2"`
	VerifyOnly  bool `name:"verify-only" help:"Deep validate all slices with CRC checks, then exit"`
	Repair      bool `help:"With --verify-only: remove invalid slices (otherwise just report)"`
	RepairSlice int  `name:"repair-slice" default:"-1" help:"Repair specific slice by rebuilding indexes from data.log"`
	RepairAll   bool `name:"repair-all" help:"Scan all slices and repair any with invalid indexes"`

	ABIPath string `name:"abi-source" alias:"abi-path" help:"Path to ABI cache (default: {index-storage}/abis/)"`
}

func (c *Config) ValidateTraceCleanup() error {
	if c.TraceCleanupMode == "" {
		return nil
	}

	switch c.TraceCleanupMode {
	case "delete":
		if c.TraceCleanupBlockDelay <= 0 {
			return fmt.Errorf("trace-cleanup-block-delay must be > 0 when cleanup is enabled")
		}
		return nil

	case "move":
		if c.TraceCleanupArchivePath == "" {
			return fmt.Errorf("trace-cleanup-archive-path is required when trace-cleanup-mode=move")
		}
		if c.TraceCleanupBlockDelay <= 0 {
			return fmt.Errorf("trace-cleanup-block-delay must be > 0 when cleanup is enabled")
		}
		if err := os.MkdirAll(c.TraceCleanupArchivePath, 0755); err != nil {
			return fmt.Errorf("cannot create archive directory %s: %w", c.TraceCleanupArchivePath, err)
		}
		return nil

	default:
		return fmt.Errorf("invalid trace-cleanup-mode: '%s' (must be 'delete' or 'move')", c.TraceCleanupMode)
	}
}

func (c *Config) IsTraceCleanupEnabled() bool {
	return c.TraceCleanupMode != "" && c.ValidateTraceCleanup() == nil
}

func (c *Config) GetLogInterval() time.Duration {
	if c.LogInterval == "" {
		return 3 * time.Second
	}
	if parsed, err := time.ParseDuration(c.LogInterval); err == nil {
		return parsed
	}
	if secs, err := time.ParseDuration(c.LogInterval + "s"); err == nil {
		return secs
	}
	return 3 * time.Second
}
