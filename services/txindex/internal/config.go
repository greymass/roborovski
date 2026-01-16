package internal

import "time"

type Config struct {
	ABIPath          string   `name:"abi-source" alias:"abi-path" help:"Path to ABI data directory (for action decoding)"`
	BlockCacheMB     int      `name:"block-cache-size-mb" alias:"block-cache-mb" default:"2048" help:"Block cache size in MB"`
	BulkBufferMB     int      `name:"bulk-buffer-mb" default:"1024" help:"Bulk buffer size in MB"`
	Compactors       int      `default:"4" help:"Number of concurrent Pebble compaction threads"`
	Debug            bool     `help:"Enable debug logging (all categories)"`
	GOGC             int      `name:"gogc" default:"20" help:"Go GC target percentage"`
	HistorySource    string   `name:"history-source" required:"true" help:"History source (local path, http://, http+unix://, or stream://)"`
	IndexPath        string   `name:"index-storage" alias:"index-path,path" default:"./txindex" help:"Path to transaction index directory"`
	HTTPListen    string   `name:"http-listen" default:":9430" help:"HTTP API TCP address ('none' to disable)"`
	HTTPSocket    string   `name:"http-socket" default:"./txindex.sock" help:"HTTP API Unix socket ('none' to disable)"`
	MetricsListen string   `name:"metrics-listen" default:"none" help:"Metrics endpoint address (e.g., 'localhost:9090' or '/path/to/metrics.sock')"`
	LogFilter     []string `name:"log-filter" default:"startup,sync,http,timing,stats" help:"Log category filter (comma-separated)"`
	LogFile          string   `name:"log-file" help:"Log output file path (logs to both stdout and file when set)"`
	LogInterval      string   `name:"log-interval" default:"3" help:"Sync progress log interval. Supports duration syntax (500ms, 1s, 3s)."`
	LRUCacheSlices   int      `name:"lru-cache-slices" default:"20" help:"Number of slice mmap handles to keep open"`
	MergeIntervalMin int      `name:"merge-interval" default:"60" help:"Merge interval in minutes"`
	MergeThreshold   int      `name:"merge-threshold" default:"5000000" help:"Merge threshold (number of transactions)"`
	PprofPort        string   `name:"pprof-port" help:"Port for pprof debugging endpoint"`
	Profile          bool     `help:"Enable periodic CPU profiling"`
	ProfileInterval  int      `name:"profile-interval" default:"60" help:"Profile logging interval in seconds"`
	QueryTrace       bool     `name:"query-trace" help:"Enable query tracing to log index performance per request"`
	ReadOnly         bool     `name:"read-only" help:"Run in read-only mode (no syncing)"`
	SkipOnblock      bool     `name:"skip-onblock" default:"true" help:"Skip eosio::onblock actions"`
	Timing           bool     `help:"Enable detailed timing output during sync"`
	Workers          int      `default:"16" help:"Number of worker threads"`
}

func (c *Config) MergeInterval() time.Duration {
	return time.Duration(c.MergeIntervalMin) * time.Minute
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
