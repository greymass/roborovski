package internal

type Config struct {
	// Storage paths
	ABISource     string `name:"abi-source" alias:"abi-path" help:"Path to ABI data directory"`
	HistorySource string `name:"history-source" required:"true" help:"Path to sliced history data (queries always read from here)"`
	IndexStorage  string `name:"index-storage" alias:"index-path,path" default:"./actionindex.db" help:"Path to Pebble index database"`
	SyncSource    string `name:"sync-source" help:"Optional sync source (stream:// or http://). If set, uses this for syncing while history-source is used for queries."`

	// Server
	HTTPListen    string `name:"http-listen" default:":9410" help:"HTTP API TCP address ('none' to disable)"`
	HTTPSocket    string `name:"http-socket" default:"./actionindex.sock" help:"HTTP API Unix socket ('none' to disable)"`
	MetricsListen string `name:"metrics-listen" default:"none" help:"Metrics endpoint address (e.g., 'localhost:9090' or '/path/to/metrics.sock')"`
	StreamListen  string `name:"stream-listen" default:"none" help:"Streaming TCP address for WebSocket clients ('none' to disable)"`
	StreamSocket  string `name:"stream-socket" default:"none" help:"Streaming TCP address for binary protocol clients ('none' to disable)"`
	ReadOnly      bool   `name:"read-only" help:"Run in read-only mode (no indexing)"`

	// Streaming
	StreamMaxClients        int `name:"stream-max-clients" default:"100" help:"Maximum concurrent streaming clients"`
	StreamHeartbeatInterval int `name:"stream-heartbeat" default:"30" help:"Streaming heartbeat interval in seconds"`
	Compact          bool   `help:"Run full Pebble compaction on startup, then exit"`
	MigrateMetadata  bool   `name:"migrate-metadata" help:"Migrate metadata indexes (0x13-0x15 → 0x90-0x92), then exit"`
	CleanupMetadata  bool   `name:"cleanup-metadata" help:"Delete legacy metadata indexes (0x13-0x15) after migration, then exit"`
	RebuildMetadata  bool   `name:"rebuild-metadata" help:"Rebuild chunk metadata from database, then exit"`

	// Performance tuning
	BlockCacheSizeMB  int64 `name:"block-cache-size-mb" alias:"block-cache-size" default:"2048" help:"Cache size in MB for parsed block data. 0 to disable."`
	BlockIndexCache   bool  `name:"blockindex-cache" default:"true" help:"Cache block index in memory for faster lookups. false to disable."`
	LRUCacheSlices    int   `name:"lru-cache-slices" default:"20" help:"Number of slice mmap handles to keep open"`
	Compactors        int   `default:"4" help:"Number of concurrent Pebble compaction threads"`
	PebbleCacheSizeMB int64 `name:"pebble-cache-size-mb" default:"2048" help:"Pebble block cache size in MB for index reads"`
	GOGC              int   `name:"gogc" default:"20" help:"Go GC target percentage. Lower = more frequent GC, more CPU for less memory usage."`
	SyncMemoryGB      int   `name:"sync-memory-gb" default:"4" help:"RAM budget for bulk sync in GB. Higher values = fewer disk writes = less GC pressure."`
	Workers           int   `default:"16" help:"Parallel workers for query processing"`

	// Data processing
	IgnoredActions  []string `name:"ignored-actions" help:"Actions to ignore during sync (format: contract::action, e.g. eosio::onblock)"`
	FilterContracts []string `name:"filter-contracts" help:"Only index these contracts (allowlist). Empty = index all. Format: contract1,contract2"`
	OmitNullFields  bool     `name:"omit-null-fields" default:"true" help:"Remove null fields from decoded action data"`

	// Logging and debugging
	Debug           bool     `help:"Enable debug logging (all categories)"`
	DebugEndpoints  bool     `name:"debug-endpoints" help:"Enable debug API endpoints (/debug/*)"`
	LogFilter       []string `name:"log-filter" default:"startup,sync,http,pebble" help:"Log category filter (comma-separated)"`
	LogInterval     string   `name:"log-interval" default:"3" help:"Sync progress log interval (0 for every block). Supports duration syntax (500ms, 1s)."`
	PprofPort       string   `name:"pprof-port" help:"Port for pprof debugging endpoint"`
	Profile         bool     `help:"Enable periodic CPU profiling"`
	ProfileInterval int      `name:"profile-interval" default:"60" help:"Profile logging interval in seconds"`
	QueryTrace      bool     `name:"query-trace" help:"Enable query tracing to log index performance per request"`
	Timing          bool     `help:"Enable detailed timing output during sync"`
	LogFile         string   `name:"log-file" help:"Log output file path (logs to both stdout and file when set)"`
}
