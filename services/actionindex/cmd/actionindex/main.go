package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/config"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/profiler"
	"github.com/greymass/roborovski/libraries/server"
	"github.com/greymass/roborovski/services/actionindex/internal"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Version = "dev"

var (
	productionCategories = []string{"startup", "sync", "http", "pebble", "timing", "profiler", "stream"}
	debugCategories      = []string{"debug", "debug-pebble", "debug-index", "debug-query", "debug-perf", "debug-timing", "debug-chunks"}
	allCategories        = append(append([]string{}, productionCategories...), debugCategories...)
)

var acceptHTTP atomic.Bool

func main() {
	config.CheckVersion(Version)

	cfg := &internal.Config{}
	if err := config.Load(cfg, os.Args[1:]); err != nil {
		logger.Fatal("Config error: %v", err)
	}

	logger.RegisterCategories(allCategories...)
	if cfg.Debug {
		logger.SetMinLevel(logger.LevelDebug)
		logger.SetCategoryFilter(nil)
		logger.Printf("debug-perf", "Debug logging enabled - all categories active")
	} else if cfg.Compact {
		logger.SetMinLevel(logger.LevelDebug)
		logger.SetCategoryFilter([]string{"startup", "pebble", "debug-pebble"})
	} else if cfg.Timing || cfg.QueryTrace {
		logger.SetMinLevel(logger.LevelDebug)
		filter := append([]string{}, cfg.LogFilter...)
		if cfg.Timing {
			filter = append(filter, "debug-timing", "debug-chunks")
		}
		if cfg.QueryTrace {
			filter = append(filter, "debug-query", "debug-perf")
		}
		logger.SetCategoryFilter(filter)
	} else {
		logger.SetCategoryFilter(cfg.LogFilter)
	}

	if cfg.LogFile != "" {
		if err := logger.SetLogFile(cfg.LogFile); err != nil {
			logger.Fatal("Failed to open log file %s: %v", cfg.LogFile, err)
		}
		defer logger.Close()
		logger.Printf("startup", "Logging to file: %s", cfg.LogFile)
	}

	if cfg.GOGC > 0 {
		debug.SetGCPercent(cfg.GOGC)
	}

	runtime.GOMAXPROCS(cfg.Workers)

	logger.Printf("startup", "actionindex %s starting...", Version)

	if err := internal.InitOpenAPI(Version); err != nil {
		logger.Fatal("Failed to load OpenAPI spec: %v", err)
	}
	if err := internal.ValidateOpenAPIRoutes(cfg.DebugEndpoints); err != nil {
		logger.Fatal("%v", err)
	}

	logger.Printf("startup", "Storage:")
	logger.Printf("startup", "  index-storage: %s", cfg.IndexStorage)
	logger.Printf("startup", "  history-source: %s", cfg.HistorySource)
	if cfg.SyncSource != "" {
		logger.Printf("startup", "  sync-source: %s", cfg.SyncSource)
	}
	if cfg.ABISource != "" {
		logger.Printf("startup", "  abi-source: %s", cfg.ABISource)
	} else {
		logger.Printf("startup", "  abi-source: (not set, action data will not be decoded)")
	}

	memTableSizeMB := cfg.SyncMemoryGB * 1024 / 4
	if memTableSizeMB < 64 {
		memTableSizeMB = 64 // Minimum 64 MB
	}
	if memTableSizeMB > 2048 {
		memTableSizeMB = 2048 // Cap at 2 GB per memtable for reasonable flush times
	}

	memTableStopThreshold := (cfg.SyncMemoryGB * 1024) / memTableSizeMB
	if memTableStopThreshold < 4 {
		memTableStopThreshold = 4 // Minimum 4
	}
	totalMemTableBuffer := memTableSizeMB * memTableStopThreshold

	bulkCommitBlocks := uint32(cfg.SyncMemoryGB * 2500)
	if bulkCommitBlocks < 10000 {
		bulkCommitBlocks = 10000
	}

	logger.Printf("startup", "Indexing:")
	logger.Printf("startup", "  workers: %d", cfg.Workers)
	logger.Printf("startup", "  compactors: %d", cfg.Compactors)
	logger.Printf("startup", "  pebble-cache-size: %d MB", cfg.PebbleCacheSizeMB)
	logger.Printf("startup", "  blockindex-cache: %v", cfg.BlockIndexCache)
	logger.Printf("startup", "  lru-cache-slices: %d", cfg.LRUCacheSlices)
	logger.Printf("startup", "  gogc: %d", cfg.GOGC)
	logger.Printf("startup", "  read-only: %v", cfg.ReadOnly)
	logger.Printf("startup", "  sync-memory-gb: %d", cfg.SyncMemoryGB)
	logger.Printf("startup", "  memtable-size: %d MB (calculated)", memTableSizeMB)
	logger.Printf("startup", "  memtable-threshold: %d (calculated, total buffer: %d MB)", memTableStopThreshold, totalMemTableBuffer)
	logger.Printf("startup", "  bulk-commit-interval: %d blocks (calculated)", bulkCommitBlocks)
	if len(cfg.IgnoredActions) > 0 {
		logger.Printf("startup", "  ignored-actions: %v", cfg.IgnoredActions)
	}

	logger.Printf("startup", "API:")
	if cfg.HTTPListen != "none" {
		logger.Printf("startup", "  http-listen: %s", cfg.HTTPListen)
	}
	if cfg.HTTPSocket != "none" {
		logger.Printf("startup", "  http-socket: %s", cfg.HTTPSocket)
	}
	logger.Printf("startup", "  omit-null-fields: %v", cfg.OmitNullFields)

	logger.Printf("startup", "Logging:")
	logger.Printf("startup", "  log-filter: %s", strings.Join(cfg.LogFilter, ", "))
	if cfg.Timing {
		logger.Printf("startup", "  timing: enabled (bulk sync metrics)")
	}
	if cfg.QueryTrace {
		logger.Printf("startup", "  query-trace: enabled (index performance per request)")
	}

	if cfg.Profile {
		logger.Println("startup", "")
		logger.Printf("startup", "Profiling: enabled (interval %ds)", cfg.ProfileInterval)
	}

	if cfg.Debug || cfg.DebugEndpoints || cfg.PprofPort != "" {
		logger.Println("startup", "")
		logger.Printf("startup", "Debugging:")
		if cfg.Debug {
			logger.Printf("startup", "  debug: %v", cfg.Debug)
		}
		if cfg.DebugEndpoints {
			logger.Printf("startup", "  debug-endpoints: %v", cfg.DebugEndpoints)
		}
		if cfg.PprofPort != "" {
			logger.Printf("startup", "  pprof-port: %s", cfg.PprofPort)
		}
	}

	logger.Println("startup", "")

	if cfg.PprofPort != "" {
		go func() {
			addr := "localhost:" + cfg.PprofPort
			if err := http.ListenAndServe(addr, nil); err != nil {
				logger.Printf("startup", "pprof server error: %v", err)
			}
		}()
	}

	if cfg.Profile {
		profiler.EnableBlockProfiling()
		profiler.Start(profiler.Config{
			ServiceName: "actionindex",
			Interval:    time.Duration(cfg.ProfileInterval) * time.Second,
		})
	}

	storeCfg := internal.StoreConfig{
		MemTableSizeMB:        memTableSizeMB,
		MemTableStopThreshold: memTableStopThreshold,
		Compactors:            cfg.Compactors,
		CacheSizeMB:           cfg.PebbleCacheSizeMB,
	}
	store, err := internal.NewStore(cfg.IndexStorage, cfg.ReadOnly, storeCfg)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}

	if cfg.Compact {
		logger.Printf("startup", "Running full Pebble compaction...")
		start := time.Now()
		endKey := make([]byte, 32)
		for i := range endKey {
			endKey[i] = 0xff
		}
		if err := store.DB().Compact(context.Background(), []byte{0x00}, endKey, true); err != nil {
			logger.Fatal("Compaction failed: %v", err)
		}
		logger.Printf("startup", "Compaction complete in %v", time.Since(start))
		store.Close()
		logger.Printf("startup", "Done")
		return
	}

	if cfg.MigrateMetadata {
		hasLegacy, hasNew := internal.CheckMetadataMigrationNeeded(store.DB())
		if !hasLegacy {
			logger.Printf("startup", "No legacy metadata indexes found (0x13-0x15), nothing to migrate")
			store.Close()
			return
		}
		if hasNew {
			logger.Printf("startup", "WARNING: New metadata indexes already exist (0x90-0x92)")
			logger.Printf("startup", "Run with --cleanup-metadata to remove legacy indexes after verifying migration")
		}

		logger.Printf("startup", "Migrating metadata indexes (0x13-0x15 → 0x90-0x92)...")
		start := time.Now()

		stats, err := internal.MigrateMetadataIndexes(store.DB())
		if err != nil {
			logger.Fatal("Migration failed: %v", err)
		}

		logger.Printf("startup", "Migration complete in %v", time.Since(start))
		logger.Printf("startup", "  Properties: %d", stats.PropertiesMigrated)
		logger.Printf("startup", "  WAL keys: %d", stats.WALKeysMigrated)
		logger.Printf("startup", "  TimeMap keys: %d", stats.TimeMapMigrated)
		logger.Printf("startup", "  Bytes read: %d, written: %d", stats.BytesRead, stats.BytesWritten)
		logger.Printf("startup", "")
		logger.Printf("startup", "Next steps:")
		logger.Printf("startup", "  1. Verify service works correctly")
		logger.Printf("startup", "  2. Run with --cleanup-metadata to remove legacy indexes")
		logger.Printf("startup", "  3. Run with --compact to reclaim space")
		store.Close()
		logger.Printf("startup", "Done")
		return
	}

	if cfg.CleanupMetadata {
		hasLegacy, hasNew := internal.CheckMetadataMigrationNeeded(store.DB())
		if !hasLegacy {
			logger.Printf("startup", "No legacy metadata indexes found (0x13-0x15), nothing to cleanup")
			store.Close()
			return
		}
		if !hasNew {
			logger.Fatal("New metadata indexes not found (0x90-0x92). Run --migrate-metadata first!")
		}

		logger.Printf("startup", "Deleting legacy metadata indexes (0x13-0x15)...")
		start := time.Now()

		stats, err := internal.CleanupLegacyMetadata(store.DB())
		if err != nil {
			logger.Fatal("Cleanup failed: %v", err)
		}

		logger.Printf("startup", "Cleanup complete in %v", time.Since(start))
		logger.Printf("startup", "  Properties deleted: %d", stats.PropertiesDeleted)
		logger.Printf("startup", "  WAL deleted: %d", stats.WALDeleted)
		logger.Printf("startup", "  TimeMap deleted: %d", stats.TimeMapDeleted)
		logger.Printf("startup", "  Bytes marked for deletion: %d", stats.BytesDeleted)
		logger.Printf("startup", "")
		logger.Printf("startup", "Run with --compact to reclaim disk space")
		store.Close()
		logger.Printf("startup", "Done")
		return
	}

	if cfg.RebuildMetadata {
		logger.Printf("startup", "Rebuilding chunk metadata from database...")
		start := time.Now()

		metadata, err := internal.RebuildChunkMetadataFromDB(store.DB())
		if err != nil {
			logger.Fatal("Metadata rebuild failed: %v", err)
		}

		logger.Printf("startup", "Rebuilt metadata: %d accounts", metadata.Stats())

		metadataPath := filepath.Join(cfg.IndexStorage, "chunk_metadata.bin")
		if err := metadata.SaveToFile(metadataPath); err != nil {
			logger.Fatal("Failed to save metadata: %v", err)
		}

		logger.Printf("startup", "Metadata rebuild complete in %v, saved to %s", time.Since(start), metadataPath)
		store.Close()
		logger.Printf("startup", "Done")
		return
	}

	indexes, err := internal.NewIndexes(store.DB(), cfg.IndexStorage)
	if err != nil {
		logger.Fatal("Failed to create indexes: %v", err)
	}

	if cfg.ReadOnly {
		indexes.SetBulkMode(false)
	}

	libNum, _, _ := indexes.GetProperties()
	startBlock := libNum + 1
	if startBlock == 1 {
		startBlock = 2
	}

	var syncReader corereader.Reader
	var queryReader corereader.Reader

	if strings.HasPrefix(cfg.HistorySource, "stream://") {
		logger.Fatal("history-source cannot be a stream:// URL. Use a filesystem path for queries, and set sync-source for streaming.")
	}
	if strings.HasPrefix(cfg.HistorySource, "http://") || strings.HasPrefix(cfg.HistorySource, "https://") {
		logger.Warning("history-source is an HTTP URL. This will be slower than a direct filesystem path.")
	}

	queryReaderOpts := corereader.QueryReaderOptions()
	queryReaderOpts.BlockCacheSizeMB = cfg.BlockCacheSizeMB
	queryReaderOpts.LRUCacheSlices = cfg.LRUCacheSlices
	if cfg.BlockIndexCache {
		queryReaderOpts.BlockIndexCachePath = filepath.Join(cfg.IndexStorage, "blockindex.cache")
	}
	queryOpenCfg := corereader.DefaultOpenConfig()
	queryOpenCfg.StartBlock = startBlock
	queryOpenCfg.SliceReaderOptions = &queryReaderOpts

	queryReader, err = corereader.Open(cfg.HistorySource, queryOpenCfg)
	if err != nil {
		logger.Fatal("Failed to open query reader: %v", err)
	}

	if cfg.SyncSource != "" {
		syncReaderOpts := corereader.SyncReaderOptions()
		syncOpenCfg := corereader.DefaultOpenConfig()
		syncOpenCfg.StartBlock = startBlock
		syncOpenCfg.SliceReaderOptions = &syncReaderOpts
		if unwrapper, ok := queryReader.(interface {
			GetBaseReader() corereader.BaseReader
		}); ok {
			syncOpenCfg.QueryReader = unwrapper.GetBaseReader()
		}

		syncReader, err = corereader.Open(cfg.SyncSource, syncOpenCfg)
		if err != nil {
			logger.Fatal("Failed to open sync reader: %v", err)
		}
	} else {
		syncReaderOpts := corereader.SyncReaderOptions()
		syncOpenCfg := corereader.DefaultOpenConfig()
		syncOpenCfg.StartBlock = startBlock
		syncOpenCfg.SliceReaderOptions = &syncReaderOpts

		syncReader, err = corereader.Open(cfg.HistorySource, syncOpenCfg)
		if err != nil {
			logger.Fatal("Failed to open sync reader: %v", err)
		}
	}

	var abiReader *abicache.Reader
	if cfg.ABISource != "" {
		abiReader, err = abicache.NewReader(cfg.ABISource)
		if err != nil {
			logger.Warning("Failed to create ABI reader: %v", err)
		}
	}

	libNum, headNum, err := indexes.GetProperties()
	if err != nil {
		logger.Printf("startup", "Fresh database, starting from block 2")
	} else {
		logger.Printf("startup", "Database state: LIB=%d HEAD=%d", libNum, headNum)
	}

	acceptHTTP.Store(true)

	rpcMux := http.NewServeMux()
	rpcMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleRPC(cfg, store, indexes, queryReader, abiReader, w, r)
	})
	httpServer := &http.Server{Handler: rpcMux}

	if cfg.HTTPListen != "none" {
		tcpListener := server.SocketListen(cfg.HTTPListen)
		go httpServer.Serve(tcpListener)
	}
	if cfg.HTTPSocket != "none" {
		unixListener := server.SocketListen(cfg.HTTPSocket)
		go httpServer.Serve(unixListener)
	}

	if cfg.MetricsListen != "none" && cfg.MetricsListen != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsListener := server.SocketListen(cfg.MetricsListen)
		go func() {
			if err := http.Serve(metricsListener, metricsMux); err != nil {
				logger.Printf("startup", "metrics server failed: %v", err)
			}
		}()
		logger.Printf("startup", "Metrics server listening on %s", cfg.MetricsListen)
	}

	var syncer *internal.Syncer
	var timing *internal.SyncTiming
	var broadcaster *internal.ActionBroadcaster
	var streamServer *internal.StreamServer
	if !cfg.ReadOnly {
		if cfg.Timing || cfg.Debug {
			timing = internal.NewSyncTiming()
			timing.SetStore(store)
			timing.StartReporter(3 * time.Second)
			logger.Printf("startup", "Sync timing metrics enabled (reporting every 3s)")

			indexes.SetTiming(timing)
		}
		broadcaster = internal.NewActionBroadcaster()

		if cfg.StreamListen != "none" || cfg.StreamSocket != "none" {
			var baseReader corereader.BaseReader
			if unwrapper, ok := queryReader.(interface {
				GetBaseReader() corereader.BaseReader
			}); ok {
				baseReader = unwrapper.GetBaseReader()
			}

			streamServer = internal.NewStreamServer(
				broadcaster,
				indexes,
				baseReader,
				abiReader,
				cfg.StreamMaxClients,
				cfg.StreamHeartbeatInterval,
			)

			if cfg.StreamSocket != "none" {
				if err := streamServer.StartTCP(cfg.StreamSocket); err != nil {
					logger.Fatal("Failed to start streaming TCP server: %v", err)
				}
			}
			if cfg.StreamListen != "none" {
				if err := streamServer.StartWebSocket(cfg.StreamListen); err != nil {
					logger.Fatal("Failed to start streaming WebSocket server: %v", err)
				}
			}

			streamServer.StartStatsLogger(30 * time.Second)
		}

		syncer = internal.NewSyncer(indexes, store, syncReader, cfg, timing, broadcaster)
		if err := syncer.Start(); err != nil {
			logger.Fatal("Failed to start syncer: %v", err)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Printf("startup", "Service running. Press Ctrl+C to stop.")
	<-sigChan

	logger.Printf("startup", "Shutting down...")
	acceptHTTP.Store(false)

	if cfg.Profile {
		profiler.Stop()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)

	if timing != nil {
		timing.Stop()
	}

	syncReader.Close()
	queryReader.Close()

	if syncer != nil {
		syncer.Stop()
	}

	if streamServer != nil {
		streamServer.Close()
	}

	if broadcaster != nil {
		broadcaster.Close()
	}

	logger.Printf("startup", "Closing indexes...")
	if err := indexes.Close(); err != nil {
		logger.Printf("startup", "Error closing indexes: %v", err)
	}

	logger.Printf("startup", "Closing database (may take time for pending compactions)...")
	store.Close()
	logger.Printf("startup", "Shutdown complete")
}

func handleRPC(
	cfg *internal.Config,
	store *internal.Store,
	indexes internal.ActionIndexer,
	reader corereader.Reader,
	abiReader *abicache.Reader,
	w http.ResponseWriter,
	r *http.Request,
) {
	defer func() {
		if rec := recover(); rec != nil {
			logger.Printf("http", "Recovered from panic in RPC handler: %v", rec)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
	}()

	if !acceptHTTP.Load() {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}

	path := r.URL.Path

	switch {
	case path == "/openapi.json" || path == "/openapi.yaml":
		internal.HandleOpenAPI(cfg.DebugEndpoints, w, r)
	case path == "/v1/history/get_actions":
		internal.HandleGetActions(cfg, store, indexes, reader, abiReader, w, r)
	case strings.HasPrefix(path, "/account/") && strings.HasSuffix(path, "/activity"):
		internal.HandleAccountActivity(cfg, store, indexes, reader, abiReader, w, r)
	case strings.HasPrefix(path, "/account/") && strings.HasSuffix(path, "/stats"):
		internal.HandleAccountStats(indexes, w, r)
	case strings.HasPrefix(path, "/debug/"):
		if !cfg.DebugEndpoints {
			http.Error(w, "debug endpoints not enabled (use --debug-endpoints)", http.StatusNotFound)
			return
		}
		handleDebugEndpoint(indexes, reader, w, r, path)
	default:
		http.Error(w, fmt.Sprintf("Unknown endpoint: %s", path), http.StatusNotFound)
	}
}

func handleDebugEndpoint(indexes internal.ActionIndexer, reader corereader.Reader, w http.ResponseWriter, r *http.Request, path string) {
	switch path {
	case "/debug/account":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugAccount(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/timemap":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugTimeMap(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/seq":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugSeqLookup(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/chunk":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugChunkNear(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/seq-to-date":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugSeqToDate(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/compare":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugCompareIndexes(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/read-chunk":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugReadChunk(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/wal":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugWALStatus(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/scan-contract-action":
		if idx, ok := indexes.(*internal.Indexes); ok {
			internal.HandleDebugScanContractAction(idx, w, r)
		} else {
			http.Error(w, "debug endpoints not available", http.StatusNotImplemented)
		}
	case "/debug/slice":
		if unwrapper, ok := reader.(interface {
			GetBaseReader() corereader.BaseReader
		}); ok {
			if sr, ok := unwrapper.GetBaseReader().(*corereader.SliceReader); ok {
				internal.HandleDebugSliceStats(sr, w, r)
				return
			}
		}
		http.Error(w, "slice debug only available with local reader", http.StatusNotImplemented)
	case "/debug/block":
		internal.HandleDebugBlock(reader, w, r)
	case "/debug/action":
		internal.HandleDebugAction(reader, w, r)
	case "/debug/search-account-in-block":
		internal.HandleDebugSearchAccountInBlock(reader, w, r)
	case "/debug/filter-block":
		internal.HandleDebugFilterBlock(reader, w, r)
	default:
		http.Error(w, fmt.Sprintf("Unknown debug endpoint: %s", path), http.StatusNotFound)
	}
}
