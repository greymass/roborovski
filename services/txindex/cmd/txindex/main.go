package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/config"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/profiler"
	"github.com/greymass/roborovski/libraries/server"
	"github.com/greymass/roborovski/services/txindex/internal"
)

var Version = "dev"

func main() {
	config.CheckVersion(Version)

	cfg := &internal.Config{}
	if err := config.Load(cfg, os.Args[1:]); err != nil {
		logger.Fatal("Config error: %v", err)
	}

	logger.RegisterCategories("startup", "sync", "http", "debug", "timing", "debug-timing", "debug-query", "stats")
	if cfg.Debug {
		logger.SetCategoryFilter(nil)
		logger.SetMinLevel(logger.LevelDebug)
	} else if cfg.QueryTrace {
		logger.SetMinLevel(logger.LevelDebug)
		filter := append([]string{}, cfg.LogFilter...)
		filter = append(filter, "debug-query")
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

	runtime.GOMAXPROCS(cfg.Workers)

	logger.Printf("startup", "txindex %s starting...", Version)

	if err := internal.InitOpenAPI(Version); err != nil {
		logger.Fatal("Failed to load OpenAPI spec: %v", err)
	}
	if err := internal.ValidateOpenAPIRoutes(); err != nil {
		logger.Fatal("%v", err)
	}

	logger.Printf("startup", "Storage:")
	logger.Printf("startup", "  index-path: %s", cfg.IndexPath)
	logger.Printf("startup", "  history-source: %s", cfg.HistorySource)
	if cfg.ABIPath != "" {
		logger.Printf("startup", "  abi-path: %s", cfg.ABIPath)
	} else {
		logger.Printf("startup", "  abi-path: (not set, action data will not be decoded)")
	}

	logger.Printf("startup", "Indexing:")
	logger.Printf("startup", "  workers: %d", cfg.Workers)
	logger.Printf("startup", "  block-cache-mb: %d", cfg.BlockCacheMB)
	logger.Printf("startup", "  lru-cache-slices: %d", cfg.LRUCacheSlices)
	logger.Printf("startup", "  skip-onblock: %v", cfg.SkipOnblock)

	logger.Printf("startup", "API:")
	if cfg.HTTPListen != "none" {
		logger.Printf("startup", "  http-listen: %s", cfg.HTTPListen)
	}
	if cfg.HTTPSocket != "none" {
		logger.Printf("startup", "  http-socket: %s", cfg.HTTPSocket)
	}

	if cfg.Debug || cfg.PprofPort != "" || cfg.QueryTrace {
		logger.Printf("startup", "Debugging:")
		if cfg.Debug {
			logger.Printf("startup", "  debug: %v", cfg.Debug)
		}
		if cfg.QueryTrace {
			logger.Printf("startup", "  query-trace: enabled (index performance per request)")
		}
		if cfg.PprofPort != "" {
			logger.Printf("startup", "  pprof-port: %s", cfg.PprofPort)
		}
	}

	logger.Println("startup", "")

	if cfg.PprofPort != "" {
		go func() {
			addr := "localhost:" + cfg.PprofPort
			logger.Printf("startup", "Starting pprof on %s", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				logger.Printf("startup", "pprof server error: %v", err)
			}
		}()
	}

	if cfg.Profile {
		profiler.EnableBlockProfiling()
		profiler.Start(profiler.Config{
			ServiceName: "txindex",
			Interval:    time.Duration(cfg.ProfileInterval) * time.Second,
		})
	}

	readerOpts := corereader.QueryReaderOptions()
	readerOpts.BlockCacheSizeMB = int64(cfg.BlockCacheMB)
	readerOpts.LRUCacheSlices = cfg.LRUCacheSlices

	openCfg := corereader.DefaultOpenConfig()
	openCfg.StartBlock = 2
	openCfg.SliceReaderOptions = &readerOpts

	reader, err := corereader.Open(cfg.HistorySource, openCfg)
	if err != nil {
		logger.Fatal("Failed to open history source: %v", err)
	}
	defer reader.Close()

	idx, err := internal.NewTrxIndex(cfg.IndexPath, reader)
	if err != nil {
		logger.Fatal("Failed to create transaction index: %v", err)
	}
	defer idx.Close()

	meta := idx.GetMeta()
	logger.Printf("startup", "Index state: mode=%s lastIndexed=%d lastMerged=%d bulkRuns=%d bulkLastBlock=%d",
		meta.Mode, meta.LastIndexedBlock, meta.LastMergedBlock, meta.BulkRunCount, meta.BulkLastBlock)

	_, chainLIB, err := reader.GetStateProps(true)
	if err != nil {
		logger.Fatal("Failed to get chain state: %v", err)
	}
	logger.Printf("startup", "Chain state: LIB=%d", chainLIB)

	if meta.Mode == "bulk" {
		startBlock := meta.LastIndexedBlock + 1
		if meta.BulkLastBlock > 0 {
			startBlock = meta.BulkLastBlock + 1
		}

		if startBlock <= chainLIB {
			logger.Printf("startup", "Starting bulk sync from block %d to %d", startBlock, chainLIB)
			exit := false

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigChan
				logger.Printf("startup", "Signal received, stopping bulk sync...")
				exit = true
			}()

			bulkIndexer := internal.NewBulkIndexer(idx, reader, cfg)
			if err := bulkIndexer.ProcessRange(startBlock, chainLIB, &exit); err != nil {
				logger.Fatal("Bulk sync failed: %v", err)
			}

			if exit {
				logger.Printf("startup", "Bulk sync interrupted, saving progress...")
				idx.SaveMeta()
				logger.Printf("startup", "Progress saved. Restart to continue.")
				return
			}

			if err := bulkIndexer.Finalize(); err != nil {
				logger.Fatal("Bulk sync finalize failed: %v", err)
			}
			idx.SetMode("live")
			idx.SaveMeta()
			logger.Printf("startup", "Bulk sync complete, switching to live mode")

			signal.Stop(sigChan)
		} else {
			logger.Printf("startup", "Bulk sync already complete, finalizing...")
			bulkIndexer := internal.NewBulkIndexer(idx, reader, cfg)
			if err := bulkIndexer.Finalize(); err != nil {
				logger.Fatal("Bulk sync finalize failed: %v", err)
			}
			idx.SetMode("live")
			idx.SaveMeta()
			logger.Printf("startup", "Finalize complete, switching to live mode")
		}
	}

	meta = idx.GetMeta()
	startBlock := meta.LastIndexedBlock + 1
	if startBlock == 1 {
		startBlock = 2
	}

	reader.SetCurrentBlock(startBlock)

	blockSource := internal.NewPollingBlockSource(reader, startBlock, cfg.SkipOnblock)
	logger.Printf("startup", "Using polling sync (100ms interval)")

	liveSyncer := internal.NewLiveSyncer(idx, blockSource, cfg)
	if err := liveSyncer.Start(); err != nil {
		logger.Fatal("Failed to start live syncer: %v", err)
	}

	var abiReader *abicache.Reader
	if cfg.ABIPath != "" {
		abiReader, err = abicache.NewReader(cfg.ABIPath)
		if err != nil {
			logger.Printf("startup", "Warning: Failed to open ABI reader: %v (action decoding disabled)", err)
		} else {
			defer abiReader.Close()
			logger.Printf("startup", "ABI reader initialized from %s", cfg.ABIPath)
		}
	}

	rpcServer := internal.NewRPCServer(idx, reader, abiReader, cfg)

	if cfg.HTTPListen != "none" {
		tcpListener := server.SocketListen(cfg.HTTPListen)
		defer tcpListener.Close()
		go func() {
			logger.Printf("startup", "RPC server listening on %s", cfg.HTTPListen)
			if err := http.Serve(tcpListener, rpcServer); err != nil && err != http.ErrServerClosed {
				logger.Printf("http", "RPC server error: %v", err)
			}
		}()
	}
	if cfg.HTTPSocket != "none" {
		unixListener := server.SocketListen(cfg.HTTPSocket)
		defer unixListener.Close()
		go func() {
			logger.Printf("startup", "RPC server listening on %s", cfg.HTTPSocket)
			if err := http.Serve(unixListener, rpcServer); err != nil && err != http.ErrServerClosed {
				logger.Printf("http", "RPC server error: %v", err)
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Printf("startup", "Service running. Press Ctrl+C to stop.")
	<-sigChan

	logger.Printf("startup", "Shutting down...")
	liveSyncer.Stop()
	if cfg.Profile {
		profiler.Stop()
	}
	logger.Printf("startup", "Shutdown complete")
}
