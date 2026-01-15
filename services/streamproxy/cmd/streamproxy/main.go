package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"time"

	"github.com/greymass/roborovski/libraries/config"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/profiler"
	"github.com/greymass/roborovski/services/streamproxy/internal/backend"
	"github.com/greymass/roborovski/services/streamproxy/internal/metrics"
	"github.com/greymass/roborovski/services/streamproxy/internal/proxy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Version = "dev"

var (
	productionCategories = []string{"startup", "stream", "health"}
	debugCategories      = []string{"debug", "debug-proxy"}
	allCategories        = append(append([]string{}, productionCategories...), debugCategories...)
)

func main() {
	config.CheckVersion(Version)

	cfg, err := loadConfig("")
	if err != nil {
		logger.Fatal("Failed to load config: %v", err)
	}

	logger.RegisterCategories(allCategories...)
	if cfg.Debug {
		logger.SetMinLevel(logger.LevelDebug)
		logger.SetCategoryFilter(nil)
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

	if cfg.Debug {
		logger.Printf("startup", "Debug mode enabled")
	}

	if cfg.GOGC > 0 {
		debug.SetGCPercent(cfg.GOGC)
	}

	if cfg.PprofPort != "" {
		go func() {
			pprofAddr := "localhost:" + cfg.PprofPort
			logger.Printf("startup", "Starting pprof server on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				logger.Printf("startup", "pprof server failed: %v", err)
			}
		}()
	}

	if cfg.Profile {
		profiler.Start(profiler.Config{
			ServiceName: "streamproxy",
			Interval:    time.Duration(cfg.ProfileInterval) * time.Second,
		})
		defer profiler.Stop()
	}

	be := backend.New(backend.Config{
		Address:             cfg.ActionIndex.Address,
		HealthCheckInterval: cfg.HealthCheckInterval,
		HealthCheckTimeout:  cfg.HealthCheckTimeout,
	})

	logger.Printf("startup", "ActionIndex: %s", cfg.ActionIndex.Address)

	be.Start()
	defer be.Close()

	go healthMetricsUpdater(be)

	var tcpProxy *proxy.TCPProxy
	var wsProxy *proxy.WebSocketProxy

	if cfg.TCPListen != "" {
		tcpProxy = proxy.NewTCPProxy(be, proxy.TCPProxyConfig{
			MaxConnections:    cfg.MaxConnections,
			ConnectionTimeout: cfg.ConnectionTimeout,
		})

		if err := tcpProxy.Listen(cfg.TCPListen); err != nil {
			logger.Fatal("Failed to start TCP listener on %s: %v", cfg.TCPListen, err)
		}
		defer tcpProxy.Close()

		logger.Printf("startup", "TCP listener started on %s", cfg.TCPListen)
	}

	if cfg.WebSocketListen != "" {
		wsProxy = proxy.NewWebSocketProxy(be, proxy.WebSocketProxyConfig{
			MaxConnections:    cfg.MaxConnections,
			ConnectionTimeout: cfg.ConnectionTimeout,
		})

		if err := wsProxy.Listen(cfg.WebSocketListen); err != nil {
			logger.Fatal("Failed to start WebSocket listener on %s: %v", cfg.WebSocketListen, err)
		}
		defer wsProxy.Close()

		logger.Printf("startup", "WebSocket listener started on %s", cfg.WebSocketListen)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if be.IsHealthy() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK\n")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "UNHEALTHY\n")
		}

		tcpConns := 0
		wsConns := 0
		if tcpProxy != nil {
			tcpConns = tcpProxy.GetConnectionCount()
		}
		if wsProxy != nil {
			wsConns = wsProxy.GetConnectionCount()
		}
		fmt.Fprintf(w, "actionindex: %s\n", cfg.ActionIndex.Address)
		fmt.Fprintf(w, "tcp_connections: %d\n", tcpConns)
		fmt.Fprintf(w, "ws_connections: %d\n", wsConns)
		fmt.Fprintf(w, "total_connections: %d/%d\n", tcpConns+wsConns, cfg.MaxConnections)
	})

	mux.Handle("/metrics", promhttp.Handler())

	metricsServer := &http.Server{
		Addr:         cfg.MetricsListen,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Printf("startup", "Metrics server started on %s", cfg.MetricsListen)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("startup", "Metrics server error: %v", err)
		}
	}()

	logger.Printf("startup", "streamproxy %s started", Version)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	logger.Printf("startup", "Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	metricsServer.Shutdown(ctx)

	logger.Printf("startup", "Shutdown complete")
}

func healthMetricsUpdater(be *backend.Backend) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		metrics.UpdateBackendHealth("backend", be.IsHealthy())
	}
}
