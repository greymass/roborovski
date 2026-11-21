package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/corestream"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/server"
	"github.com/greymass/roborovski/services/coreindex/internal/follower"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const followerPollInterval = 100 * time.Millisecond

func refusedFollowerFlags(cfg *Config) []string {
	var refused []string
	add := func(on bool, flag string) {
		if on {
			refused = append(refused, flag)
		}
	}
	add(cfg.Replay, "--replay")
	add(cfg.Verify, "--verify")
	add(cfg.VerifyOnly, "--verify-only")
	add(cfg.Repair, "--repair")
	add(cfg.RepairSlice >= 0, "--repair-slice")
	add(cfg.RepairAll, "--repair-all")
	add(cfg.RebuildABIs, "--rebuild-abis")
	add(cfg.SliceDownloadURL != "", "--slice-download-url")
	add(cfg.TraceCleanupMode != "", "--trace-cleanup-mode")
	add(cfg.StartBlock > 0, "--start-block")
	return refused
}

type abiHolder struct {
	p atomic.Pointer[abicache.Reader]
}

func (h *abiHolder) get() *abicache.Reader { return h.p.Load() }

// open opens the ABI cache once its files exist and bounds it by the follower's manifest.
func (h *abiHolder) open(path string, store *follower.Store) {
	if h.p.Load() != nil || store.Manifest() == nil {
		return
	}
	r, err := abicache.NewReader(path)
	if err != nil {
		return
	}
	limit := func() (uint32, bool) {
		if m := store.Manifest(); m != nil {
			return m.ABIIndexLen, true
		}
		return 0, true
	}
	if err := r.SetLimit(limit); err != nil {
		r.Close()
		logger.Printf("warning", "ABI cache %s: %v", path, err)
		return
	}
	h.p.Store(r)
	logger.Printf("startup", "ABI cache: %s (bounded by the manifest)", path)
}

func runFollower(cfg *Config, serverConfig *server.Config) {
	if refused := refusedFollowerFlags(cfg); len(refused) > 0 {
		logger.Fatal("--read-only follows storage another process writes; it refuses %s", strings.Join(refused, ", "))
	}
	abiPath := cfg.ABIPath
	if abiPath == "" {
		abiPath = filepath.Join(cfg.Path, "abis")
	}
	if _, err := os.Stat(cfg.Path); err != nil {
		logger.Fatal("Storage directory %s: %v", cfg.Path, err)
	}

	logger.Println("startup", "Follower mode (--read-only):")
	logger.Printf("startup", "  Core storage: %s (written by another process)", cfg.Path)
	logger.Printf("startup", "  ABI cache:    %s", abiPath)
	logger.Printf("startup", "  Published:    %s, polled every %v", coremanifest.PublishedPath(cfg.Path), followerPollInterval)

	opts := corereader.QueryReaderOptions()
	opts.DisableStatsReporter = true
	store := follower.NewStore(cfg.Path, opts, follower.Hooks{})
	defer store.Close()

	var abis abiHolder
	defer func() {
		if r := abis.get(); r != nil {
			r.Close()
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		doHandleRPC(serverConfig, store, abis.get(), w, r)
	})
	var httpListeners []string
	for _, addr := range []string{cfg.HTTPListen, cfg.HTTPSocket} {
		if addr == "" || addr == "none" {
			continue
		}
		listener := server.SocketListen(addr)
		httpListeners = append(httpListeners, addr)
		go func(a string) {
			if err := http.Serve(listener, nil); err != nil {
				logger.Printf("http", "HTTP server on %s: %v", a, err)
			}
		}(addr)
	}
	if len(httpListeners) > 0 {
		serverConfig.AcceptHTTP = true
		logger.Printf("http", "HTTP API ready on %s", strings.Join(httpListeners, ", "))
	}
	if cfg.MetricsListen != "none" && cfg.MetricsListen != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsListener := server.SocketListen(cfg.MetricsListen)
		go func() {
			if err := http.Serve(metricsListener, metricsMux); err != nil {
				logger.Printf("http", "metrics server failed: %v", err)
			}
		}()
		logger.Printf("http", "Metrics server listening on %s", cfg.MetricsListen)
	}

	var streamServer *corestream.Server
	if cfg.StreamEnabled {
		streamServer = corestream.NewServer(&streamBlockProvider{store: store}, corestream.ServerConfig{
			MaxClients:        cfg.StreamMaxClients,
			HeartbeatInterval: 5 * time.Second,
			Debug:             cfg.Debug,
		})
		for _, addr := range []string{cfg.StreamListen, cfg.StreamSocket} {
			if addr == "" || addr == "none" {
				continue
			}
			if err := streamServer.Listen(addr); err != nil {
				logger.Fatal("Failed to start stream server on %s: %v", addr, err)
			}
		}
		defer streamServer.Close()
	}
	store.SetHooks(followerHooks(streamServer))

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	signal.Ignore(syscall.SIGPIPE)

	ticker := time.NewTicker(followerPollInterval)
	defer ticker.Stop()
	var lastErr string
	for {
		select {
		case sig := <-signalChan:
			logger.Printf("shutdown", "Received %v, shutting down", sig)
			return
		case <-ticker.C:
		}
		err := store.Poll()
		if err != nil && err.Error() != lastErr {
			logger.Printf("warning", "Follower poll: %v", err)
		}
		lastErr = fmt.Sprint(err)
		if err == nil {
			lastErr = ""
		}
		abis.open(abiPath, store)
	}
}

func followerHooks(streamServer *corestream.Server) follower.Hooks {
	if streamServer == nil {
		return follower.Hooks{}
	}
	return follower.Hooks{
		Broadcast: streamServer.Broadcast,
		EpochChanged: func(m *coremanifest.Manifest) {
			n := streamServer.DisconnectAll(fmt.Sprintf("store reset to block %d (epoch %d)", m.Head, m.Epoch))
			logger.Printf("stream", "Disconnected %d stream clients after a reset to block %d", n, m.Head)
		},
		SetLive: func(live bool) {
			if streamServer.IsLiveMode() != live {
				streamServer.SetLiveMode(live)
			}
		},
	}
}
