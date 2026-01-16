package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/greymass/roborovski/libraries/config"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/server"
	"github.com/greymass/roborovski/services/apiproxy/internal/docs"
	"github.com/greymass/roborovski/services/apiproxy/internal/middleware"
	"github.com/greymass/roborovski/services/apiproxy/internal/openapi"
	"github.com/greymass/roborovski/services/apiproxy/internal/proxy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sony/gobreaker"
)

var Version = "dev"

var (
	productionCategories = []string{"startup", "http", "proxy", "circuit"}
	debugCategories      = []string{"debug", "debug-proxy", "debug-cache"}
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

	if cfg.Workers > 0 {
		runtime.GOMAXPROCS(cfg.Workers)
	}

	cfMiddleware, err := middleware.NewCloudflareMiddleware(cfg.UseCloudflareHeaders, cfg.RequireCloudflareSource)
	if err != nil {
		logger.Fatal("Failed to initialize Cloudflare middleware: %v", err)
	}

	rateLimiter := middleware.NewRateLimiter(cfg.GlobalRateLimit, cfg.GlobalRateLimit*2)

	proxyHandler := proxy.NewProxy(cfg.GlobalCacheDuration > 0)

	openapiAggregator := openapi.NewAggregator(
		"Roborovski API",
		"Unified blockchain history API aggregating multiple backend services",
		Version,
		60*time.Second,
	)

	for _, route := range cfg.Routes {
		cbSettings := gobreaker.Settings{
			Name:        route.Path,
			MaxRequests: uint32(cfg.CircuitBreakerMaxRequests),
			Interval:    time.Duration(cfg.CircuitBreakerTimeout) * time.Second,
			Timeout:     time.Duration(cfg.CircuitBreakerTimeout) * time.Second,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
				return counts.Requests >= 3 && failureRatio >= 0.6
			},
		}

		validatorCfg := proxy.GetDefaultValidator(route.Path)

		if err := proxyHandler.AddBackend(route.Path, route.Backend, route.Timeout, route.CacheDuration, validatorCfg, cbSettings); err != nil {
			logger.Fatal("Failed to add backend for %s: %v", route.Path, err)
		}

		openapiAggregator.AddRoute(route.Path, route.Backend)

		cacheStatus := "disabled"
		if route.CacheDuration > 0 {
			cacheStatus = fmt.Sprintf("%ds", route.CacheDuration)
		}
		validationStatus := "disabled"
		if validatorCfg != nil {
			validationStatus = "enabled"
		}
		logger.Printf("startup", "Registered route: %s -> %s (cache: %s, validation: %s)", route.Path, route.Backend, cacheStatus, validationStatus)
	}

	mux := http.NewServeMux()

	mux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK\n")
	}))

	mux.Handle("/openapi.json", openapiAggregator.Handler())
	mux.Handle("/openapi.yaml", openapiAggregator.Handler())

	docsHandler := docs.Handler(docs.Config{
		Title:       "Roborovski API",
		Description: "Unified blockchain history API aggregating multiple backend services",
		SpecURL:     "/openapi.json",
		Theme:       "default",
		DarkMode:    true,
	})
	mux.Handle("/docs", docsHandler)
	mux.Handle("/docs/", docsHandler)

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/docs", http.StatusFound)
			return
		}
		proxyHandler.ServeHTTP(w, r)
	}))

	handler := middleware.CorrelationMiddleware(
		cfMiddleware.Middleware(
			middleware.LoggingMiddleware(
				rateLimiter.Middleware(mux),
			),
		),
	)

	httpServer := &http.Server{
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
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

	logger.Printf("startup", "Starting apiproxy %s", Version)

	if cfg.HTTPListen != "none" && cfg.HTTPListen != "" {
		tcpListener := server.SocketListen(cfg.HTTPListen)
		go httpServer.Serve(tcpListener)
		logger.Printf("startup", "  http-listen: %s", cfg.HTTPListen)
	}
	if cfg.HTTPSocket != "none" && cfg.HTTPSocket != "" {
		unixListener := server.SocketListen(cfg.HTTPSocket)
		go httpServer.Serve(unixListener)
		logger.Printf("startup", "  http-socket: %s", cfg.HTTPSocket)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	logger.Printf("startup", "Shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Printf("startup", "Server shutdown error: %v", err)
	}

	logger.Printf("startup", "Server stopped")
}
