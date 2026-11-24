package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/services/coreverify/internal"
)

var Version = "dev"

// All categories for column alignment (includes corereader categories)
var logCategories = []string{
	"info", "progress", "result", "summary", "error", "warning",
	"config", "startup", "stats", "live", "buffer", "stream",
	"debug", "debug-timing", "debug-perf",
}

// Categories to show by default (excludes corereader's periodic stats/live messages)
var defaultCategories = []string{
	"info", "progress", "result", "summary", "error", "warning", "config",
}

func main() {
	// Register log categories for column alignment
	logger.RegisterCategories(logCategories...)
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-version" {
			fmt.Println(Version)
			os.Exit(0)
		}
	}

	if len(os.Args) > 1 && os.Args[1] == "live-probe" {
		os.Exit(runLiveProbe(os.Args[2:]))
	}

	dataPath := flag.String("data-path", "", "Path to coreindex storage (required)")
	workers := flag.Int("workers", 4, "Number of parallel workers")
	batchSize := flag.Int("batch-size", 1000, "Blocks per batch when reading (higher = faster, more memory)")
	full := flag.Bool("full", false, "Enable full data validation (decompress + parse blocks)")
	checkOnblock := flag.Bool("check-onblock", false, "Check each block contains an eosio::onblock action")
	maxBlocks := flag.Int("max-blocks", 0, "Maximum number of blocks to verify (0 = all)")
	outputJSON := flag.Bool("output-json", false, "Output JSON report instead of human-readable")
	debug := flag.Bool("debug", false, "Enable debug logging")
	comparePath := flag.String("compare-path", "", "Compare --data-path against this second store instead of validating")
	abiPath := flag.String("abi-path", "", "ABI cache beside --data-path (compare mode)")
	compareABIPath := flag.String("compare-abi-path", "", "ABI cache beside --compare-path (compare mode)")
	startBlock := flag.Uint("start-block", 0, "First block to compare (0 = first block of --data-path)")
	endBlock := flag.Uint("end-block", 0, "Last block to compare (0 = last block of --data-path)")
	ignoreElapsed := flag.Bool("ignore-elapsed", false, "Compare mode: exclude each action's measured execution time, which differs between any two executions of a block")
	staleEmptyData := flag.Bool("stale-empty-data", false, "Compare mode: accept an action whose --data-path data is stale where the compare store's is empty, and a block whose data table differs only in unused or reordered entries, as trace ingestion's decoder wrote them, and count each separately")
	abiSnapshotBlock := flag.Uint("abi-snapshot-block", 0, "Snapshot height whose --compare-abi-path entries are listed for a get_abi check instead of compared")
	flag.Parse()

	// Validate required flags
	if *dataPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --data-path is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// Check if path exists
	if _, err := os.Stat(*dataPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Storage path does not exist: %s\n", *dataPath)
		os.Exit(1)
	}

	// Configure logging
	if *outputJSON {
		// Suppress all logs when outputting JSON for clean parseable output
		logger.SetMinLevel(logger.LevelFatal)
	} else if *debug {
		// Show all categories including corereader internals
		logger.SetMinLevel(logger.LevelDebug)
	} else {
		// Show only coreverify categories + config, block corereader's periodic stats/live
		logger.SetMinLevel(logger.LevelWarning)
		logger.SetCategoryFilter(defaultCategories)
	}

	if *comparePath != "" {
		if _, err := os.Stat(*comparePath); err != nil {
			fmt.Fprintf(os.Stderr, "Error: Compare path does not exist: %s\n", *comparePath)
			os.Exit(1)
		}
		comparer := internal.NewComparer(internal.CompareConfig{
			StorePath:        *dataPath,
			ComparePath:      *comparePath,
			ABIPath:          *abiPath,
			CompareABIPath:   *compareABIPath,
			StartBlock:       uint32(*startBlock),
			EndBlock:         uint32(*endBlock),
			ABISnapshotBlock: uint32(*abiSnapshotBlock),
			Workers:          *workers,
			BatchSize:        *batchSize,
			OutputJSON:       *outputJSON,
			IgnoreElapsed:    *ignoreElapsed,
			StaleEmptyData:   *staleEmptyData,
		})
		report, err := comparer.Compare()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Comparison failed: %v\n", err)
			os.Exit(2)
		}
		comparer.PrintReport(report)
		if report.Status == "failed" {
			os.Exit(2)
		}
		os.Exit(0)
	}

	// Create validator
	validator := internal.NewValidator(*dataPath, *workers, *batchSize, *full, *checkOnblock, *maxBlocks, *debug, *outputJSON)

	// Run validation
	report, err := validator.ValidateStorage()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Validation failed: %v\n", err)
		os.Exit(2)
	}

	// Print report
	validator.PrintReport(report)

	// Exit with appropriate code
	if report.Status == "failed" {
		os.Exit(2)
	}
	os.Exit(0)
}

func runLiveProbe(args []string) int {
	fs := flag.NewFlagSet("live-probe", flag.ExitOnError)
	cfg := internal.ProbeConfig{}
	fs.StringVar(&cfg.WriterURL, "writer-url", "http://127.0.0.1:8888", "nodeos HTTP address serving /v1/roborovski_writer/recent")
	fs.StringVar(&cfg.NodeosURL, "nodeos-url", "", "nodeos HTTP address for get_info (default --writer-url)")
	fs.StringVar(&cfg.DataPath, "data-path", "", "Core storage to read through corereader (local files path)")
	fs.StringVar(&cfg.CoreindexURL, "coreindex-url", "", "coreindex HTTP address (HTTP and ABI paths)")
	fs.StringVar(&cfg.StreamAddress, "stream-address", "", "coreindex corestream address, host:port or a .sock path")
	fs.StringVar(&cfg.RecordsPath, "records", "", "Write one JSON record per block per path to this file")
	fs.DurationVar(&cfg.Duration, "duration", 0, "Stop after this long (0 = until interrupted)")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", 50*time.Millisecond, "Polling interval for the writer ring and the polled paths")
	fs.DurationVar(&cfg.LateAfter, "late-after", time.Second, "Latency above which a block is late")
	fs.DurationVar(&cfg.MissingAfter, "missing-after", 60*time.Second, "Time after notification at which an unseen block is missing")
	fs.DurationVar(&cfg.BehindAfter, "behind-after", 5*time.Second, "nodeos head lag above which a block is counted as behind")
	fs.DurationVar(&cfg.RestartGrace, "restart-grace", 10*time.Minute, "Window after a restart whose blocks are counted separately")
	fs.BoolVar(&cfg.OutputJSON, "output-json", false, "Print the report as JSON")
	fs.Parse(args)

	if cfg.OutputJSON {
		logger.SetMinLevel(logger.LevelFatal)
	} else {
		logger.SetMinLevel(logger.LevelWarning)
		logger.SetCategoryFilter([]string{"error", "warning"})
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }()
	probe := internal.NewLiveProbe(cfg)
	report, err := probe.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Live probe failed: %v\n", err)
		return 1
	}
	probe.PrintReport(report)
	if report.Status != "passed" {
		return 2
	}
	return 0
}
