package main

import (
	"flag"
	"fmt"
	"os"

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

	dataPath := flag.String("data-path", "", "Path to coreindex storage (required)")
	workers := flag.Int("workers", 4, "Number of parallel workers")
	batchSize := flag.Int("batch-size", 1000, "Blocks per batch when reading (higher = faster, more memory)")
	full := flag.Bool("full", false, "Enable full data validation (decompress + parse blocks)")
	checkOnblock := flag.Bool("check-onblock", false, "Check each block contains an eosio::onblock action")
	maxBlocks := flag.Int("max-blocks", 0, "Maximum number of blocks to verify (0 = all)")
	repair := flag.Bool("repair", false, "Repair mode: rebuild corrupt indices (DANGEROUS)")
	outputJSON := flag.Bool("output-json", false, "Output JSON report instead of human-readable")
	debug := flag.Bool("debug", false, "Enable debug logging")
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

	// Create validator
	validator := internal.NewValidator(*dataPath, *workers, *batchSize, *full, *checkOnblock, *maxBlocks, *repair, *debug, *outputJSON)

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
