package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/greymass/roborovski/services/coreverify/internal"
)

var Version = "dev"

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-version" {
			fmt.Println(Version)
			os.Exit(0)
		}
	}

	dataPath := flag.String("data-path", "", "Path to coreindex storage (required)")
	workers := flag.Int("workers", 4, "Number of parallel workers")
	full := flag.Bool("full", false, "Enable full data validation (decompress + parse blocks)")
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

	// Create validator
	validator := internal.NewValidator(*dataPath, *workers, *full, *repair, *debug, *outputJSON)

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
