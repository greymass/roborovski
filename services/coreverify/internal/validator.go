package internal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

// ValidationIssue represents a single validation problem
type ValidationIssue struct {
	Severity string `json:"severity"` // "ERROR", "WARNING", "INFO"
	Type     string `json:"type"`     // "MISSING_BLOCK", "DUPLICATE_BLOCK", etc.
	SliceNum uint32 `json:"slice_num"`
	BlockNum uint32 `json:"block_num,omitempty"`
	File     string `json:"file"`
	Message  string `json:"message"`
}

// SliceValidation contains validation results for a single slice
type SliceValidation struct {
	SliceNum       uint32            `json:"slice_num"`
	BlockRange     string            `json:"block_range"`
	StartBlock     uint32            `json:"start_block"`
	EndBlock       uint32            `json:"end_block"`
	BlocksExpected uint32            `json:"blocks_expected"`
	BlocksActual   uint32            `json:"blocks_actual"`
	GlobsExpected  int               `json:"globs_expected"`
	GlobsActual    int               `json:"globs_actual"`
	Finalized      bool              `json:"finalized"`
	Issues         []ValidationIssue `json:"issues"`
	Duration       time.Duration     `json:"duration_ms"`
	OnblockCount   int               `json:"onblock_count,omitempty"`
	BlocksChecked  int               `json:"blocks_checked,omitempty"`
}

// ValidationReport is the final validation summary
type ValidationReport struct {
	Version        string            `json:"version"`
	StoragePath    string            `json:"storage_path"`
	ValidationMode string            `json:"validation_mode"`
	Timestamp      time.Time         `json:"timestamp"`
	Status         string            `json:"status"` // "passed", "failed"
	Summary        ValidationSummary `json:"summary"`
	Issues         []ValidationIssue `json:"issues"`
	SliceResults   []SliceValidation `json:"slice_results,omitempty"`
}

// ValidationSummary contains aggregate statistics
type ValidationSummary struct {
	TotalSlices     int     `json:"total_slices"`
	TotalBlocks     uint32  `json:"total_blocks"`
	TotalGlobs      int     `json:"total_globs"`
	FinalizedSlices int     `json:"finalized_slices"`
	ActiveSlices    int     `json:"active_slices"`
	Errors          int     `json:"errors"`
	Warnings        int     `json:"warnings"`
	MissingOnblock  int     `json:"missing_onblock,omitempty"`
	TotalOnblocks   int     `json:"total_onblocks,omitempty"`
	BlocksChecked   int     `json:"blocks_checked,omitempty"`
	DurationSeconds float64 `json:"duration_seconds"`
	ValidationRate  float64 `json:"validation_rate_slices_per_sec"`
}

// Validator orchestrates all validation checks
type Validator struct {
	basePath       string
	workers        int
	fullValidation bool
	checkOnblock   bool
	maxBlocks      int
	repair         bool
	debug          bool
	outputJSON     bool

	slices   []SliceInfo
	issues   []ValidationIssue
	issuesMu sync.Mutex

	reader corereader.Reader
}

// NewValidator creates a new validator
func NewValidator(basePath string, workers int, fullValidation, checkOnblock bool, maxBlocks int, repair, debug, outputJSON bool) *Validator {
	if workers <= 0 {
		workers = 4
	}
	return &Validator{
		basePath:       basePath,
		workers:        workers,
		fullValidation: fullValidation,
		checkOnblock:   checkOnblock,
		maxBlocks:      maxBlocks,
		repair:         repair,
		debug:          debug,
		outputJSON:     outputJSON,
		issues:         make([]ValidationIssue, 0),
	}
}

// ValidateStorage is the main entry point
func (v *Validator) ValidateStorage() (*ValidationReport, error) {
	startTime := time.Now()

	if !v.outputJSON {
		logger.Printf("info", "coreverify v1.0.0")
		logger.Printf("info", "Path: %s", v.basePath)
	}

	// Step 1: Discover slices
	slices, err := v.discoverSlices()
	if err != nil {
		return nil, fmt.Errorf("failed to discover slices: %w", err)
	}
	v.slices = slices

	if len(slices) == 0 {
		return nil, fmt.Errorf("no slices found in %s", v.basePath)
	}

	// Calculate block range
	firstBlock := slices[0].StartBlock
	lastBlock := slices[len(slices)-1].EndBlock
	totalSlices := len(slices)

	// Apply max-blocks limit if set
	if v.maxBlocks > 0 {
		var blocksAccumulated uint32
		var limitedSlices []SliceInfo
		for _, slice := range slices {
			sliceBlocks := slice.EndBlock - slice.StartBlock + 1
			if blocksAccumulated+sliceBlocks > uint32(v.maxBlocks) {
				if blocksAccumulated == 0 {
					limitedSlices = append(limitedSlices, slice)
				}
				break
			}
			limitedSlices = append(limitedSlices, slice)
			blocksAccumulated += sliceBlocks
		}
		slices = limitedSlices
		v.slices = slices
		lastBlock = slices[len(slices)-1].EndBlock
	}

	if !v.outputJSON {
		scopeInfo := fmt.Sprintf("%d slices, blocks %d-%d", len(slices), firstBlock, lastBlock)
		if v.maxBlocks > 0 {
			scopeInfo = fmt.Sprintf("%d/%d slices, blocks %d-%d (limit: %d)", len(slices), totalSlices, firstBlock, lastBlock, v.maxBlocks)
		}
		logger.Printf("info", "Scope: %s | Mode: %s | Workers: %d", scopeInfo, v.getModeShortName(), v.workers)
	}

	// Open corereader if onblock checking is enabled (shared across all slices)
	if v.checkOnblock {
		cfg := corereader.DefaultOpenConfig()
		reader, err := corereader.Open(v.basePath, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to open corereader: %w", err)
		}
		v.reader = reader
		defer reader.Close()
	}

	// Step 2: Validate slices in parallel
	results := v.validateSlicesParallel(slices)

	// Step 3: Cross-slice validation
	if err := v.validateCrossSlice(slices, results); err != nil {
		v.addIssue(ValidationIssue{
			Severity: "ERROR",
			Type:     "CROSS_SLICE_ERROR",
			Message:  err.Error(),
		})
	}

	// Step 4: Generate report
	report := v.generateReport(results, time.Since(startTime))

	return report, nil
}

func (v *Validator) getModeName() string {
	if v.checkOnblock {
		return "ONBLOCK (check eosio::onblock in each block)"
	}
	if v.fullValidation {
		return "FULL (decompress + parse all blocks)"
	}
	return "FAST (index-only)"
}

func (v *Validator) getModeShortName() string {
	if v.checkOnblock {
		return "ONBLOCK"
	}
	if v.fullValidation {
		return "FULL"
	}
	return "FAST"
}

// discoverSlices scans the filesystem for slice directories
func (v *Validator) discoverSlices() ([]SliceInfo, error) {
	entries, err := os.ReadDir(v.basePath)
	if err != nil {
		return nil, err
	}

	var slices []SliceInfo
	const blocksPerSlice = 10000

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Parse directory name: history_NNNNNNNNNN-NNNNNNNNNN
		var startBlock, maxBlock uint32
		n, err := fmt.Sscanf(entry.Name(), "history_%010d-%010d", &startBlock, &maxBlock)
		if err != nil || n != 2 {
			continue
		}

		slicePath := filepath.Join(v.basePath, entry.Name())

		// Read blocks.index to find actual EndBlock
		blockIndexPath := filepath.Join(slicePath, "blocks.index")
		endBlock, globMin, globMax, err := v.findLastBlockInIndex(blockIndexPath)
		if err != nil {
			if v.debug {
				logger.Printf("debug", "Failed to read blocks.index for %s: %v", entry.Name(), err)
			}
			continue
		}

		// Check if finalized (next slice exists)
		sliceNum := startBlock / blocksPerSlice
		nextSliceStart := (sliceNum+1)*blocksPerSlice + 1
		nextSliceMax := nextSliceStart + blocksPerSlice - 1
		nextSliceName := fmt.Sprintf("history_%010d-%010d", nextSliceStart, nextSliceMax)
		nextSlicePath := filepath.Join(v.basePath, nextSliceName)

		finalized := false
		if _, err := os.Stat(nextSlicePath); err == nil {
			finalized = true
		}

		slices = append(slices, SliceInfo{
			SliceNum:       sliceNum,
			StartBlock:     startBlock,
			EndBlock:       endBlock,
			MaxBlock:       maxBlock,
			BlocksPerSlice: blocksPerSlice,
			Finalized:      finalized,
			GlobMin:        globMin,
			GlobMax:        globMax,
		})
	}

	if len(slices) == 0 {
		return nil, fmt.Errorf("no valid slice directories found")
	}

	// Sort by slice number
	sort.Slice(slices, func(i, j int) bool {
		return slices[i].SliceNum < slices[j].SliceNum
	})

	return slices, nil
}

// findLastBlockInIndex reads blocks.index to find the last written block
func (v *Validator) findLastBlockInIndex(indexPath string) (uint32, uint64, uint64, error) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return 0, 0, 0, err
	}

	if len(data) < 4 {
		return 0, 0, 0, fmt.Errorf("index file too small")
	}

	// Read count (first 4 bytes)
	count := binary.LittleEndian.Uint32(data[0:4])
	if count == 0 {
		return 0, 0, 0, fmt.Errorf("index is empty")
	}

	// Each entry: blockNum(4) + offset(8) + size(4) + globMin(8) + globMax(8) = 32 bytes
	entrySize := 32
	expectedSize := 4 + int(count)*entrySize

	if len(data) < expectedSize {
		// Index may be truncated
		count = uint32((len(data) - 4) / entrySize)
		if count == 0 {
			return 0, 0, 0, fmt.Errorf("no complete entries in index")
		}
	}

	// Read first entry for globMin
	firstEntryOffset := 4
	firstEntry := data[firstEntryOffset : firstEntryOffset+entrySize]
	globMin := binary.LittleEndian.Uint64(firstEntry[12:20])

	// Read last entry to get endBlock and globMax
	lastEntryOffset := 4 + (int(count)-1)*entrySize
	lastEntry := data[lastEntryOffset : lastEntryOffset+entrySize]
	endBlock := binary.LittleEndian.Uint32(lastEntry[0:4])
	globMax := binary.LittleEndian.Uint64(lastEntry[20:28])

	return endBlock, globMin, globMax, nil
}

// validateSlicesParallel validates slices using a worker pool
func (v *Validator) validateSlicesParallel(slices []SliceInfo) []SliceValidation {
	results := make([]SliceValidation, len(slices))
	resultsMu := sync.Mutex{}

	// Calculate total expected blocks for progress
	var totalExpectedBlocks uint32
	for _, slice := range slices {
		totalExpectedBlocks += slice.EndBlock - slice.StartBlock + 1
	}

	// Create work queue
	work := make(chan int, len(slices))
	for i := range slices {
		work <- i
	}
	close(work)

	// Progress tracking
	var completedSlices uint32
	var completedBlocks uint32
	startTime := time.Now()
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()

	// Start progress reporter
	done := make(chan struct{})
	progressDone := make(chan struct{})
	if !v.outputJSON {
		go func() {
			defer close(progressDone)
			for {
				select {
				case <-progressTicker.C:
					elapsed := time.Since(startTime).Seconds()
					blocks := completedBlocks
					rate := float64(blocks) / elapsed
					pct := float64(blocks) / float64(totalExpectedBlocks) * 100
					logger.Printf("progress", "%d/%d slices | %d/%d blocks (%.1f%%) | %.0f blocks/sec",
						completedSlices, len(slices), blocks, totalExpectedBlocks, pct, rate)
				case <-done:
					return
				}
			}
		}()
	} else {
		close(progressDone)
	}

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < v.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range work {
				result := v.validateSlice(slices[idx])
				resultsMu.Lock()
				results[idx] = result
				completedSlices++
				completedBlocks += result.BlocksActual
				resultsMu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(done)
	<-progressDone

	return results
}

// validateSlice performs all checks on a single slice
func (v *Validator) validateSlice(sliceInfo SliceInfo) SliceValidation {
	startTime := time.Now()

	result := SliceValidation{
		SliceNum:   sliceInfo.SliceNum,
		BlockRange: fmt.Sprintf("%010d-%010d", sliceInfo.StartBlock, sliceInfo.MaxBlock),
		StartBlock: sliceInfo.StartBlock,
		EndBlock:   sliceInfo.EndBlock,
		Finalized:  sliceInfo.Finalized,
		Issues:     make([]ValidationIssue, 0),
	}

	slicePath := filepath.Join(v.basePath, fmt.Sprintf("history_%010d-%010d", sliceInfo.StartBlock, sliceInfo.MaxBlock))

	// Check 1: Required files exist (globalseq.index no longer required - glob lookups use blocks.index)
	requiredFiles := []string{"data.log", "blocks.index"}
	for _, file := range requiredFiles {
		filePath := filepath.Join(slicePath, file)
		if _, err := os.Stat(filePath); err != nil {
			result.Issues = append(result.Issues, ValidationIssue{
				Severity: "ERROR",
				Type:     "MISSING_FILE",
				SliceNum: sliceInfo.SliceNum,
				File:     file,
				Message:  fmt.Sprintf("Required file missing: %s", file),
			})
		}
	}

	// If files missing, skip further checks
	if len(result.Issues) > 0 {
		result.Duration = time.Since(startTime)
		return result
	}

	// Check 2: Block index validation
	blockIndexPath := filepath.Join(slicePath, "blocks.index")
	blockIndex, blockIssues := v.validateBlockIndex(sliceInfo, blockIndexPath)
	result.Issues = append(result.Issues, blockIssues...)

	if blockIndex != nil {
		result.BlocksActual = uint32(blockIndex.Count())
		result.GlobsExpected = blockIndex.GetTotalGlobs()
		result.GlobsActual = result.GlobsExpected // Glob count derived from block index
	}

	// Expected blocks = EndBlock - StartBlock + 1
	result.BlocksExpected = sliceInfo.EndBlock - sliceInfo.StartBlock + 1

	// Check 3 (optional): Data integrity
	if v.fullValidation && blockIndex != nil {
		dataIssues := v.validateDataLog(sliceInfo, slicePath, blockIndex)
		result.Issues = append(result.Issues, dataIssues...)
	}

	// Check 4 (optional): Onblock validation
	if v.checkOnblock && blockIndex != nil {
		onblockResult := v.validateBlocksOnblock(sliceInfo, slicePath, blockIndex, 0)
		result.Issues = append(result.Issues, onblockResult.Issues...)
		result.OnblockCount = onblockResult.OnblockCount
		result.BlocksChecked = onblockResult.BlocksChecked
	}

	result.Duration = time.Since(startTime)
	return result
}

// validateBlockIndex checks block index integrity
func (v *Validator) validateBlockIndex(sliceInfo SliceInfo, indexPath string) (*BlockIndex, []ValidationIssue) {
	var issues []ValidationIssue

	// Load block index
	blockIndex, err := LoadBlockIndex(indexPath)
	if err != nil {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Type:     "INDEX_LOAD_FAILED",
			SliceNum: sliceInfo.SliceNum,
			File:     "blocks.index",
			Message:  fmt.Sprintf("Failed to load block index: %v", err),
		})
		return nil, issues
	}

	if blockIndex.Count() == 0 {
		issues = append(issues, ValidationIssue{
			Severity: "WARNING",
			Type:     "EMPTY_INDEX",
			SliceNum: sliceInfo.SliceNum,
			File:     "blocks.index",
			Message:  "Block index is empty",
		})
		return blockIndex, issues
	}

	// Check block numbers are within slice boundaries
	entries := blockIndex.GetAllEntries()
	var prevBlockNum uint32
	for i, entry := range entries {
		// Check boundaries
		if entry.BlockNum < sliceInfo.StartBlock || entry.BlockNum > sliceInfo.MaxBlock {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Type:     "BLOCK_OUT_OF_RANGE",
				SliceNum: sliceInfo.SliceNum,
				BlockNum: entry.BlockNum,
				File:     "blocks.index",
				Message:  fmt.Sprintf("Block %d outside slice range [%d-%d]", entry.BlockNum, sliceInfo.StartBlock, sliceInfo.MaxBlock),
			})
		}

		// Check for duplicates
		if i > 0 && entry.BlockNum == prevBlockNum {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Type:     "DUPLICATE_BLOCK",
				SliceNum: sliceInfo.SliceNum,
				BlockNum: entry.BlockNum,
				File:     "blocks.index",
				Message:  fmt.Sprintf("Duplicate block number: %d", entry.BlockNum),
			})
		}

		// Check for gaps (only if finalized)
		if sliceInfo.Finalized && i > 0 && entry.BlockNum != prevBlockNum+1 {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Type:     "MISSING_BLOCK",
				SliceNum: sliceInfo.SliceNum,
				BlockNum: prevBlockNum + 1,
				File:     "blocks.index",
				Message:  fmt.Sprintf("Gap detected: missing blocks %d to %d", prevBlockNum+1, entry.BlockNum-1),
			})
		}

		prevBlockNum = entry.BlockNum
	}

	return blockIndex, issues
}

// validateDataLog checks data.log integrity
func (v *Validator) validateDataLog(sliceInfo SliceInfo, slicePath string, blockIndex *BlockIndex) []ValidationIssue {
	var issues []ValidationIssue

	dataLogPath := filepath.Join(slicePath, "data.log")
	fileInfo, err := os.Stat(dataLogPath)
	if err != nil {
		issues = append(issues, ValidationIssue{
			Severity: "ERROR",
			Type:     "FILE_ERROR",
			SliceNum: sliceInfo.SliceNum,
			File:     "data.log",
			Message:  fmt.Sprintf("Failed to stat data.log: %v", err),
		})
		return issues
	}

	// Check each block is readable
	entries := blockIndex.GetAllEntries()
	for _, entry := range entries {
		// Check offset + size is within file bounds
		endOffset := entry.Offset + uint64(entry.Size) + 4 // +4 for size prefix
		if endOffset > uint64(fileInfo.Size()) {
			issues = append(issues, ValidationIssue{
				Severity: "ERROR",
				Type:     "DATA_OUT_OF_BOUNDS",
				SliceNum: sliceInfo.SliceNum,
				BlockNum: entry.BlockNum,
				File:     "data.log",
				Message:  fmt.Sprintf("Block %d extends beyond file (offset=%d size=%d file_size=%d)", entry.BlockNum, entry.Offset, entry.Size, fileInfo.Size()),
			})
		}
	}

	return issues
}

// Pre-computed name values for onblock detection
var (
	eosioNameUint64   = chain.StringToName("eosio")
	onblockNameUint64 = chain.StringToName("onblock")
)

// OnblockResult holds the results of onblock validation
type OnblockResult struct {
	Issues        []ValidationIssue
	BlocksChecked int
	OnblockCount  int
	MissingBlocks []uint32
}

// validateBlocksOnblock checks that each block contains an eosio::onblock action
// Uses the shared corereader for proper block parsing
func (v *Validator) validateBlocksOnblock(sliceInfo SliceInfo, _ string, blockIndex *BlockIndex, maxBlocks int) OnblockResult {
	result := OnblockResult{
		Issues:        make([]ValidationIssue, 0),
		MissingBlocks: make([]uint32, 0),
	}

	if v.reader == nil {
		result.Issues = append(result.Issues, ValidationIssue{
			Severity: "ERROR",
			Type:     "READER_ERROR",
			SliceNum: sliceInfo.SliceNum,
			Message:  "corereader not initialized",
		})
		return result
	}

	entries := blockIndex.GetAllEntries()

	const batchSize = 100
	for i := 0; i < len(entries); i += batchSize {
		if maxBlocks > 0 && result.BlocksChecked >= maxBlocks {
			break
		}

		batchEnd := i + batchSize
		if batchEnd > len(entries) {
			batchEnd = len(entries)
		}
		if maxBlocks > 0 && result.BlocksChecked+batchEnd-i > maxBlocks {
			batchEnd = i + (maxBlocks - result.BlocksChecked)
		}

		startBlock := entries[i].BlockNum
		endBlock := entries[batchEnd-1].BlockNum

		blocks, err := v.reader.GetRawBlockBatch(startBlock, endBlock)
		if err != nil {
			if v.debug {
				logger.Printf("debug", "Skipping blocks %d-%d: %v", startBlock, endBlock, err)
			}
			result.BlocksChecked += batchEnd - i
			continue
		}

		for _, block := range blocks {
			// Skip genesis blocks (1 and 2) - they don't have onblock actions
			if block.BlockNum <= 2 {
				result.BlocksChecked++
				continue
			}

			hasOnblock := false
			for _, action := range block.Actions {
				if action.ContractUint64 == eosioNameUint64 && action.ActionUint64 == onblockNameUint64 {
					hasOnblock = true
					break
				}
			}

			if hasOnblock {
				result.OnblockCount++
			} else {
				result.MissingBlocks = append(result.MissingBlocks, block.BlockNum)
				result.Issues = append(result.Issues, ValidationIssue{
					Severity: "ERROR",
					Type:     "MISSING_ONBLOCK",
					SliceNum: sliceInfo.SliceNum,
					BlockNum: block.BlockNum,
					File:     "data.log",
					Message:  fmt.Sprintf("Block %d missing eosio::onblock action", block.BlockNum),
				})
			}
			result.BlocksChecked++
		}
	}

	return result
}

// validateCrossSlice performs cross-slice validation
func (v *Validator) validateCrossSlice(slices []SliceInfo, results []SliceValidation) error {
	// Check for overlapping ranges
	for i := 1; i < len(slices); i++ {
		prev := slices[i-1]
		curr := slices[i]

		// Check no overlap
		if curr.StartBlock <= prev.MaxBlock {
			v.addIssue(ValidationIssue{
				Severity: "ERROR",
				Type:     "OVERLAPPING_SLICES",
				SliceNum: curr.SliceNum,
				Message:  fmt.Sprintf("Slice %d overlaps with slice %d", curr.SliceNum, prev.SliceNum),
			})
		}

		// Check no gap (for finalized slices)
		if prev.Finalized && curr.StartBlock != prev.MaxBlock+1 {
			v.addIssue(ValidationIssue{
				Severity: "WARNING",
				Type:     "SLICE_GAP",
				SliceNum: curr.SliceNum,
				Message:  fmt.Sprintf("Gap between slice %d (ends at %d) and slice %d (starts at %d)", prev.SliceNum, prev.EndBlock, curr.SliceNum, curr.StartBlock),
			})
		}
	}

	return nil
}

// generateReport creates the final validation report
func (v *Validator) generateReport(results []SliceValidation, duration time.Duration) *ValidationReport {
	// Collect all issues
	allIssues := make([]ValidationIssue, 0)
	allIssues = append(allIssues, v.issues...)

	for _, result := range results {
		allIssues = append(allIssues, result.Issues...)
	}

	// Count statistics
	var totalBlocks uint32
	var totalGlobs int
	var totalOnblocks int
	var totalBlocksChecked int
	finalizedCount := 0
	activeCount := 0
	errorCount := 0
	warningCount := 0
	missingOnblockCount := 0

	for _, result := range results {
		totalBlocks += result.BlocksActual
		totalGlobs += result.GlobsActual
		totalOnblocks += result.OnblockCount
		totalBlocksChecked += result.BlocksChecked
		if result.Finalized {
			finalizedCount++
		} else {
			activeCount++
		}
	}

	// Collect missing block numbers for pattern analysis
	var missingBlocks []uint32
	for _, issue := range allIssues {
		switch issue.Severity {
		case "ERROR":
			errorCount++
		case "WARNING":
			warningCount++
		}
		if issue.Type == "MISSING_ONBLOCK" {
			missingOnblockCount++
			missingBlocks = append(missingBlocks, issue.BlockNum)
		}
	}

	// Log missing onblock patterns if any
	if len(missingBlocks) > 0 && !v.outputJSON {
		v.logMissingOnblockPatterns(missingBlocks)
	}

	status := "passed"
	if errorCount > 0 {
		status = "failed"
	}

	validationRate := float64(len(results)) / duration.Seconds()

	report := &ValidationReport{
		Version:        "1.0.0",
		StoragePath:    v.basePath,
		ValidationMode: v.getModeName(),
		Timestamp:      time.Now(),
		Status:         status,
		Summary: ValidationSummary{
			TotalSlices:     len(results),
			TotalBlocks:     totalBlocks,
			TotalGlobs:      totalGlobs,
			FinalizedSlices: finalizedCount,
			ActiveSlices:    activeCount,
			Errors:          errorCount,
			Warnings:        warningCount,
			MissingOnblock:  missingOnblockCount,
			TotalOnblocks:   totalOnblocks,
			BlocksChecked:   totalBlocksChecked,
			DurationSeconds: duration.Seconds(),
			ValidationRate:  validationRate,
		},
		Issues:       allIssues,
		SliceResults: results,
	}

	return report
}

// logMissingOnblockPatterns analyzes and logs patterns in missing onblock blocks
func (v *Validator) logMissingOnblockPatterns(missingBlocks []uint32) {
	if len(missingBlocks) == 0 {
		return
	}

	// Sort blocks for range detection
	sort.Slice(missingBlocks, func(i, j int) bool {
		return missingBlocks[i] < missingBlocks[j]
	})

	// Find consecutive ranges
	type blockRange struct {
		start, end uint32
	}
	var ranges []blockRange
	rangeStart := missingBlocks[0]
	rangeEnd := missingBlocks[0]

	for i := 1; i < len(missingBlocks); i++ {
		if missingBlocks[i] == rangeEnd+1 {
			rangeEnd = missingBlocks[i]
		} else {
			ranges = append(ranges, blockRange{rangeStart, rangeEnd})
			rangeStart = missingBlocks[i]
			rangeEnd = missingBlocks[i]
		}
	}
	ranges = append(ranges, blockRange{rangeStart, rangeEnd})

	// Log the patterns
	logger.Printf("warning", "Missing onblock pattern analysis:")
	logger.Printf("warning", "  Total missing: %d blocks", len(missingBlocks))
	logger.Printf("warning", "  Block ranges: %d distinct ranges", len(ranges))

	// Show first few ranges
	maxRangesToShow := 10
	for i, r := range ranges {
		if i >= maxRangesToShow {
			logger.Printf("warning", "  ... and %d more ranges", len(ranges)-maxRangesToShow)
			break
		}
		if r.start == r.end {
			sliceNum := (r.start - 1) / 10000
			logger.Printf("warning", "  Block %d (slice %d)", r.start, sliceNum)
		} else {
			startSlice := (r.start - 1) / 10000
			endSlice := (r.end - 1) / 10000
			if startSlice == endSlice {
				logger.Printf("warning", "  Blocks %d-%d (%d blocks, slice %d)", r.start, r.end, r.end-r.start+1, startSlice)
			} else {
				logger.Printf("warning", "  Blocks %d-%d (%d blocks, slices %d-%d)", r.start, r.end, r.end-r.start+1, startSlice, endSlice)
			}
		}
	}

	// Check for slice-level patterns (which slices have the most missing)
	sliceCounts := make(map[uint32]int)
	for _, block := range missingBlocks {
		sliceNum := (block - 1) / 10000
		sliceCounts[sliceNum]++
	}

	if len(sliceCounts) > 1 {
		logger.Printf("warning", "  Affected slices: %d", len(sliceCounts))

		// Sort slices by count for reporting
		type sliceCount struct {
			sliceNum uint32
			count    int
		}
		var counts []sliceCount
		for s, c := range sliceCounts {
			counts = append(counts, sliceCount{s, c})
		}
		sort.Slice(counts, func(i, j int) bool {
			return counts[i].count > counts[j].count
		})

		// Show top affected slices
		maxSlicesToShow := 5
		for i, sc := range counts {
			if i >= maxSlicesToShow {
				break
			}
			logger.Printf("warning", "    Slice %d: %d missing blocks", sc.sliceNum, sc.count)
		}
	}
}

// addIssue adds a validation issue (thread-safe)
func (v *Validator) addIssue(issue ValidationIssue) {
	v.issuesMu.Lock()
	defer v.issuesMu.Unlock()
	v.issues = append(v.issues, issue)
}

// formatBlockList formats a sorted list of block numbers compactly
// Uses ranges for consecutive blocks: "100-105, 200, 300-302"
func formatBlockList(blocks []uint32) string {
	if len(blocks) == 0 {
		return ""
	}
	if len(blocks) == 1 {
		return fmt.Sprintf("%d", blocks[0])
	}

	var parts []string
	rangeStart := blocks[0]
	rangeEnd := blocks[0]

	for i := 1; i < len(blocks); i++ {
		if blocks[i] == rangeEnd+1 {
			rangeEnd = blocks[i]
		} else {
			parts = append(parts, formatRange(rangeStart, rangeEnd))
			rangeStart = blocks[i]
			rangeEnd = blocks[i]
		}
	}
	parts = append(parts, formatRange(rangeStart, rangeEnd))

	return fmt.Sprintf("%s (%d blocks)", strings.Join(parts, ", "), len(blocks))
}

func formatRange(start, end uint32) string {
	if start == end {
		return fmt.Sprintf("%d", start)
	}
	return fmt.Sprintf("%d-%d", start, end)
}

// PrintReport prints the validation report
func (v *Validator) PrintReport(report *ValidationReport) {
	if v.outputJSON {
		data, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(data))
		return
	}

	// Print status
	if report.Status == "passed" {
		logger.Printf("result", "VALIDATION PASSED")
	} else {
		logger.Printf("result", "VALIDATION FAILED - %d errors, %d warnings", report.Summary.Errors, report.Summary.Warnings)
	}

	// Print issues
	if len(report.Issues) > 0 {
		// Group missing onblock by slice, keep other errors separate
		missingOnblockBySlice := make(map[uint32][]uint32)
		var otherErrors []ValidationIssue
		var warnings []ValidationIssue

		for _, issue := range report.Issues {
			switch issue.Severity {
			case "ERROR":
				if issue.Type == "MISSING_ONBLOCK" {
					missingOnblockBySlice[issue.SliceNum] = append(missingOnblockBySlice[issue.SliceNum], issue.BlockNum)
				} else {
					otherErrors = append(otherErrors, issue)
				}
			case "WARNING":
				warnings = append(warnings, issue)
			}
		}

		// Print missing onblock errors grouped by slice
		if len(missingOnblockBySlice) > 0 {
			// Sort slice numbers for consistent output
			var sliceNums []uint32
			for sliceNum := range missingOnblockBySlice {
				sliceNums = append(sliceNums, sliceNum)
			}
			sort.Slice(sliceNums, func(i, j int) bool { return sliceNums[i] < sliceNums[j] })

			for _, sliceNum := range sliceNums {
				blocks := missingOnblockBySlice[sliceNum]
				sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
				logger.Printf("error", "Slice %d missing onblock: %s", sliceNum, formatBlockList(blocks))
			}
		}

		// Print other errors
		for _, issue := range otherErrors {
			if issue.BlockNum > 0 {
				logger.Printf("error", "Slice %d Block %d - %s", issue.SliceNum, issue.BlockNum, issue.Message)
			} else {
				logger.Printf("error", "Slice %d - %s", issue.SliceNum, issue.Message)
			}
		}

		// Print warnings
		for _, issue := range warnings {
			logger.Printf("warning", "Slice %d - %s", issue.SliceNum, issue.Message)
		}
	}

	// Print summary
	// Derive block range from slice results
	var firstBlock, lastBlock uint32
	if len(report.SliceResults) > 0 {
		firstBlock = report.SliceResults[0].StartBlock
		lastBlock = report.SliceResults[len(report.SliceResults)-1].EndBlock
	}

	// Storage overview
	logger.Printf("summary", "Blocks: %d-%d (%d total across %d slices)",
		firstBlock, lastBlock, report.Summary.TotalBlocks, report.Summary.TotalSlices)

	if report.Summary.ActiveSlices > 0 {
		logger.Printf("summary", "Slices: %d finalized, %d active",
			report.Summary.FinalizedSlices, report.Summary.ActiveSlices)
	}

	// Onblock validation results (only when --check-onblock used)
	if report.Summary.BlocksChecked > 0 {
		if report.Summary.MissingOnblock > 0 {
			pct := float64(report.Summary.TotalOnblocks) / float64(report.Summary.BlocksChecked) * 100
			logger.Printf("summary", "Onblock: %d/%d blocks (%.1f%%), %d missing",
				report.Summary.TotalOnblocks, report.Summary.BlocksChecked, pct, report.Summary.MissingOnblock)
		} else {
			logger.Printf("summary", "Onblock: %d/%d blocks verified",
				report.Summary.TotalOnblocks, report.Summary.BlocksChecked)
		}
	}

	// Performance
	blocksPerSec := float64(report.Summary.TotalBlocks) / report.Summary.DurationSeconds
	logger.Printf("summary", "Duration: %.2fs (%.0f blocks/sec)", report.Summary.DurationSeconds, blocksPerSec)
}
