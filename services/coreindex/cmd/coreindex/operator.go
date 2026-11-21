package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/greymass/roborovski/libraries/coremanifest"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/tracereader"
	"github.com/greymass/roborovski/services/coreindex/internal/preflight"
	"github.com/greymass/roborovski/services/coreindex/internal/publish"
)

const (
	exitBlocked      = 2
	exitInconclusive = 3
)

func (c *Config) abiRoot() string {
	if c.ABIPath != "" {
		return c.ABIPath
	}
	return filepath.Join(c.Path, "abis")
}

func (c *Config) operatorModes() []string {
	var modes []string
	if c.Preflight {
		modes = append(modes, "--preflight")
	}
	if c.Recover {
		modes = append(modes, "--recover")
	}
	if c.ResetAfterBlock >= 0 {
		modes = append(modes, "--reset-after-block")
	}
	return modes
}

func (c *Config) checkOperatorConflicts() error {
	modes := c.operatorModes()
	if len(modes) > 1 {
		return fmt.Errorf("%s and %s are separate modes; run one at a time", modes[0], modes[1])
	}
	conflicts := map[string]bool{
		"--read-only": c.ReadOnly, "--replay": c.Replay, "--verify": c.Verify, "--verify-only": c.VerifyOnly,
		"--repair-slice": c.RepairSlice >= 0, "--repair-all": c.RepairAll, "--rebuild-abis": c.RebuildABIs,
	}
	for flag, set := range conflicts {
		if set {
			return fmt.Errorf("%s cannot be combined with %s", modes[0], flag)
		}
	}
	return nil
}

// runOperatorMode runs --preflight, --recover or --reset-after-block and reports whether one ran.
func runOperatorMode(cfg *Config, traceConfig *tracereader.Config) bool {
	if len(cfg.operatorModes()) == 0 {
		return false
	}
	if err := cfg.checkOperatorConflicts(); err != nil {
		logger.Fatal("%v", err)
	}
	switch {
	case cfg.Preflight:
		runPreflight(cfg, traceConfig)
	case cfg.Recover:
		runRecover(cfg)
	default:
		runReset(cfg)
	}
	return true
}

func configuredChainID(cfg *Config) [32]byte {
	var id [32]byte
	if cfg.ChainID != "" {
		var err error
		if id, err = coremanifest.ParseChainID(cfg.ChainID); err != nil {
			logger.Fatal("Invalid --chain-id: %v", err)
		}
	}
	return id
}

func runPreflight(cfg *Config, traceConfig *tracereader.Config) {
	o := preflight.Options{
		Target:    preflight.Target(cfg.PreflightTarget),
		CoreRoot:  cfg.Path,
		ABIRoot:   cfg.abiRoot(),
		SliceSpan: uint32(cfg.SliceSize),
		ChainID:   configuredChainID(cfg),
		HeadID:    blockIDFromBlob,
	}
	switch o.Target {
	case preflight.TargetDirect:
		if cfg.SnapshotInfo != "" {
			info, err := preflight.ReadSnapshotInfo(cfg.SnapshotInfo)
			if err != nil {
				logger.Fatal("--snapshot-info: %v", err)
			}
			o.Snapshot = info
		}
	case preflight.TargetTrace:
		if cfg.SnapshotInfo != "" {
			logger.Fatal("--snapshot-info applies to --preflight-target direct")
		}
		o.TraceCovers = func(block uint32) (bool, error) {
			blocks, err := tracereader.GetRawBlocksWithMetadata(block, 1, traceConfig)
			if errors.Is(err, tracereader.ErrNotFound) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return len(blocks) > 0 && blocks[0].BlockNum == block, nil
		}
	default:
		logger.Fatal("--preflight needs --preflight-target direct or --preflight-target trace")
	}
	report := preflight.Run(o)
	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		logger.Fatal("%v", err)
	}
	fmt.Println(string(out))
	switch report.Result {
	case preflight.ResultBlocked:
		os.Exit(exitBlocked)
	case preflight.ResultInconclusive:
		os.Exit(exitInconclusive)
	}
}

func operatorPublishConfig(cfg *Config) publish.Config {
	return publish.Config{
		CoreRoot:  cfg.Path,
		ABIRoot:   cfg.abiRoot(),
		SliceSpan: uint32(cfg.SliceSize),
		HeadID:    blockIDFromBlob,
	}
}

func runRecover(cfg *Config) {
	m, adopted, err := publish.Recover(operatorPublishConfig(cfg))
	if err != nil {
		logger.Fatal("Recovery failed: %v", err)
	}
	if adopted {
		logger.Printf("startup", "Adopted the store at block %d under epoch %d", m.Head, m.Epoch)
		return
	}
	logger.Printf("startup", "Core storage and the ABI cache match the manifest: block %d, epoch %d, %d ABI entries", m.Head, m.Epoch, m.ABIIndexLen)
}

func runReset(cfg *Config) {
	p, err := publish.Open(operatorPublishConfig(cfg))
	if err != nil {
		logger.Fatal("Cannot take ownership of storage: %v", err)
	}
	defer p.Close()
	res, err := p.ResetAfter(uint32(cfg.ResetAfterBlock), nil)
	if err != nil {
		p.Close()
		if res != nil {
			logger.Fatal("Reset to block %d stopped under epoch %d; run --recover or this reset again to finish it: %v", res.Boundary, res.Epoch, err)
		}
		logger.Fatal("Reset refused; storage is unchanged: %v", err)
	}
	if res.Resumed {
		logger.Printf("startup", "Finished the reset to block %d under epoch %d", res.Boundary, res.Epoch)
	} else {
		logger.Printf("startup", "Reset to block %d under epoch %d", res.Boundary, res.Epoch)
	}
	logger.Printf("startup", "Removed %d slices and %d ABI entries; replay from a snapshot at or below block %d rebuilds the rest",
		len(res.RemovedSlices), res.ABIEntriesCut, res.Boundary)
}
