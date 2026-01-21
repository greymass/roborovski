package main

import (
	"fmt"
	"os"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/actionstream"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
)

type RebuildABIsResult struct {
	ABIsFound      int
	ABIsWritten    int
	ABIsFailed     int
	BlocksScanned  uint32
	ActionsScanned uint64
	Duration       time.Duration
}

func RebuildABIs(store *appendlog.SliceStore, abiPath string, actionIndexSource string) (*RebuildABIsResult, error) {
	startTime := time.Now()

	tempPath := abiPath + ".tmp"
	if err := os.RemoveAll(tempPath); err != nil {
		return nil, fmt.Errorf("failed to remove temp directory: %w", err)
	}

	writer, err := abicache.NewWriter(tempPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create ABI writer: %w", err)
	}
	defer writer.Close()

	var result *RebuildABIsResult

	if actionIndexSource != "" {
		result, err = rebuildFromActionIndex(writer, actionIndexSource)
	} else {
		result, err = rebuildFromSlices(store, writer)
	}

	if err != nil {
		os.RemoveAll(tempPath)
		return nil, err
	}

	if err := writer.Sync(); err != nil {
		os.RemoveAll(tempPath)
		return nil, fmt.Errorf("failed to sync ABI writer: %w", err)
	}
	writer.Close()

	backupPath := abiPath + ".old"
	os.RemoveAll(backupPath)

	if _, err := os.Stat(abiPath); err == nil {
		if err := os.Rename(abiPath, backupPath); err != nil {
			os.RemoveAll(tempPath)
			return nil, fmt.Errorf("failed to backup old ABI cache: %w", err)
		}
	}

	if err := os.Rename(tempPath, abiPath); err != nil {
		os.Rename(backupPath, abiPath)
		return nil, fmt.Errorf("failed to move new ABI cache into place: %w", err)
	}

	os.RemoveAll(backupPath)

	result.Duration = time.Since(startTime)
	return result, nil
}

func rebuildFromSlices(store *appendlog.SliceStore, writer *abicache.Writer) (*RebuildABIsResult, error) {
	result := &RebuildABIsResult{}

	eosioName := chain.StringToName("eosio")
	setabiName := chain.StringToName("setabi")

	sliceInfos := store.GetSliceInfos()
	if len(sliceInfos) == 0 {
		logger.Printf("startup", "No slices found, nothing to rebuild")
		return result, nil
	}

	totalBlocks := uint32(0)
	for _, info := range sliceInfos {
		if info.EndBlock > info.StartBlock {
			totalBlocks += info.EndBlock - info.StartBlock + 1
		}
	}

	logger.Printf("startup", "Scanning %d slices (%d blocks) for eosio::setabi actions...", len(sliceInfos), totalBlocks)

	lastLogTime := time.Now()
	processedBlocks := uint32(0)

	for sliceIdx, info := range sliceInfos {
		for blockNum := info.StartBlock; blockNum <= info.EndBlock; blockNum++ {
			blockData, err := store.GetBlock(blockNum)
			if err != nil {
				continue
			}

			blob := bytesToBlockBlob(blockData)
			if blob == nil || blob.Block == nil {
				continue
			}

			for i, cat := range blob.Cats {
				if cat == nil {
					continue
				}

				result.ActionsScanned++

				contractName := blob.Block.NamesInBlock[cat.ContractNameIndex]
				actionName := blob.Block.NamesInBlock[cat.ActionNameIndex]

				if contractName != eosioName || actionName != setabiName {
					continue
				}

				result.ABIsFound++

				actionData := blob.Block.DataInBlock[cat.DataIndex]
				contract, abiJSON, err := abicache.ParseSetabi(actionData)
				if err != nil {
					logger.Printf("debug-abi", "Failed to parse setabi at block %d action %d: %v", blockNum, i, err)
					result.ABIsFailed++
					continue
				}

				if len(abiJSON) == 0 {
					continue
				}

				if err := writer.Write(blockNum, contract, abiJSON); err != nil {
					logger.Printf("warning", "Failed to write ABI for contract %d at block %d: %v", contract, blockNum, err)
					result.ABIsFailed++
					continue
				}

				result.ABIsWritten++
			}

			processedBlocks++
			result.BlocksScanned = processedBlocks

			if time.Since(lastLogTime) >= 3*time.Second {
				pct := float64(processedBlocks) / float64(totalBlocks) * 100
				logger.Printf("startup", "Progress: slice %d/%d, block %d (%.1f%%) - found %d ABIs, written %d, failed %d",
					sliceIdx+1, len(sliceInfos), blockNum, pct,
					result.ABIsFound, result.ABIsWritten, result.ABIsFailed)
				lastLogTime = time.Now()
			}
		}
	}

	return result, nil
}

func rebuildFromActionIndex(writer *abicache.Writer, actionIndexSource string) (*RebuildABIsResult, error) {
	result := &RebuildABIsResult{}

	eosioName := chain.StringToName("eosio")
	setabiName := chain.StringToName("setabi")

	filter := actionstream.Filter{
		Contracts: []uint64{eosioName},
		Actions:   []uint64{setabiName},
	}

	config := actionstream.DefaultClientConfig()
	config.AckInterval = 10000

	client := actionstream.NewClient(actionIndexSource, filter, 0, config)

	logger.Printf("startup", "Connecting to actionindex at %s...", actionIndexSource)

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to actionindex: %w", err)
	}
	defer client.Close()

	logger.Printf("startup", "Connected, streaming eosio::setabi actions from beginning...")

	lastLogTime := time.Now()
	headSeq := client.GetHeadSeq()

	for {
		action, ok, err := client.NextWithTimeout(5 * time.Second)
		if err != nil {
			return nil, fmt.Errorf("error receiving action: %w", err)
		}
		if !ok {
			if client.IsCatchupComplete() {
				logger.Printf("startup", "Catchup complete, stream finished")
				break
			}
			newHeadSeq := client.GetHeadSeq()
			if newHeadSeq > headSeq {
				headSeq = newHeadSeq
			}
			continue
		}

		result.ActionsScanned++

		if chain.StringToName(action.Action) != setabiName {
			continue
		}

		result.ABIsFound++

		contract, abiJSON, err := abicache.ParseSetabi(action.ActionData)
		if err != nil {
			logger.Printf("debug-abi", "Failed to parse setabi at block %d: %v", action.BlockNum, err)
			result.ABIsFailed++
			continue
		}

		if len(abiJSON) == 0 {
			continue
		}

		if err := writer.Write(action.BlockNum, contract, abiJSON); err != nil {
			logger.Printf("warning", "Failed to write ABI for contract %d at block %d: %v", contract, action.BlockNum, err)
			result.ABIsFailed++
			continue
		}

		result.ABIsWritten++
		result.BlocksScanned = action.BlockNum

		if time.Since(lastLogTime) >= 3*time.Second {
			headSeq = client.GetHeadSeq()
			pct := float64(0)
			if headSeq > 0 {
				pct = float64(action.GlobalSeq) / float64(headSeq) * 100
			}
			logger.Printf("startup", "Progress: seq %d/%d (%.1f%%) block %d - found %d ABIs, written %d, failed %d",
				action.GlobalSeq, headSeq, pct, action.BlockNum,
				result.ABIsFound, result.ABIsWritten, result.ABIsFailed)
			lastLogTime = time.Now()
		}
	}

	return result, nil
}
