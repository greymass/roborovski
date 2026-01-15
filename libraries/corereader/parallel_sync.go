package corereader

import (
	"runtime"
	"sync"
)

// FilterBlocksParallel filters multiple raw blocks in parallel.
// Returns results in block order (sorted by BlockNum).
func FilterBlocksParallel(batch *SliceBatch, startBlock, endBlock uint32, filterFunc ActionFilterFunc, workers int) ([]Block, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	blockCount := int(endBlock - startBlock + 1)
	results := make([]Block, blockCount)

	type workItem struct {
		blockNum uint32
		idx      int
	}
	workChan := make(chan workItem, blockCount)

	blockDataSlice := make([][]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		blockNum := startBlock + uint32(i)
		data, err := batch.ExtractBlock(blockNum)
		if err != nil {
			return nil, err
		}
		blockDataSlice[i] = data
		workChan <- workItem{blockNum: blockNum, idx: i}
	}
	close(workChan)

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			actionsBuf := make([]Action, 0, 1024)
			execBuf := make([]ContractExecution, 0, 256)

			for work := range workChan {
				blockData := blockDataSlice[work.idx]

				notif := parseRawBlock(blockData, work.blockNum, filterFunc)

				filtered, newActionsBuf, newExecBuf := FilterRawBlockInto(
					notif, filterFunc, actionsBuf, execBuf,
				)
				actionsBuf = newActionsBuf
				execBuf = newExecBuf

				actionsCopy := make([]Action, len(filtered.Actions))
				copy(actionsCopy, filtered.Actions)

				execCopy := make([]ContractExecution, len(filtered.Executions))
				copy(execCopy, filtered.Executions)

				results[work.idx] = Block{
					BlockNum:   work.blockNum,
					BlockTime:  notif.BlockTime,
					Actions:    actionsCopy,
					Executions: execCopy,
					MinSeq:     filtered.MinSeq,
					MaxSeq:     filtered.MaxSeq,
				}
			}
		}()
	}

	wg.Wait()

	return results, nil
}
