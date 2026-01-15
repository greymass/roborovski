package internal

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/server"
)

type ActionResult struct {
	GlobalActionSeq  uint64      `json:"global_action_seq"`
	AccountActionSeq *uint64     `json:"account_action_seq,omitempty"`
	BlockNum         uint32      `json:"block_num"`
	BlockTime        string      `json:"block_time"`
	ActionTrace      interface{} `json:"action_trace"`
	Irreversible     bool        `json:"irreversible"`
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	server.WriteJSON(w, http.StatusOK, data)
}

func writeError(w http.ResponseWriter, message string, code int) {
	server.WriteError(w, code, message)
}

func buildActionTrace(at chain.ActionTrace, abiReader *abicache.Reader, omitNullFields bool) interface{} {
	return map[string]interface{}{
		"action_ordinal":                             at.ActionOrdinal,
		"creator_action_ordinal":                     at.CreatorAO,
		"closest_unnotified_ancestor_action_ordinal": at.ClosestUAAO,
		"receipt":            at.Receipt,
		"receiver":           at.Receiver,
		"act":                buildAction(at.Act, abiReader, at.BlockNum, omitNullFields),
		"context_free":       at.ContextFree,
		"elapsed":            at.Elapsed,
		"trx_id":             at.TrxID,
		"block_num":          at.BlockNum,
		"block_time":         at.BlockTime,
		"producer_block_id":  at.ProducerBlockID,
		"account_ram_deltas": at.AccountRAMDeltas,
	}
}

func buildAction(act chain.Action, abiReader *abicache.Reader, blockNum uint32, omitNullFields bool) map[string]interface{} {
	result := map[string]interface{}{
		"account":       act.Account,
		"name":          act.Name,
		"authorization": act.Authorization,
		"hex_data":      act.Data,
	}

	if abiReader != nil {
		contract := chain.StringToName(act.Account)
		actionName := chain.StringToName(act.Name)
		decoded, err := abiReader.DecodeHex(contract, actionName, act.Data, blockNum)
		if err == nil && decoded != nil {
			if omitNullFields {
				decoded = removeNullValues(decoded)
			}
			result["data"] = decoded
		} else {
			result["data"] = act.Data
		}
	} else {
		result["data"] = act.Data
	}

	return result
}

func removeNullValues(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		if v == nil {
			continue
		}
		if nested, ok := v.(map[string]interface{}); ok {
			result[k] = removeNullValues(nested)
		} else if arr, ok := v.([]interface{}); ok {
			cleaned := make([]interface{}, 0, len(arr))
			for _, item := range arr {
				if itemMap, ok := item.(map[string]interface{}); ok {
					cleaned = append(cleaned, removeNullValues(itemMap))
				} else if item != nil {
					cleaned = append(cleaned, item)
				}
			}
			result[k] = cleaned
		} else {
			result[k] = v
		}
	}
	return result
}

func fetchActionsByGlobalSeqs(reader corereader.Reader, globalSeqs []uint64, abiReader *abicache.Reader, omitNullFields bool, includeAccountSeq bool) ([]ActionResult, *corereader.FetchTimings, error) {
	if len(globalSeqs) == 0 {
		return []ActionResult{}, nil, nil
	}

	actions, timings, err := reader.GetActionsByGlobalSeqs(globalSeqs)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "not found") {
			logger.Printf("error", "GetActionsByGlobalSeqs failed: %v | requested globs: %v", err, formatGlobList(globalSeqs))
		} else {
			logger.Printf("error", "GetActionsByGlobalSeqs failed: %v", err)
		}
		return nil, nil, err
	}

	results := make([]ActionResult, len(actions))

	var wg sync.WaitGroup
	for i, at := range actions {
		wg.Add(1)
		go func(idx int, at chain.ActionTrace) {
			defer wg.Done()
			result := ActionResult{
				GlobalActionSeq: globalSeqs[idx],
				BlockNum:        at.BlockNum,
				BlockTime:       at.BlockTime,
				ActionTrace:     buildActionTrace(at, abiReader, omitNullFields),
				Irreversible:    true,
			}
			if includeAccountSeq {
				zero := uint64(0)
				result.AccountActionSeq = &zero
			}
			results[idx] = result
		}(i, at)
	}
	wg.Wait()

	return results, timings, nil
}

func formatGlobList(globs []uint64) string {
	if len(globs) == 0 {
		return "[]"
	}
	if len(globs) <= 10 {
		return formatGlobSlice(globs)
	}
	return fmt.Sprintf("%s...%s (total=%d)", formatGlobSlice(globs[:5]), formatGlobSlice(globs[len(globs)-5:]), len(globs))
}

func formatGlobSlice(globs []uint64) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, g := range globs {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%d", g))
	}
	sb.WriteString("]")
	return sb.String()
}
