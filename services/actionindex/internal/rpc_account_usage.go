package internal

import (
	"net/http"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
)

type UsageAction struct {
	GlobalActionSeq  uint64               `json:"global_action_seq"`
	Contract         string               `json:"contract"`
	Action           string               `json:"action"`
	AccountRAMDeltas []chain.AccountDelta `json:"account_ram_deltas"`
}

type UsageTransaction struct {
	TrxID         string        `json:"trx_id"`
	BlockNum      uint32        `json:"block_num"`
	BlockTime     string        `json:"block_time"`
	CpuUsageUs    uint32        `json:"cpu_usage_us"`
	NetUsageWords uint32        `json:"net_usage_words"`
	Actions       []UsageAction `json:"actions"`
}

type AccountUsageResponse struct {
	Results    []UsageTransaction      `json:"results"`
	NextCursor string                  `json:"next_cursor,omitempty"`
	PrevCursor string                  `json:"prev_cursor,omitempty"`
	Trace      *querytrace.TraceOutput `json:"trace,omitempty"`
}

func HandleAccountUsage(
	cfg *Config,
	store *Store,
	indexes ActionIndexer,
	reader corereader.Reader,
	abiReader *abicache.Reader,
	w http.ResponseWriter,
	r *http.Request,
) {
	startTime := time.Now()

	q, ok := newAccountQuery(cfg, indexes, reader, abiReader, w, r, "/usage", "account_usage")
	if !ok {
		return
	}

	if !q.execute() {
		return
	}

	results, ok := q.fetchUsageEntries()
	if !ok {
		return
	}

	response := AccountUsageResponse{
		Results:    results,
		NextCursor: q.nextCursor,
		PrevCursor: q.prevCursor,
		Trace:      q.finalize(len(results)),
	}

	writeJSON(w, response)

	logger.Printf("timing", "account_usage account=%s contract=%s action=%s date=%s results=%d duration=%v",
		q.req.accountName, q.req.Contract, q.req.Action, q.req.Date, len(results), time.Since(startTime))
}

func (q *accountQuery) fetchUsageEntries() ([]UsageTransaction, bool) {
	if len(q.selectedSeqs) == 0 {
		return []UsageTransaction{}, true
	}

	var fetchStart time.Time
	var cacheHitsBefore, cacheMissesBefore uint64
	if q.trace.Enabled() {
		fetchStart = time.Now()
		cacheHitsBefore, cacheMissesBefore, _, _ = q.reader.GetBlockCacheStats()
	}

	results, timings, err := fetchUsageByGlobalSeqs(q.reader, q.selectedSeqs)
	if err != nil {
		logger.Printf("error", "account usage query failed for account=%s contract=%s action=%s: %v",
			q.req.accountName, q.req.Contract, q.req.Action, err)
		writeAccountActivityError(q.w, "internal_error", "failed to fetch actions", http.StatusInternalServerError)
		return nil, false
	}

	q.timings = timings
	q.addFetchTrace(fetchStart, cacheHitsBefore, cacheMissesBefore, len(results))

	return results, true
}

func fetchUsageByGlobalSeqs(reader corereader.Reader, globalSeqs []uint64) ([]UsageTransaction, *corereader.FetchTimings, error) {
	if len(globalSeqs) == 0 {
		return []UsageTransaction{}, nil, nil
	}

	actions, timings, err := getActionsByGlobalSeqs(reader, globalSeqs)
	if err != nil {
		return nil, nil, err
	}

	var results []UsageTransaction
	trxIndex := make(map[string]int)

	for i, at := range actions {
		action := UsageAction{
			GlobalActionSeq:  globalSeqs[i],
			Contract:         at.Act.Account,
			Action:           at.Act.Name,
			AccountRAMDeltas: at.AccountRAMDeltas,
		}

		if idx, exists := trxIndex[at.TrxID]; exists {
			results[idx].Actions = append(results[idx].Actions, action)
		} else {
			trxIndex[at.TrxID] = len(results)
			results = append(results, UsageTransaction{
				TrxID:         at.TrxID,
				BlockNum:      at.BlockNum,
				BlockTime:     at.BlockTime,
				CpuUsageUs:    at.CpuUsageUs,
				NetUsageWords: at.NetUsageWords,
				Actions:       []UsageAction{action},
			})
		}
	}

	return results, timings, nil
}
