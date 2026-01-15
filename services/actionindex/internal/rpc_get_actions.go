package internal

import (
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/encoding"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
	"github.com/greymass/roborovski/libraries/server"
)

type GetActionsRequest struct {
	AccountName    string `json:"account_name"`
	Pos            int64  `json:"pos"`
	Offset         int64  `json:"offset"`
	Contract       string `json:"contract"`
	Action         string `json:"action"`
	Decode         bool   `json:"decode"`
	DecodeExplicit bool
}

type GetActionsResponse struct {
	Actions               []ActionResult `json:"actions"`
	HeadBlockNum          uint32         `json:"head_block_num"`
	LastIrreversibleBlock uint32         `json:"last_irreversible_block"`
}

func HandleGetActions(
	cfg *Config,
	store *Store,
	indexes ActionIndexer,
	reader corereader.Reader,
	abiReader *abicache.Reader,
	w http.ResponseWriter,
	r *http.Request,
) {
	startTime := time.Now()

	if indexes.IsBulkMode() {
		libNum, _, _ := indexes.GetProperties()
		_, chainLIB, _ := reader.GetStateProps(true)
		pct := float64(0)
		if chainLIB > 0 {
			pct = float64(libNum) / float64(chainLIB) * 100
		}
		writeError(w, fmt.Sprintf("service is syncing (%.1f%% complete, block %d/%d) - queries unavailable during bulk sync", pct, libNum, chainLIB), http.StatusServiceUnavailable)
		return
	}

	req, err := parseGetActionsRequest(r)
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.AccountName == "" {
		writeError(w, "account_name required", http.StatusBadRequest)
		return
	}

	if req.Action != "" && req.Contract == "" {
		writeError(w, "action filter requires contract to be specified", http.StatusBadRequest)
		return
	}

	if req.DecodeExplicit && req.Decode && abiReader == nil {
		writeError(w, "decode=true requested but ABI data is not available", http.StatusBadRequest)
		return
	}

	shouldDecode := abiReader != nil && req.Decode

	account := chain.StringToName(req.AccountName)

	trace := querytrace.New("get_actions", req.AccountName)

	libNum, headNum, err := indexes.GetProperties()
	if err != nil {
		writeError(w, "failed to get database state", http.StatusInternalServerError)
		return
	}

	requestedCount := req.Offset
	if requestedCount < 0 {
		requestedCount = -requestedCount
	}
	if requestedCount == 0 {
		requestedCount = 1
	}

	var loadLimit int
	var loadDescending bool
	if req.Pos < 0 {
		loadLimit = int(requestedCount)
		loadDescending = true
	} else {
		loadLimit = int(req.Pos) + int(requestedCount)
		loadDescending = false
	}

	globalSeqs, err := indexes.LoadActions(account, req.Contract, req.Action, loadLimit, loadDescending, trace)
	if err != nil {
		writeError(w, "failed to load actions", http.StatusInternalServerError)
		return
	}

	if cfg.Debug {
		if len(globalSeqs) > 10 {
			logger.Printf("debug", "[get_actions] account=%s loaded %d globalSeqs, first10=%v last10=%v",
				req.AccountName, len(globalSeqs), globalSeqs[:10], globalSeqs[len(globalSeqs)-10:])
		} else {
			logger.Printf("debug", "[get_actions] account=%s loaded %d globalSeqs: %v",
				req.AccountName, len(globalSeqs), globalSeqs)
		}
	}

	if len(globalSeqs) == 0 {
		trace.SetResult(0, "empty")
		trace.Log()
		writeJSON(w, GetActionsResponse{
			Actions:               []ActionResult{},
			HeadBlockNum:          headNum,
			LastIrreversibleBlock: libNum,
		})
		return
	}

	var selectedSeqs []uint64
	var startIndex int

	var sliceStart time.Time
	if trace.Enabled() {
		sliceStart = time.Now()
	}
	if req.Pos < 0 {
		startIndex = len(globalSeqs) - int(requestedCount)
		if startIndex < 0 {
			startIndex = 0
		}
		selectedSeqs = globalSeqs[startIndex:]
	} else {
		if req.Pos == 0 {
			startIndex = 0
		} else {
			startIndex = int(req.Pos) - 1
		}
		if startIndex >= len(globalSeqs) {
			startIndex = len(globalSeqs) - 1
		}
		if startIndex < 0 {
			startIndex = 0
		}

		if req.Offset >= 0 {
			endIdx := startIndex + int(requestedCount)
			if endIdx > len(globalSeqs) {
				endIdx = len(globalSeqs)
			}
			selectedSeqs = globalSeqs[startIndex:endIdx]
		} else {
			endIdx := startIndex + 1
			startIndex = endIdx - int(requestedCount)
			if startIndex < 0 {
				startIndex = 0
			}
			selectedSeqs = globalSeqs[startIndex:endIdx]
		}
	}

	if req.Offset < 0 {
		reverseUint64Slice(selectedSeqs)
	}
	if trace.Enabled() {
		trace.AddStepWithCount("slice", "paginate", time.Since(sliceStart), len(selectedSeqs), "")
	}

	if cfg.Debug {
		logger.Printf("debug", "[get_actions] account=%s fetching %d actions, selectedSeqs=%v startIndex=%d",
			req.AccountName, len(selectedSeqs), selectedSeqs, startIndex)
	}

	var effectiveAbiReader *abicache.Reader
	if shouldDecode {
		effectiveAbiReader = abiReader
	}

	reader.GetStateProps(true)

	var fetchStart time.Time
	var cacheHitsBefore, cacheMissesBefore uint64
	if trace.Enabled() {
		fetchStart = time.Now()
		cacheHitsBefore, cacheMissesBefore, _, _ = reader.GetBlockCacheStats()
	}
	results, timings, err := fetchActionsByGlobalSeqs(reader, selectedSeqs, effectiveAbiReader, cfg.OmitNullFields, true)
	if err != nil {
		logger.Printf("error", "get_actions failed for account=%s contract=%s action=%s: %v", req.AccountName, req.Contract, req.Action, err)
		writeError(w, "failed to fetch actions", http.StatusInternalServerError)
		return
	}
	if trace.Enabled() {
		cacheHitsAfter, cacheMissesAfter, cacheSize, cacheBlocks := reader.GetBlockCacheStats()
		hits := cacheHitsAfter - cacheHitsBefore
		misses := cacheMissesAfter - cacheMissesBefore
		details := fmt.Sprintf("cache=%d/%d size=%.1fMB blocks=%d", hits, hits+misses, float64(cacheSize)/(1024*1024), cacheBlocks)
		if timings != nil {
			details += fmt.Sprintf(" phase1=%v(a=%v b=%v c=%v) phase2=%v decompress=%v(%d) parse=%v(%d) match=%v",
				timings.Phase1, timings.Phase1a, timings.Phase1b, timings.Phase1c,
				timings.Phase2,
				timings.DecompressTime, timings.DecompressCount,
				timings.ParseTime, timings.ParseCount,
				timings.MatchTime)
		}
		trace.AddStepWithCount("corereader", "fetch", time.Since(fetchStart), len(results), details)
	}

	if req.Offset < 0 {
		slices.Reverse(results)
	}

	indexUsed := "AllActions"
	if req.Contract != "" && req.Action != "" {
		indexUsed = "ContractAction"
	} else if req.Contract != "" {
		indexUsed = "ContractWildcard"
	}
	trace.SetResult(len(results), indexUsed)
	trace.Log()

	writeJSON(w, GetActionsResponse{
		Actions:               results,
		HeadBlockNum:          headNum,
		LastIrreversibleBlock: libNum,
	})

	logger.Printf("timing", "get_actions account=%s contract=%s action=%s results=%d duration=%v",
		req.AccountName, req.Contract, req.Action, len(results), time.Since(startTime))
}

func parseGetActionsRequest(r *http.Request) (*GetActionsRequest, error) {
	req := &GetActionsRequest{
		Pos:    -1,
		Offset: -20,
		Decode: true,
	}

	params, err := server.GetRequestParams(r)
	if err != nil {
		return nil, err
	}

	if v, ok := params["account_name"].(string); ok {
		req.AccountName = v
	}
	if posParam, found := params["pos"]; found {
		if pos, ok := encoding.MaybeGetInt64(posParam); ok {
			req.Pos = pos
		}
	}
	if offsetParam, found := params["offset"]; found {
		if offset, ok := encoding.MaybeGetInt64(offsetParam); ok {
			req.Offset = offset
		}
	}
	if v, ok := params["contract"].(string); ok {
		req.Contract = v
	}
	if v, ok := params["action"].(string); ok {
		req.Action = v
	}
	if v, ok := params["decode"].(bool); ok {
		req.Decode = v
		req.DecodeExplicit = true
	}

	return req, nil
}

func reverseUint64Slice(s []uint64) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
