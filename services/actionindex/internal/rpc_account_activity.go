package internal

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/encoding"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
	"github.com/greymass/roborovski/libraries/server"
)

type AccountActivityRequest struct {
	Limit     int64  `json:"limit"`
	Order     string `json:"order"`
	Cursor    string `json:"cursor"`
	Contract  string `json:"contract"`
	Action    string `json:"action"`
	Date      string `json:"date"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
	Decode    bool   `json:"decode"`
	Trace     bool   `json:"trace"`

	accountName    string
	decodeExplicit bool
}

type AccountActivityResponse struct {
	Results    []ActionResult          `json:"results"`
	NextCursor string                  `json:"next_cursor,omitempty"`
	PrevCursor string                  `json:"prev_cursor,omitempty"`
	Trace      *querytrace.TraceOutput `json:"trace,omitempty"`
}

type AccountActivityErrorResponse struct {
	Error AccountActivityError `json:"error"`
}

type AccountActivityError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func HandleAccountActivity(
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
		writeAccountActivityError(w, "service_unavailable",
			fmt.Sprintf("service is syncing (%.1f%% complete, block %d/%d)", pct, libNum, chainLIB),
			http.StatusServiceUnavailable)
		return
	}

	req, err := parseAccountActivityRequest(r)
	if err != nil {
		writeAccountActivityError(w, "invalid_request", err.Error(), http.StatusBadRequest)
		return
	}

	if req.accountName == "" {
		writeAccountActivityError(w, "missing_account", "account name required in path", http.StatusBadRequest)
		return
	}

	if req.Action != "" && req.Contract == "" {
		writeAccountActivityError(w, "invalid_filter", "action filter requires contract to be specified", http.StatusBadRequest)
		return
	}

	if req.Date != "" && (req.StartDate != "" || req.EndDate != "") {
		writeAccountActivityError(w, "invalid_filter", "date parameter cannot be combined with start_date or end_date", http.StatusBadRequest)
		return
	}

	if req.decodeExplicit && req.Decode && abiReader == nil {
		writeAccountActivityError(w, "decode_unavailable", "decode=true requested but ABI data is not available", http.StatusBadRequest)
		return
	}

	shouldDecode := abiReader != nil && req.Decode

	account := chain.StringToName(req.accountName)

	trace := querytrace.New("account_activity", req.accountName)

	_, _, err = indexes.GetProperties()
	if err != nil {
		writeAccountActivityError(w, "internal_error", "failed to get database state", http.StatusInternalServerError)
		return
	}

	dateRange, err := parseDateParam(req.Date, req.StartDate, req.EndDate)
	if err != nil {
		writeAccountActivityError(w, "invalid_date", err.Error(), http.StatusBadRequest)
		return
	}

	var cursor *ActivityCursor
	if req.Cursor != "" {
		cursor, err = decodeActivityCursor(req.Cursor)
		if err != nil {
			writeAccountActivityError(w, "invalid_cursor", fmt.Sprintf("invalid cursor: %v", err), http.StatusBadRequest)
			return
		}
	}

	queryFingerprint := computeAccountActivityFingerprint(req)
	if cursor != nil && cursor.QueryFingerprint != queryFingerprint {
		cursor = nil
	}

	order := req.Order
	if cursor != nil {
		order = cursor.Order
	}
	descending := order == "desc"
	limit := int(req.Limit)

	var globalSeqs []uint64
	if cursor != nil {
		globalSeqs, err = indexes.LoadActionsFromCursorWithDateRange(account, req.Contract, req.Action, cursor.GlobalSeq, dateRange, limit, descending, trace)
	} else if dateRange != nil {
		globalSeqs, err = indexes.LoadActionsWithDateRange(account, req.Contract, req.Action, dateRange, limit, descending, trace)
	} else {
		globalSeqs, err = indexes.LoadActions(account, req.Contract, req.Action, limit, descending, trace)
	}
	if err != nil {
		writeAccountActivityError(w, "internal_error", "failed to load actions", http.StatusInternalServerError)
		return
	}

	if cfg.Debug {
		if len(globalSeqs) > 10 {
			logger.Printf("debug", "[account_activity] account=%s loaded %d globalSeqs, first10=%v last10=%v",
				req.accountName, len(globalSeqs), globalSeqs[:10], globalSeqs[len(globalSeqs)-10:])
		} else {
			logger.Printf("debug", "[account_activity] account=%s loaded %d globalSeqs: %v",
				req.accountName, len(globalSeqs), globalSeqs)
		}
	}

	if len(globalSeqs) == 0 {
		trace.SetResult(0, "empty")
		trace.Log()
		response := AccountActivityResponse{
			Results: []ActionResult{},
		}
		if req.Trace && trace.Enabled() {
			response.Trace = trace.ToJSON()
		}
		writeJSON(w, response)
		return
	}

	var sliceStart time.Time
	if trace.Enabled() {
		sliceStart = time.Now()
	}
	selectedSeqs := globalSeqs

	// If we used a cursor with flipped order (backwards navigation), reverse results
	// to match the originally requested order
	if cursor != nil && cursor.Order != req.Order {
		for i, j := 0, len(selectedSeqs)-1; i < j; i, j = i+1, j-1 {
			selectedSeqs[i], selectedSeqs[j] = selectedSeqs[j], selectedSeqs[i]
		}
	}

	if trace.Enabled() {
		trace.AddStepWithCount("slice", "paginate", time.Since(sliceStart), len(selectedSeqs), fmt.Sprintf("order=%s", req.Order))
	}

	var effectiveAbiReader *abicache.Reader
	if shouldDecode {
		effectiveAbiReader = abiReader
	}

	reader.GetStateProps(true)

	if cfg.Debug {
		logger.Printf("debug", "[account_activity] account=%s fetching %d actions, selectedSeqs=%v",
			req.accountName, len(selectedSeqs), selectedSeqs)
	}

	var fetchStart time.Time
	var cacheHitsBefore, cacheMissesBefore uint64
	if trace.Enabled() {
		fetchStart = time.Now()
		cacheHitsBefore, cacheMissesBefore, _, _ = reader.GetBlockCacheStats()
	}
	results, timings, err := fetchActionsByGlobalSeqs(reader, selectedSeqs, effectiveAbiReader, cfg.OmitNullFields, false)
	if err != nil {
		logger.Printf("error", "account_activity failed for account=%s contract=%s action=%s: %v", req.accountName, req.Contract, req.Action, err)
		writeAccountActivityError(w, "internal_error", "failed to fetch actions", http.StatusInternalServerError)
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

	response := AccountActivityResponse{
		Results: results,
	}

	// Results are now always in req.Order (reversed if needed above)
	// Generate cursors based on req.Order, not the cursor's query order
	reqDescending := req.Order == "desc"

	if len(results) > 0 && len(results) == limit {
		lastSeq := selectedSeqs[len(selectedSeqs)-1]
		nextCursor := &ActivityCursor{
			GlobalSeq:        lastSeq,
			Order:            req.Order,
			QueryFingerprint: queryFingerprint,
		}
		response.NextCursor = encodeActivityCursor(nextCursor)
	}

	if cursor != nil && len(results) > 0 {
		firstSeq := selectedSeqs[0]

		// Check if we're at the first page based on req.Order.
		// Results are already in req.Order, so:
		// - For desc: firstSeq is highest, check for even higher seqs (ascending query)
		// - For asc: firstSeq is lowest, check for even lower seqs (descending query)
		boundarySeq := firstSeq
		checkDescForBoundary := !reqDescending

		var hasPrev bool
		if dateRange != nil {
			prevSeqs, _ := indexes.LoadActionsFromCursorWithDateRange(account, req.Contract, req.Action, boundarySeq, dateRange, 1, checkDescForBoundary, trace)
			hasPrev = len(prevSeqs) > 0
		} else {
			prevSeqs, _ := indexes.LoadActionsFromCursor(account, req.Contract, req.Action, boundarySeq, 1, checkDescForBoundary, trace)
			hasPrev = len(prevSeqs) > 0
		}
		if hasPrev {
			prevOrder := "asc"
			if req.Order == "asc" {
				prevOrder = "desc"
			}
			prevCursor := &ActivityCursor{
				GlobalSeq:        firstSeq,
				Order:            prevOrder,
				QueryFingerprint: queryFingerprint,
			}
			response.PrevCursor = encodeActivityCursor(prevCursor)
		}
	}

	indexUsed := "AllActions"
	if req.Contract != "" && req.Action != "" {
		indexUsed = "ContractAction"
	} else if req.Contract != "" {
		indexUsed = "ContractWildcard"
	}
	if dateRange != nil {
		indexUsed += "+DateRange"
	}
	trace.SetResult(len(results), indexUsed)
	trace.Log()

	if req.Trace && trace.Enabled() {
		response.Trace = trace.ToJSON()
	}

	writeJSON(w, response)

	logger.Printf("timing", "account_activity account=%s contract=%s action=%s date=%s results=%d duration=%v",
		req.accountName, req.Contract, req.Action, req.Date, len(results), time.Since(startTime))
}

func parseAccountActivityRequest(r *http.Request) (*AccountActivityRequest, error) {
	req := &AccountActivityRequest{
		Limit:  100,
		Order:  "desc",
		Decode: true,
	}

	path := r.URL.Path
	accountName := extractAccountFromPath(path)
	if accountName == "" {
		return nil, fmt.Errorf("could not extract account name from path")
	}
	req.accountName = accountName

	params, err := server.GetRequestParams(r)
	if err != nil && r.Method == http.MethodPost {
		return nil, err
	}

	if limitParam, ok := params["limit"]; ok {
		if limit, ok := encoding.MaybeGetInt64(limitParam); ok {
			req.Limit = limit
			if req.Limit < 1 {
				req.Limit = 1
			}
			if req.Limit > 1000 {
				req.Limit = 1000
			}
		}
	}
	if v, ok := params["order"].(string); ok {
		if v == "asc" || v == "desc" {
			req.Order = v
		}
	}
	if v, ok := params["cursor"].(string); ok {
		req.Cursor = v
	}
	if v, ok := params["contract"].(string); ok {
		req.Contract = v
	}
	if v, ok := params["action"].(string); ok {
		req.Action = v
	}
	if v, ok := params["date"].(string); ok {
		req.Date = v
	}
	if v, ok := params["start_date"].(string); ok {
		req.StartDate = v
	}
	if v, ok := params["end_date"].(string); ok {
		req.EndDate = v
	}
	if v, ok := params["decode"].(bool); ok {
		req.Decode = v
		req.decodeExplicit = true
	} else if v, ok := params["decode"].(string); ok {
		req.Decode = v == "true" || v == "1"
		req.decodeExplicit = true
	}
	if v, ok := params["trace"].(bool); ok {
		req.Trace = v
	} else if v, ok := params["trace"].(string); ok {
		req.Trace = v == "true" || v == "1"
	}

	return req, nil
}

func extractAccountFromPath(path string) string {
	path = strings.TrimPrefix(path, "/account/")
	path = strings.TrimSuffix(path, "/activity")
	if path == "" || strings.Contains(path, "/") {
		return ""
	}
	return path
}

func computeAccountActivityFingerprint(req *AccountActivityRequest) string {
	data := fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		req.accountName, req.Contract, req.Action,
		req.Date, req.StartDate, req.EndDate)
	hash := sha256.Sum256([]byte(data))
	return base64.StdEncoding.EncodeToString(hash[:8])
}

func writeAccountActivityError(w http.ResponseWriter, code, message string, status int) {
	server.WriteJSON(w, status, AccountActivityErrorResponse{
		Error: AccountActivityError{
			Code:    code,
			Message: message,
		},
	})
}

type ActivityCursor struct {
	GlobalSeq        uint64 `json:"g"`
	Order            string `json:"o"`
	QueryFingerprint string `json:"q"`
}

func encodeActivityCursor(cursor *ActivityCursor) string {
	data, _ := encoding.JSONiter.Marshal(cursor)
	return base64.StdEncoding.EncodeToString(data)
}

func decodeActivityCursor(s string) (*ActivityCursor, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var cursor ActivityCursor
	if err := encoding.JSONiter.Unmarshal(data, &cursor); err != nil {
		return nil, err
	}
	return &cursor, nil
}
