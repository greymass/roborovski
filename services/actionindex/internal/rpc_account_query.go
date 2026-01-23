package internal

import (
	"fmt"
	"net/http"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
)

type accountQuery struct {
	cfg       *Config
	indexes   ActionIndexer
	reader    corereader.Reader
	abiReader *abicache.Reader
	w         http.ResponseWriter

	req              *AccountActivityRequest
	trace            *querytrace.Tracer
	account          uint64
	dateRange        *DateRange
	queryFingerprint string
	shouldDecode     bool

	selectedSeqs []uint64
	timings      *corereader.FetchTimings
	nextCursor   string
	prevCursor   string
	indexUsed    string
}

func newAccountQuery(
	cfg *Config,
	indexes ActionIndexer,
	reader corereader.Reader,
	abiReader *abicache.Reader,
	w http.ResponseWriter,
	r *http.Request,
	suffix string,
	traceName string,
) (*accountQuery, bool) {
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
		return nil, false
	}

	req, err := parseAccountRequestWithSuffix(r, suffix)
	if err != nil {
		writeAccountActivityError(w, "invalid_request", err.Error(), http.StatusBadRequest)
		return nil, false
	}

	if req.accountName == "" {
		writeAccountActivityError(w, "missing_account", "account name required in path", http.StatusBadRequest)
		return nil, false
	}

	if req.Action != "" && req.Contract == "" {
		writeAccountActivityError(w, "invalid_filter", "action filter requires contract to be specified", http.StatusBadRequest)
		return nil, false
	}

	if req.Date != "" && (req.StartDate != "" || req.EndDate != "") {
		writeAccountActivityError(w, "invalid_filter", "date parameter cannot be combined with start_date or end_date", http.StatusBadRequest)
		return nil, false
	}

	if req.decodeExplicit && req.Decode && abiReader == nil {
		writeAccountActivityError(w, "decode_unavailable", "decode=true requested but ABI data is not available", http.StatusBadRequest)
		return nil, false
	}

	q := &accountQuery{
		cfg:          cfg,
		indexes:      indexes,
		reader:       reader,
		abiReader:    abiReader,
		w:            w,
		req:          req,
		trace:        querytrace.New(traceName, req.accountName),
		account:      chain.StringToName(req.accountName),
		shouldDecode: abiReader != nil && req.Decode,
	}

	_, _, err = indexes.GetProperties()
	if err != nil {
		writeAccountActivityError(w, "internal_error", "failed to get database state", http.StatusInternalServerError)
		return nil, false
	}

	q.dateRange, err = parseDateParam(req.Date, req.StartDate, req.EndDate)
	if err != nil {
		writeAccountActivityError(w, "invalid_date", err.Error(), http.StatusBadRequest)
		return nil, false
	}

	q.queryFingerprint = computeAccountActivityFingerprint(req)

	return q, true
}

func (q *accountQuery) execute() bool {
	req := q.req

	var cursor *ActivityCursor
	var err error
	if req.Cursor != "" {
		cursor, err = decodeActivityCursor(req.Cursor)
		if err != nil {
			writeAccountActivityError(q.w, "invalid_cursor", fmt.Sprintf("invalid cursor: %v", err), http.StatusBadRequest)
			return false
		}
	}

	if cursor != nil && cursor.QueryFingerprint != q.queryFingerprint {
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
		globalSeqs, err = q.indexes.LoadActionsFromCursorWithDateRange(q.account, req.Contract, req.Action, cursor.GlobalSeq, q.dateRange, limit, descending, q.trace)
	} else if q.dateRange != nil {
		globalSeqs, err = q.indexes.LoadActionsWithDateRange(q.account, req.Contract, req.Action, q.dateRange, limit, descending, q.trace)
	} else {
		globalSeqs, err = q.indexes.LoadActions(q.account, req.Contract, req.Action, limit, descending, q.trace)
	}
	if err != nil {
		writeAccountActivityError(q.w, "internal_error", "failed to load actions", http.StatusInternalServerError)
		return false
	}

	if len(globalSeqs) == 0 {
		q.selectedSeqs = globalSeqs
		q.computeIndexUsed()
		return true
	}

	var sliceStart time.Time
	if q.trace.Enabled() {
		sliceStart = time.Now()
	}

	q.selectedSeqs = globalSeqs

	if cursor != nil && cursor.Order != req.Order {
		for i, j := 0, len(q.selectedSeqs)-1; i < j; i, j = i+1, j-1 {
			q.selectedSeqs[i], q.selectedSeqs[j] = q.selectedSeqs[j], q.selectedSeqs[i]
		}
	}

	if q.trace.Enabled() {
		q.trace.AddStepWithCount("slice", "paginate", time.Since(sliceStart), len(q.selectedSeqs), fmt.Sprintf("order=%s", req.Order))
	}

	q.computeCursors(cursor, limit)
	q.computeIndexUsed()

	return true
}

func (q *accountQuery) computeCursors(cursor *ActivityCursor, limit int) {
	req := q.req
	reqDescending := req.Order == "desc"

	if len(q.selectedSeqs) > 0 && len(q.selectedSeqs) == limit {
		lastSeq := q.selectedSeqs[len(q.selectedSeqs)-1]
		nextCursor := &ActivityCursor{
			GlobalSeq:        lastSeq,
			Order:            req.Order,
			QueryFingerprint: q.queryFingerprint,
		}
		q.nextCursor = encodeActivityCursor(nextCursor)
	}

	if cursor != nil && len(q.selectedSeqs) > 0 {
		firstSeq := q.selectedSeqs[0]
		boundarySeq := firstSeq
		checkDescForBoundary := !reqDescending

		var hasPrev bool
		if q.dateRange != nil {
			prevSeqs, _ := q.indexes.LoadActionsFromCursorWithDateRange(q.account, req.Contract, req.Action, boundarySeq, q.dateRange, 1, checkDescForBoundary, q.trace)
			hasPrev = len(prevSeqs) > 0
		} else {
			prevSeqs, _ := q.indexes.LoadActionsFromCursor(q.account, req.Contract, req.Action, boundarySeq, 1, checkDescForBoundary, q.trace)
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
				QueryFingerprint: q.queryFingerprint,
			}
			q.prevCursor = encodeActivityCursor(prevCursor)
		}
	}
}

func (q *accountQuery) computeIndexUsed() {
	q.indexUsed = "AllActions"
	if q.req.Contract != "" && q.req.Action != "" {
		q.indexUsed = "ContractAction"
	} else if q.req.Contract != "" {
		q.indexUsed = "ContractWildcard"
	}
	if q.dateRange != nil {
		q.indexUsed += "+DateRange"
	}
}

func (q *accountQuery) effectiveAbiReader() *abicache.Reader {
	if q.shouldDecode {
		return q.abiReader
	}
	return nil
}

func (q *accountQuery) fetchActions() ([]ActionResult, bool) {
	if len(q.selectedSeqs) == 0 {
		return []ActionResult{}, true
	}

	var fetchStart time.Time
	var cacheHitsBefore, cacheMissesBefore uint64
	if q.trace.Enabled() {
		fetchStart = time.Now()
		cacheHitsBefore, cacheMissesBefore, _, _ = q.reader.GetBlockCacheStats()
	}

	results, timings, err := fetchActionsByGlobalSeqs(q.reader, q.selectedSeqs, q.effectiveAbiReader(), q.cfg.OmitNullFields, false)
	if err != nil {
		logger.Printf("error", "account query failed for account=%s contract=%s action=%s: %v",
			q.req.accountName, q.req.Contract, q.req.Action, err)
		writeAccountActivityError(q.w, "internal_error", "failed to fetch actions", http.StatusInternalServerError)
		return nil, false
	}

	q.timings = timings
	q.addFetchTrace(fetchStart, cacheHitsBefore, cacheMissesBefore, len(results))

	return results, true
}

func (q *accountQuery) fetchLogEntries() ([]LogEntry, bool) {
	if len(q.selectedSeqs) == 0 {
		return []LogEntry{}, true
	}

	var fetchStart time.Time
	var cacheHitsBefore, cacheMissesBefore uint64
	if q.trace.Enabled() {
		fetchStart = time.Now()
		cacheHitsBefore, cacheMissesBefore, _, _ = q.reader.GetBlockCacheStats()
	}

	results, timings, err := fetchLogEntriesByGlobalSeqs(q.reader, q.selectedSeqs, q.effectiveAbiReader(), q.cfg.OmitNullFields)
	if err != nil {
		logger.Printf("error", "account query failed for account=%s contract=%s action=%s: %v",
			q.req.accountName, q.req.Contract, q.req.Action, err)
		writeAccountActivityError(q.w, "internal_error", "failed to fetch actions", http.StatusInternalServerError)
		return nil, false
	}

	q.timings = timings
	q.addFetchTrace(fetchStart, cacheHitsBefore, cacheMissesBefore, len(results))

	return results, true
}

func (q *accountQuery) addFetchTrace(fetchStart time.Time, cacheHitsBefore, cacheMissesBefore uint64, resultCount int) {
	if !q.trace.Enabled() {
		return
	}

	cacheHitsAfter, cacheMissesAfter, cacheSize, cacheBlocks := q.reader.GetBlockCacheStats()
	hits := cacheHitsAfter - cacheHitsBefore
	misses := cacheMissesAfter - cacheMissesBefore
	details := fmt.Sprintf("cache=%d/%d size=%.1fMB blocks=%d", hits, hits+misses, float64(cacheSize)/(1024*1024), cacheBlocks)
	if q.timings != nil {
		details += fmt.Sprintf(" phase1=%v(a=%v b=%v c=%v) phase2=%v decompress=%v(%d) parse=%v(%d) match=%v",
			q.timings.Phase1, q.timings.Phase1a, q.timings.Phase1b, q.timings.Phase1c,
			q.timings.Phase2,
			q.timings.DecompressTime, q.timings.DecompressCount,
			q.timings.ParseTime, q.timings.ParseCount,
			q.timings.MatchTime)
	}
	q.trace.AddStepWithCount("corereader", "fetch", time.Since(fetchStart), resultCount, details)
}

func (q *accountQuery) finalize(resultCount int) *querytrace.TraceOutput {
	q.trace.SetResult(resultCount, q.indexUsed)
	q.trace.Log()

	if q.req.Trace && q.trace.Enabled() {
		return q.trace.ToJSON()
	}
	return nil
}
