package internal

import (
	"net/http"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
)

type LogEntry struct {
	GlobalActionSeq uint64         `json:"global_action_seq"`
	BlockNum        uint32         `json:"block_num"`
	BlockTime       string         `json:"block_time"`
	Action          map[string]any `json:"action"`
	TrxID           string         `json:"trx_id"`
}

type AccountLogResponse struct {
	Results    []LogEntry              `json:"results"`
	NextCursor string                  `json:"next_cursor,omitempty"`
	PrevCursor string                  `json:"prev_cursor,omitempty"`
	Trace      *querytrace.TraceOutput `json:"trace,omitempty"`
}

func HandleAccountLog(
	cfg *Config,
	store *Store,
	indexes ActionIndexer,
	reader corereader.Reader,
	abiReader *abicache.Reader,
	w http.ResponseWriter,
	r *http.Request,
) {
	startTime := time.Now()

	q, ok := newAccountQuery(cfg, indexes, reader, abiReader, w, r, "/log", "account_log")
	if !ok {
		return
	}

	if !q.execute() {
		return
	}

	results, ok := q.fetchLogEntries()
	if !ok {
		return
	}

	response := AccountLogResponse{
		Results:    results,
		NextCursor: q.nextCursor,
		PrevCursor: q.prevCursor,
		Trace:      q.finalize(len(results)),
	}

	writeJSON(w, response)

	logger.Printf("timing", "account_log account=%s contract=%s action=%s date=%s results=%d duration=%v",
		q.req.accountName, q.req.Contract, q.req.Action, q.req.Date, len(results), time.Since(startTime))
}
