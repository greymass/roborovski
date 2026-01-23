package internal

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/greymass/roborovski/libraries/abicache"
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

	q, ok := newAccountQuery(cfg, indexes, reader, abiReader, w, r, "/activity", "account_activity")
	if !ok {
		return
	}

	if !q.execute() {
		return
	}

	results, ok := q.fetchActions()
	if !ok {
		return
	}

	response := AccountActivityResponse{
		Results:    results,
		NextCursor: q.nextCursor,
		PrevCursor: q.prevCursor,
		Trace:      q.finalize(len(results)),
	}

	writeJSON(w, response)

	logger.Printf("timing", "account_activity account=%s contract=%s action=%s date=%s results=%d duration=%v",
		q.req.accountName, q.req.Contract, q.req.Action, q.req.Date, len(results), time.Since(startTime))
}

func parseAccountRequestWithSuffix(r *http.Request, suffix string) (*AccountActivityRequest, error) {
	req := &AccountActivityRequest{
		Limit:  100,
		Order:  "desc",
		Decode: true,
	}

	path := r.URL.Path
	accountName := extractAccountFromPath(path, suffix)
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

func extractAccountFromPath(path, suffix string) string {
	path = strings.TrimPrefix(path, "/account/")
	path = strings.TrimSuffix(path, suffix)
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
