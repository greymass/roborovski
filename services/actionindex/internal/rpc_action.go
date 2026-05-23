package internal

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/server"
)

type ActionLookupResponse struct {
	GlobalActionSeq uint64      `json:"global_action_seq"`
	BlockNum        uint32      `json:"block_num"`
	BlockTime       string      `json:"block_time"`
	Action          any `json:"action,omitempty"`
	ActionTrace     any `json:"action_trace,omitempty"`
	TrxID           string      `json:"trx_id"`
}

type ActionLookupError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ActionLookupErrorResponse struct {
	Error ActionLookupError `json:"error"`
}

func writeActionLookupError(w http.ResponseWriter, code, message string, status int) {
	server.WriteJSON(w, status, ActionLookupErrorResponse{
		Error: ActionLookupError{
			Code:    code,
			Message: message,
		},
	})
}

func HandleAction(cfg *Config, reader corereader.Reader, abiReader *abicache.Reader, w http.ResponseWriter, r *http.Request) {
	seqStr := strings.TrimPrefix(r.URL.Path, "/action/")
	if seqStr == "" || strings.Contains(seqStr, "/") {
		writeActionLookupError(w, "missing_seq", "sequence number required in path", http.StatusBadRequest)
		return
	}

	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		writeActionLookupError(w, "invalid_seq", "sequence must be a valid uint64", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()

	trace := false
	if v := query.Get("trace"); v == "true" || v == "1" {
		trace = true
	}

	decode := true
	if v := query.Get("decode"); v == "false" || v == "0" {
		decode = false
	}

	actions, _, err := getActionsByGlobalSeqs(reader, []uint64{seq})
	if err != nil {
		writeActionLookupError(w, "internal_error", "failed to fetch action", http.StatusInternalServerError)
		return
	}

	if len(actions) == 0 {
		writeActionLookupError(w, "not_found", "action not found", http.StatusNotFound)
		return
	}

	at := actions[0]

	var effectiveAbiReader *abicache.Reader
	if decode {
		effectiveAbiReader = abiReader
	}

	resp := ActionLookupResponse{
		GlobalActionSeq: seq,
		BlockNum:        at.BlockNum,
		BlockTime:       at.BlockTime,
		TrxID:           at.TrxID,
	}

	if trace {
		resp.ActionTrace = buildActionTrace(at, effectiveAbiReader, cfg.OmitNullFields)
	} else {
		resp.Action = buildAction(at.Act, effectiveAbiReader, at.BlockNum, cfg.OmitNullFields)
	}

	writeJSON(w, resp)
}
