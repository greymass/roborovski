package internal

import (
	"net/http"
	"strings"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/server"
)

type AccountStatsResponse struct {
	Account         string `json:"account"`
	TotalActions    int    `json:"total_actions"`
	FirstActionSeq  uint64 `json:"first_action_seq,omitempty"`
	LastActionSeq   uint64 `json:"last_action_seq,omitempty"`
	FirstActionDate string `json:"first_action_date,omitempty"`
	LastActionDate  string `json:"last_action_date,omitempty"`
}

type AccountStatsErrorResponse struct {
	Error AccountStatsError `json:"error"`
}

type AccountStatsError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func HandleAccountStats(indexes ActionIndexer, w http.ResponseWriter, r *http.Request) {
	idx, ok := indexes.(*Indexes)
	if !ok {
		writeAccountStatsError(w, "internal_error", "indexes not available", http.StatusInternalServerError)
		return
	}

	if idx.IsBulkMode() {
		writeAccountStatsError(w, "service_unavailable", "service is syncing", http.StatusServiceUnavailable)
		return
	}

	accountName := extractAccountFromStatsPath(r.URL.Path)
	if accountName == "" {
		writeAccountStatsError(w, "missing_account", "account name required in path", http.StatusBadRequest)
		return
	}

	accountID := chain.StringToName(accountName)
	if accountID == 0 {
		writeAccountStatsError(w, "invalid_account", "invalid account name", http.StatusBadRequest)
		return
	}

	chunkCount, err := idx.chunkReader.GetTotalCount(accountID)
	if err != nil {
		writeAccountStatsError(w, "internal_error", err.Error(), http.StatusInternalServerError)
		return
	}

	walSeqs, err := idx.walReader.GetEntriesForAccount(accountID)
	if err != nil {
		writeAccountStatsError(w, "internal_error", err.Error(), http.StatusInternalServerError)
		return
	}
	walCount := len(walSeqs)

	totalActions := chunkCount + walCount

	resp := AccountStatsResponse{
		Account:      accountName,
		TotalActions: totalActions,
	}

	if totalActions > 0 {
		firstBase, lastBase := idx.metadata.GetAllActionsSeqRange(accountID)

		var firstSeq, lastSeq uint64

		if firstBase > 0 {
			firstSeq = firstBase
		}
		if lastBase > 0 {
			lastSeq = lastBase
		}

		for _, seq := range walSeqs {
			if firstSeq == 0 || seq < firstSeq {
				firstSeq = seq
			}
			if seq > lastSeq {
				lastSeq = seq
			}
		}

		resp.FirstActionSeq = firstSeq
		resp.LastActionSeq = lastSeq

		if firstSeq > 0 {
			if hour, ok := idx.timeMapper.SeqToHour(firstSeq); ok {
				resp.FirstActionDate = hourToDateString(hour)
			}
		}
		if lastSeq > 0 {
			if hour, ok := idx.timeMapper.SeqToHour(lastSeq); ok {
				resp.LastActionDate = hourToDateString(hour)
			}
		}
	}

	writeJSON(w, resp)
}

func extractAccountFromStatsPath(path string) string {
	path = strings.TrimPrefix(path, "/account/")
	path = strings.TrimSuffix(path, "/stats")
	if path == "" || strings.Contains(path, "/") {
		return ""
	}
	return path
}

func writeAccountStatsError(w http.ResponseWriter, code, message string, status int) {
	server.WriteJSON(w, status, AccountStatsErrorResponse{
		Error: AccountStatsError{
			Code:    code,
			Message: message,
		},
	})
}
