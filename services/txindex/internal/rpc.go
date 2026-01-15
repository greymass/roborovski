package internal

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/greymass/go-eosio/pkg/base58"
	antelope "github.com/greymass/go-eosio/pkg/chain"
	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/querytrace"
	"github.com/greymass/roborovski/libraries/server"
)

type RPCServer struct {
	idx       *TrxIndex
	reader    corereader.Reader
	abiReader *abicache.Reader
	config    *Config
}

func NewRPCServer(idx *TrxIndex, reader corereader.Reader, abiReader *abicache.Reader, config *Config) *RPCServer {
	return &RPCServer{
		idx:       idx,
		reader:    reader,
		abiReader: abiReader,
		config:    config,
	}
}

func (s *RPCServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	switch {
	case path == "/openapi.json" || path == "/openapi.yaml":
		handleOpenAPI(w, r)
	case path == "/v1/history/get_transaction":
		s.handleGetTransaction(w, r)
	case path == "/v1/history/get_transaction_status":
		s.handleGetTransactionStatus(w, r)
	case strings.HasPrefix(path, "/transaction/") && strings.HasSuffix(path, "/status"):
		s.handleTransactionStatus(w, r)
	case strings.HasPrefix(path, "/transaction/"):
		s.handleTransaction(w, r)
	default:
		s.writeError(w, http.StatusNotFound, "unknown endpoint: "+path)
	}
}

func (s *RPCServer) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	var trxID string
	fetchTraces := true

	if r.Method == http.MethodGet {
		trxID = r.URL.Query().Get("id")
		if tracesParam := r.URL.Query().Get("traces"); tracesParam != "" {
			fetchTraces = tracesParam != "false" && tracesParam != "0"
		}
	} else {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		defer r.Body.Close()

		var req struct {
			ID     string `json:"id"`
			Traces *bool  `json:"traces"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		trxID = req.ID
		if req.Traces != nil {
			fetchTraces = *req.Traces
		}
	}

	if trxID == "" {
		s.writeError(w, http.StatusBadRequest, "id required")
		return
	}

	trxID = strings.ToLower(trxID)

	if len(trxID) < 64 {
		s.writeError(w, http.StatusBadRequest, "invalid transaction id length")
		return
	}

	trace := querytrace.New("get_transaction", trxID)

	var lookupStart time.Time
	if trace.Enabled() {
		lookupStart = time.Now()
	}

	blockNum, err := s.idx.Lookup(trxID)
	if err != nil {
		if err == ErrNotFound {
			s.writeError(w, http.StatusNotFound, "transaction not found")
		} else {
			logger.Printf("http", "Lookup error: %v", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	if trace.Enabled() {
		trace.AddStep("index", "lookup", time.Since(lookupStart), "")
	}

	var propsStart time.Time
	if trace.Enabled() {
		propsStart = time.Now()
	}

	_, lib, err := s.reader.GetStateProps(true)
	if err != nil {
		logger.Printf("http", "GetStateProps error: %v", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if trace.Enabled() {
		trace.AddStep("props", "load", time.Since(propsStart), "")
	}

	var actionsStart time.Time
	if trace.Enabled() {
		actionsStart = time.Now()
	}

	txData, err := s.reader.GetTransactionData(blockNum, trxID)
	if err != nil {
		logger.Printf("http", "GetTransactionData error: %v", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	actions := txData.Actions
	blockTime := txData.BlockTime

	if trace.Enabled() {
		trace.AddStep("actions", "fetch", time.Since(actionsStart), "")
	}

	var traces any
	if fetchTraces {
		var decodeStart time.Time
		if trace.Enabled() {
			decodeStart = time.Now()
		}

		sort.Slice(actions, func(i, j int) bool {
			return actions[i].GlobalSeqUint64 < actions[j].GlobalSeqUint64
		})

		traceList := make([]map[string]any, len(actions))
		for i, action := range actions {
			traceList[i] = s.actionToMap(action, blockNum)
		}

		traces = traceList

		if trace.Enabled() {
			trace.AddStep("decode", "abi", time.Since(decodeStart), "")
		}
	}

	trace.Log()

	logger.Printf("timing", "get_transaction trx=%s block=%d traces=%d duration=%v",
		trxID[:16], blockNum, len(actions), time.Since(startTime))

	resp := map[string]any{
		"id":                      trxID,
		"block_num":               blockNum,
		"block_time":              blockTime,
		"head_block_num":          lib,
		"last_irreversible_block": lib,
		"irreversible":            true,
		"traces":                  traces,
		"transaction_num":         txData.TrxIndex,
		"trx":                     s.buildTrxField(txData, actions, blockNum),
	}

	s.writeJSON(w, http.StatusOK, resp)
}

func (s *RPCServer) actionToMap(action chain.ActionTrace, blockNum uint32) map[string]any {
	actMap := map[string]any{
		"account": action.Act.Account,
		"name":    action.Act.Name,
		"authorization": func() []map[string]string {
			auths := make([]map[string]string, len(action.Act.Authorization))
			for i, auth := range action.Act.Authorization {
				auths[i] = map[string]string{
					"actor":      auth.Actor,
					"permission": auth.Permission,
				}
			}
			return auths
		}(),
	}

	if s.abiReader != nil {
		contractNum := chain.StringToName(action.Act.Account)
		actionNum := chain.StringToName(action.Act.Name)
		decoded, err := s.abiReader.DecodeHex(contractNum, actionNum, action.Act.Data, blockNum)
		if err == nil {
			actMap["data"] = convertLargeIntsToStrings(decoded)
			actMap["hex_data"] = action.Act.Data
		} else {
			actMap["data"] = action.Act.Data
		}
	} else {
		actMap["data"] = action.Act.Data
	}

	receipt := map[string]any{
		"receiver":        action.Receipt.Receiver,
		"act_digest":      action.Receipt.ActDigest,
		"global_sequence": action.Receipt.GlobalSequence,
		"recv_sequence":   action.Receipt.RecvSequence,
		"auth_sequence":   action.Receipt.AuthSequence,
		"code_sequence":   action.Receipt.CodeSequence,
		"abi_sequence":    action.Receipt.AbiSequence,
	}

	ramDeltas := make([]map[string]any, len(action.AccountRAMDeltas))
	for i, delta := range action.AccountRAMDeltas {
		ramDeltas[i] = map[string]any{
			"account": delta.Account,
			"delta":   delta.Delta,
		}
	}

	return map[string]any{
		"action_ordinal":                             action.ActionOrdinal,
		"creator_action_ordinal":                     action.CreatorAO,
		"closest_unnotified_ancestor_action_ordinal": action.ClosestUAAO,
		"receipt":            receipt,
		"receiver":           action.Receiver,
		"act":                actMap,
		"context_free":       action.ContextFree,
		"elapsed":            action.Elapsed,
		"trx_id":             action.TrxID,
		"block_num":          action.BlockNum,
		"block_time":         action.BlockTime,
		"producer_block_id":  action.ProducerBlockID,
		"account_ram_deltas": ramDeltas,
	}
}

func (s *RPCServer) handleGetTransactionStatus(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	var trxID string

	if r.Method == http.MethodGet {
		trxID = r.URL.Query().Get("id")
	} else {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		defer r.Body.Close()

		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		trxID = req.ID
	}

	if trxID == "" {
		s.writeError(w, http.StatusBadRequest, "id required")
		return
	}

	trxID = strings.ToLower(trxID)

	if len(trxID) < 64 {
		s.writeError(w, http.StatusBadRequest, "invalid transaction id length")
		return
	}

	trace := querytrace.New("get_transaction_status", trxID)

	var lookupStart time.Time
	if trace.Enabled() {
		lookupStart = time.Now()
	}

	blockNum, err := s.idx.Lookup(trxID)
	if err != nil {
		if err == ErrNotFound {
			s.writeError(w, http.StatusNotFound, "transaction not found")
		} else {
			logger.Printf("http", "Lookup error: %v", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	if trace.Enabled() {
		trace.AddStep("index", "lookup", time.Since(lookupStart), "")
	}

	var propsStart time.Time
	if trace.Enabled() {
		propsStart = time.Now()
	}

	_, lib, err := s.reader.GetStateProps(true)
	if err != nil {
		logger.Printf("http", "GetStateProps error: %v", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if trace.Enabled() {
		trace.AddStep("props", "load", time.Since(propsStart), "")
	}

	var blockIDStart time.Time
	if trace.Enabled() {
		blockIDStart = time.Now()
	}

	_, blockID, _, err := s.reader.GetTransactionIDsOnly(blockNum, false)
	if err != nil {
		logger.Printf("http", "GetTransactionIDsOnly error: %v", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if trace.Enabled() {
		trace.AddStep("blockid", "fetch", time.Since(blockIDStart), "")
	}

	trace.Log()

	logger.Printf("timing", "get_transaction_status trx=%s block=%d duration=%v",
		trxID[:16], blockNum, time.Since(startTime))

	resp := map[string]any{
		"state":               "IRREVERSIBLE",
		"block_number":        blockNum,
		"block_id":            blockID,
		"head_number":         lib,
		"irreversible_number": lib,
	}

	s.writeJSON(w, http.StatusOK, resp)
}

func (s *RPCServer) writeJSON(w http.ResponseWriter, status int, data any) {
	server.WriteJSON(w, status, data)
}

func (s *RPCServer) writeError(w http.ResponseWriter, status int, message string) {
	server.WriteError(w, status, message)
}

func convertLargeIntsToStrings(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(data))
	for k, v := range data {
		result[k] = convertValue(v)
	}
	return result
}

func convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case bool:
		if val {
			return 1
		}
		return 0
	case uint64:
		if val > 0xFFFFFFFF {
			return strconv.FormatUint(val, 10)
		}
		return val
	case int64:
		if val > 0x7FFFFFFF || val < -0x80000000 {
			return strconv.FormatInt(val, 10)
		}
		return val
	case map[string]interface{}:
		return convertLargeIntsToStrings(val)
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = convertValue(item)
		}
		return result
	default:
		return v
	}
}

func (s *RPCServer) buildTrxField(txData *corereader.TransactionData, actions []chain.ActionTrace, blockNum uint32) map[string]any {
	signatures := make([]string, 0, len(txData.Signatures))
	for _, sigBytes := range txData.Signatures {
		if sig := encodeSignature(sigBytes); sig != "" {
			signatures = append(signatures, sig)
		}
	}

	var formattedActions []map[string]any
	for _, action := range actions {
		if action.CreatorAO != 0 {
			continue
		}

		actMap := map[string]any{
			"account": action.Act.Account,
			"name":    action.Act.Name,
			"authorization": func() []map[string]string {
				auths := make([]map[string]string, len(action.Act.Authorization))
				for j, auth := range action.Act.Authorization {
					auths[j] = map[string]string{
						"actor":      auth.Actor,
						"permission": auth.Permission,
					}
				}
				return auths
			}(),
		}

		if s.abiReader != nil {
			contractNum := chain.StringToName(action.Act.Account)
			actionNum := chain.StringToName(action.Act.Name)
			decoded, err := s.abiReader.DecodeHex(contractNum, actionNum, action.Act.Data, blockNum)
			if err == nil {
				actMap["data"] = convertLargeIntsToStrings(decoded)
				actMap["hex_data"] = action.Act.Data
			} else {
				actMap["data"] = action.Act.Data
			}
		} else {
			actMap["data"] = action.Act.Data
		}

		formattedActions = append(formattedActions, actMap)
	}

	return map[string]any{
		"receipt": map[string]any{
			"status":          statusToString(txData.Status),
			"cpu_usage_us":    txData.CpuUsageUs,
			"net_usage_words": txData.NetUsageWords,
			"trx": []any{
				1,
				map[string]any{
					"signatures":               signatures,
					"compression":              "none",
					"packed_context_free_data": "",
					"packed_trx":               buildPackedTrx(txData, actions),
				},
			},
		},
		"trx": map[string]any{
			"expiration":           expirationToString(txData.Expiration),
			"ref_block_num":        txData.RefBlockNum,
			"ref_block_prefix":     txData.RefBlockPrefix,
			"max_net_usage_words":  0,
			"max_cpu_usage_ms":     0,
			"delay_sec":            0,
			"context_free_actions": []any{},
			"actions":              formattedActions,
			"signatures":           signatures,
			"context_free_data":    []any{},
		},
	}
}

func statusToString(status uint8) string {
	switch status {
	case 0:
		return "executed"
	case 1:
		return "soft_fail"
	case 2:
		return "hard_fail"
	case 3:
		return "delayed"
	case 4:
		return "expired"
	default:
		return "unknown"
	}
}

func expirationToString(expiration uint32) string {
	t := time.Unix(int64(expiration), 0).UTC()
	return t.Format("2006-01-02T15:04:05")
}

func encodeSignature(sigBytes []byte) string {
	if len(sigBytes) < 2 {
		return ""
	}

	keyType := sigBytes[0]
	data := sigBytes[1:]

	var suffix string
	var prefix string
	switch keyType {
	case 0:
		suffix = "K1"
		prefix = "SIG_K1_"
	case 1:
		suffix = "R1"
		prefix = "SIG_R1_"
	case 2:
		suffix = "WA"
		prefix = "SIG_WA_"
	default:
		return ""
	}

	return prefix + base58.CheckEncodeEosio(data, suffix)
}

func buildPackedTrx(txData *corereader.TransactionData, actions []chain.ActionTrace) string {
	var antelopeActions []antelope.Action
	for _, action := range actions {
		if action.CreatorAO != 0 {
			continue
		}

		auths := make([]antelope.PermissionLevel, len(action.Act.Authorization))
		for i, auth := range action.Act.Authorization {
			auths[i] = antelope.PermissionLevel{
				Actor:      antelope.N(auth.Actor),
				Permission: antelope.N(auth.Permission),
			}
		}

		dataBytes, _ := hex.DecodeString(action.Act.Data)
		antelopeActions = append(antelopeActions, antelope.Action{
			Account:       antelope.N(action.Act.Account),
			Name:          antelope.N(action.Act.Name),
			Authorization: auths,
			Data:          antelope.Bytes(dataBytes),
		})
	}

	tx := antelope.Transaction{
		TransactionHeader: antelope.TransactionHeader{
			Expiration:       antelope.TimePointSec(txData.Expiration),
			RefBlockNum:      txData.RefBlockNum,
			RefBlockPrefix:   txData.RefBlockPrefix,
			MaxNetUsageWords: 0,
			MaxCpuUsageMs:    0,
			DelaySec:         0,
		},
		ContextFreeActions: []antelope.Action{},
		Actions:            antelopeActions,
		Extensions:         []antelope.TransactionExtension{},
	}

	buf := new(bytes.Buffer)
	enc := antelope.NewEncoder(buf)
	if err := tx.MarshalABI(enc); err != nil {
		return ""
	}

	return hex.EncodeToString(buf.Bytes())
}

type TransactionResponse struct {
	Results interface{} `json:"results"`
}

type TransactionErrorResponse struct {
	Error TransactionError `json:"error"`
}

type TransactionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func extractTxIDFromPath(path string) string {
	path = strings.TrimPrefix(path, "/transaction/")
	path = strings.TrimSuffix(path, "/status")
	if path == "" || strings.Contains(path, "/") {
		return ""
	}
	return path
}

func (s *RPCServer) handleTransaction(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	trxID := extractTxIDFromPath(r.URL.Path)
	if trxID == "" {
		s.writeTransactionError(w, "missing_txid", "transaction id required in path", http.StatusBadRequest)
		return
	}

	trxID = strings.ToLower(trxID)

	if len(trxID) < 64 {
		s.writeTransactionError(w, "invalid_txid", "invalid transaction id length", http.StatusBadRequest)
		return
	}

	fetchTraces := true
	params, _ := server.GetRequestParams(r)
	if tracesParam, ok := params["traces"].(string); ok {
		fetchTraces = tracesParam != "false" && tracesParam != "0"
	} else if tracesParam, ok := params["traces"].(bool); ok {
		fetchTraces = tracesParam
	}

	trace := querytrace.New("transaction", trxID)

	var lookupStart time.Time
	if trace.Enabled() {
		lookupStart = time.Now()
	}

	blockNum, err := s.idx.Lookup(trxID)
	if err != nil {
		if err == ErrNotFound {
			s.writeTransactionError(w, "not_found", "transaction not found", http.StatusNotFound)
		} else {
			logger.Printf("http", "Lookup error: %v", err)
			s.writeTransactionError(w, "internal_error", "internal error", http.StatusInternalServerError)
		}
		return
	}

	if trace.Enabled() {
		trace.AddStep("index", "lookup", time.Since(lookupStart), "")
	}

	var propsStart time.Time
	if trace.Enabled() {
		propsStart = time.Now()
	}

	_, lib, err := s.reader.GetStateProps(true)
	if err != nil {
		logger.Printf("http", "GetStateProps error: %v", err)
		s.writeTransactionError(w, "internal_error", "internal error", http.StatusInternalServerError)
		return
	}

	if trace.Enabled() {
		trace.AddStep("props", "load", time.Since(propsStart), "")
	}

	var actionsStart time.Time
	if trace.Enabled() {
		actionsStart = time.Now()
	}

	txData, err := s.reader.GetTransactionData(blockNum, trxID)
	if err != nil {
		logger.Printf("http", "GetTransactionData error: %v", err)
		s.writeTransactionError(w, "internal_error", "internal error", http.StatusInternalServerError)
		return
	}
	actions := txData.Actions
	blockTime := txData.BlockTime

	if trace.Enabled() {
		trace.AddStep("actions", "fetch", time.Since(actionsStart), "")
	}

	var traces any
	if fetchTraces {
		var decodeStart time.Time
		if trace.Enabled() {
			decodeStart = time.Now()
		}

		sort.Slice(actions, func(i, j int) bool {
			return actions[i].GlobalSeqUint64 < actions[j].GlobalSeqUint64
		})

		traceList := make([]map[string]any, len(actions))
		for i, action := range actions {
			traceList[i] = s.actionToMap(action, blockNum)
		}

		traces = traceList

		if trace.Enabled() {
			trace.AddStep("decode", "abi", time.Since(decodeStart), "")
		}
	}

	trace.Log()

	logger.Printf("timing", "transaction trx=%s block=%d traces=%d duration=%v",
		trxID[:16], blockNum, len(actions), time.Since(startTime))

	result := map[string]any{
		"id":                      trxID,
		"block_num":               blockNum,
		"block_time":              blockTime,
		"head_block_num":          lib,
		"last_irreversible_block": lib,
		"irreversible":            true,
		"traces":                  traces,
		"transaction_num":         txData.TrxIndex,
		"trx":                     s.buildTrxField(txData, actions, blockNum),
	}

	s.writeJSON(w, http.StatusOK, TransactionResponse{Results: result})
}

func (s *RPCServer) handleTransactionStatus(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	path := strings.TrimSuffix(r.URL.Path, "/status")
	trxID := extractTxIDFromPath(path + "/status")
	if trxID == "" {
		s.writeTransactionError(w, "missing_txid", "transaction id required in path", http.StatusBadRequest)
		return
	}

	trxID = strings.ToLower(trxID)

	if len(trxID) < 64 {
		s.writeTransactionError(w, "invalid_txid", "invalid transaction id length", http.StatusBadRequest)
		return
	}

	trace := querytrace.New("transaction_status", trxID)

	var lookupStart time.Time
	if trace.Enabled() {
		lookupStart = time.Now()
	}

	blockNum, err := s.idx.Lookup(trxID)
	if err != nil {
		if err == ErrNotFound {
			s.writeTransactionError(w, "not_found", "transaction not found", http.StatusNotFound)
		} else {
			logger.Printf("http", "Lookup error: %v", err)
			s.writeTransactionError(w, "internal_error", "internal error", http.StatusInternalServerError)
		}
		return
	}

	if trace.Enabled() {
		trace.AddStep("index", "lookup", time.Since(lookupStart), "")
	}

	var propsStart time.Time
	if trace.Enabled() {
		propsStart = time.Now()
	}

	_, lib, err := s.reader.GetStateProps(true)
	if err != nil {
		logger.Printf("http", "GetStateProps error: %v", err)
		s.writeTransactionError(w, "internal_error", "internal error", http.StatusInternalServerError)
		return
	}

	if trace.Enabled() {
		trace.AddStep("props", "load", time.Since(propsStart), "")
	}

	var blockIDStart time.Time
	if trace.Enabled() {
		blockIDStart = time.Now()
	}

	_, blockID, _, err := s.reader.GetTransactionIDsOnly(blockNum, false)
	if err != nil {
		logger.Printf("http", "GetTransactionIDsOnly error: %v", err)
		s.writeTransactionError(w, "internal_error", "internal error", http.StatusInternalServerError)
		return
	}

	if trace.Enabled() {
		trace.AddStep("blockid", "fetch", time.Since(blockIDStart), "")
	}

	trace.Log()

	logger.Printf("timing", "transaction_status trx=%s block=%d duration=%v",
		trxID[:16], blockNum, time.Since(startTime))

	result := map[string]any{
		"state":               "IRREVERSIBLE",
		"block_number":        blockNum,
		"block_id":            blockID,
		"head_number":         lib,
		"irreversible_number": lib,
	}

	s.writeJSON(w, http.StatusOK, TransactionResponse{Results: result})
}

func (s *RPCServer) writeTransactionError(w http.ResponseWriter, code, message string, status int) {
	server.WriteJSON(w, status, TransactionErrorResponse{
		Error: TransactionError{
			Code:    code,
			Message: message,
		},
	})
}
