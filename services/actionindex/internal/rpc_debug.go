package internal

import (
	"net/http"
	"strconv"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
)

type DebugAccountInfo struct {
	Account          string   `json:"account"`
	AccountID        uint64   `json:"account_id"`
	AllActionsChunks int      `json:"all_actions_chunks"`
	ChunkBases       []uint64 `json:"chunk_bases,omitempty"`
	FirstBase        uint64   `json:"first_base"`
	LastBase         uint64   `json:"last_base"`
}

type DebugTimeMapInfo struct {
	TotalEntries int    `json:"total_entries"`
	MinHour      uint32 `json:"min_hour"`
	MaxHour      uint32 `json:"max_hour"`
	MinHourDate  string `json:"min_hour_date"`
	MaxHourDate  string `json:"max_hour_date"`
}

type DebugSeqLookup struct {
	Seq      uint64 `json:"seq"`
	Hour     uint32 `json:"hour"`
	HourDate string `json:"hour_date"`
	InRange  bool   `json:"in_range"`
	RangeMin uint64 `json:"range_min"`
	RangeMax uint64 `json:"range_max"`
}

func HandleDebugAccount(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	if account == "" {
		http.Error(w, "account parameter required", http.StatusBadRequest)
		return
	}

	showBases := r.URL.Query().Get("bases") == "true"
	accountID := chain.StringToName(account)

	chunkCount := indexes.metadata.GetAllActionsChunkCount(accountID)
	firstBase, lastBase := indexes.metadata.GetAllActionsSeqRange(accountID)

	info := DebugAccountInfo{
		Account:          account,
		AccountID:        accountID,
		AllActionsChunks: chunkCount,
		FirstBase:        firstBase,
		LastBase:         lastBase,
	}

	if showBases {
		indexes.metadata.mu.RLock()
		bases := indexes.metadata.allActions[accountID]
		info.ChunkBases = make([]uint64, len(bases))
		copy(info.ChunkBases, bases)
		indexes.metadata.mu.RUnlock()
	}

	writeJSON(w, info)
}

func HandleDebugTimeMap(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	minHour, maxHour := indexes.timeMapper.HourRange()

	info := DebugTimeMapInfo{
		TotalEntries: indexes.timeMapper.Len(),
		MinHour:      minHour,
		MaxHour:      maxHour,
		MinHourDate:  hourToDateString(minHour),
		MaxHourDate:  hourToDateString(maxHour),
	}

	writeJSON(w, info)
}

func HandleDebugSeqLookup(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	seqStr := r.URL.Query().Get("seq")
	if seqStr == "" {
		http.Error(w, "seq parameter required", http.StatusBadRequest)
		return
	}

	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid seq parameter", http.StatusBadRequest)
		return
	}

	hour := r.URL.Query().Get("hour")
	if hour == "" {
		http.Error(w, "hour parameter required (use /debug/timemap to find hour range)", http.StatusBadRequest)
		return
	}

	hourNum, err := strconv.ParseUint(hour, 10, 32)
	if err != nil {
		http.Error(w, "invalid hour parameter", http.StatusBadRequest)
		return
	}

	minSeq, maxSeq := indexes.timeMapper.GetSeqRangeForHours(uint32(hourNum), uint32(hourNum))

	info := DebugSeqLookup{
		Seq:      seq,
		Hour:     uint32(hourNum),
		HourDate: hourToDateString(uint32(hourNum)),
		InRange:  seq >= minSeq && seq <= maxSeq,
		RangeMin: minSeq,
		RangeMax: maxSeq,
	}

	writeJSON(w, info)
}

func HandleDebugChunkNear(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	seqStr := r.URL.Query().Get("seq")

	if account == "" || seqStr == "" {
		http.Error(w, "account and seq parameters required", http.StatusBadRequest)
		return
	}

	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid seq parameter", http.StatusBadRequest)
		return
	}

	accountID := chain.StringToName(account)
	prevBase, nextBase, chunkID := indexes.metadata.GetChunkBaseSeqsNear(accountID, seq)

	result := map[string]interface{}{
		"account":    account,
		"account_id": accountID,
		"target_seq": seq,
		"chunk_id":   chunkID,
		"prev_base":  prevBase,
		"next_base":  nextBase,
		"gap_size":   nextBase - prevBase,
	}

	writeJSON(w, result)
}

func HandleDebugSeqToDate(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	seqStr := r.URL.Query().Get("seq")
	if seqStr == "" {
		http.Error(w, "seq parameter required", http.StatusBadRequest)
		return
	}

	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid seq parameter", http.StatusBadRequest)
		return
	}

	minHour, maxHour := indexes.timeMapper.HourRange()
	var foundHour uint32
	var foundInHour bool

	indexes.timeMapper.mu.RLock()
	for hour := minHour; hour <= maxHour; hour++ {
		r, ok := indexes.timeMapper.hourlyRanges[hour]
		if ok && seq >= r.MinSeq && seq <= r.MaxSeq {
			foundHour = hour
			foundInHour = true
			break
		}
	}
	indexes.timeMapper.mu.RUnlock()

	result := map[string]interface{}{
		"seq":           seq,
		"found_in_hour": foundInHour,
	}
	if foundInHour {
		result["hour"] = foundHour
		result["date"] = hourToDateString(foundHour)
	}

	writeJSON(w, result)
}

func HandleDebugCompareIndexes(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	_ = r.URL.Query().Get("contract")
	_ = r.URL.Query().Get("action")

	if account == "" {
		http.Error(w, "account parameter required", http.StatusBadRequest)
		return
	}

	accountID := chain.StringToName(account)

	allActionsCount := indexes.metadata.GetAllActionsChunkCount(accountID)
	allActionsMin, allActionsMax := indexes.metadata.GetAllActionsSeqRange(accountID)

	result := map[string]interface{}{
		"account":    account,
		"account_id": accountID,
		"all_actions": map[string]interface{}{
			"chunk_count": allActionsCount,
			"min_base":    allActionsMin,
			"max_base":    allActionsMax,
		},
	}

	writeJSON(w, result)
}

func HandleDebugReadChunk(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	chunkStr := r.URL.Query().Get("chunk")

	if account == "" || chunkStr == "" {
		http.Error(w, "account and chunk parameters required", http.StatusBadRequest)
		return
	}

	chunkID, err := strconv.ParseUint(chunkStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid chunk parameter", http.StatusBadRequest)
		return
	}

	accountID := chain.StringToName(account)
	key := makeLegacyAccountActionsKey(accountID, uint32(chunkID))

	val, closer, err := indexes.db.Get(key)
	if err != nil {
		result := map[string]interface{}{
			"account":  account,
			"chunk_id": chunkID,
			"error":    err.Error(),
		}
		writeJSON(w, result)
		return
	}
	defer closer.Close()

	chunk, err := DecodeChunk(val)
	if err != nil {
		result := map[string]interface{}{
			"account":  account,
			"chunk_id": chunkID,
			"error":    "decode error: " + err.Error(),
		}
		writeJSON(w, result)
		return
	}

	seqCount := len(chunk.Seqs)
	var firstSeqs, lastSeqs []uint64
	if seqCount <= 20 {
		firstSeqs = chunk.Seqs
	} else {
		firstSeqs = chunk.Seqs[:10]
		lastSeqs = chunk.Seqs[seqCount-10:]
	}

	result := map[string]interface{}{
		"account":   account,
		"chunk_id":  chunkID,
		"base_seq":  chunk.BaseSeq,
		"seq_count": seqCount,
	}
	if seqCount > 0 {
		result["first_seq"] = chunk.Seqs[0]
		result["last_seq"] = chunk.Seqs[seqCount-1]
		result["first_seqs"] = firstSeqs
		if lastSeqs != nil {
			result["last_seqs"] = lastSeqs
		}
	}

	writeJSON(w, result)
}

func HandleDebugWALStatus(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	count, minSeq, maxSeq := indexes.walIndex.Stats()

	result := map[string]interface{}{
		"wal_entries": count,
		"min_seq":     minSeq,
		"max_seq":     maxSeq,
		"bulk_mode":   indexes.IsBulkMode(),
	}

	if minSeq > 0 {
		result["min_date"] = seqToApproxDate(indexes, minSeq)
		result["max_date"] = seqToApproxDate(indexes, maxSeq)
	}

	writeJSON(w, result)
}

func HandleDebugScanContractAction(indexes *Indexes, w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	contract := r.URL.Query().Get("contract")
	action := r.URL.Query().Get("action")

	if account == "" || contract == "" || action == "" {
		http.Error(w, "account, contract, and action parameters required", http.StatusBadRequest)
		return
	}

	accountID := chain.StringToName(account)
	contractID := chain.StringToName(contract)
	actionID := chain.StringToName(action)

	prefix := makeContractActionPrefix(accountID, contractID, actionID)
	upperBound := incrementPrefix(prefix)

	iter, err := indexes.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	defer iter.Close()

	var chunkCount int
	var minBase, maxBase uint64
	var lastChunkSeqs []uint64

	for iter.First(); iter.Valid(); iter.Next() {
		chunk, err := DecodeChunk(iter.Value())
		if err != nil {
			continue
		}
		chunkCount++
		if minBase == 0 || chunk.BaseSeq < minBase {
			minBase = chunk.BaseSeq
		}
		if chunk.BaseSeq > maxBase {
			maxBase = chunk.BaseSeq
			if len(chunk.Seqs) <= 10 {
				lastChunkSeqs = chunk.Seqs
			} else {
				lastChunkSeqs = chunk.Seqs[len(chunk.Seqs)-10:]
			}
		}
	}

	result := map[string]interface{}{
		"account":         account,
		"contract":        contract,
		"action":          action,
		"db_chunk_count":  chunkCount,
		"db_min_base":     minBase,
		"db_max_base":     maxBase,
		"last_chunk_seqs": lastChunkSeqs,
	}

	if maxBase > 0 {
		result["max_base_date"] = seqToApproxDate(indexes, maxBase)
	}

	writeJSON(w, result)
}

func HandleDebugSliceStats(reader *corereader.SliceReader, w http.ResponseWriter, r *http.Request) {
	sliceStr := r.URL.Query().Get("slice")
	if sliceStr == "" {
		sliceInfos := reader.GetSliceInfos()
		result := map[string]interface{}{
			"total_slices": len(sliceInfos),
			"slices":       sliceInfos,
			"usage":        "Use ?slice=N for detailed block stats, or ?slice=N&compare=M to compare two slices",
		}
		writeJSON(w, result)
		return
	}

	sliceNum, err := strconv.ParseUint(sliceStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid slice parameter", http.StatusBadRequest)
		return
	}

	compareStr := r.URL.Query().Get("compare")
	if compareStr != "" {
		compareNum, err := strconv.ParseUint(compareStr, 10, 32)
		if err != nil {
			http.Error(w, "invalid compare parameter", http.StatusBadRequest)
			return
		}

		stats1, err1 := reader.GetSliceBlockStats(uint32(sliceNum))
		stats2, err2 := reader.GetSliceBlockStats(uint32(compareNum))

		result := map[string]interface{}{}
		if err1 != nil {
			result["slice1_error"] = err1.Error()
		} else {
			result["slice1"] = stats1
		}
		if err2 != nil {
			result["slice2_error"] = err2.Error()
		} else {
			result["slice2"] = stats2
		}

		if stats1 != nil && stats2 != nil {
			result["comparison"] = map[string]interface{}{
				"size_ratio":    float64(stats1.TotalSizeBytes) / float64(stats2.TotalSizeBytes),
				"actions_ratio": float64(stats1.TotalActions) / float64(stats2.TotalActions),
				"density_ratio": stats1.AvgActionsBlock / stats2.AvgActionsBlock,
			}
		}
		writeJSON(w, result)
		return
	}

	stats, err := reader.GetSliceBlockStats(uint32(sliceNum))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	writeJSON(w, stats)
}

func seqToApproxDate(indexes *Indexes, seq uint64) string {
	minHour, maxHour := indexes.timeMapper.HourRange()
	indexes.timeMapper.mu.RLock()
	defer indexes.timeMapper.mu.RUnlock()
	for hour := minHour; hour <= maxHour; hour++ {
		r, ok := indexes.timeMapper.hourlyRanges[hour]
		if ok && seq >= r.MinSeq && seq <= r.MaxSeq {
			return hourToDateString(hour)
		}
	}
	return "unknown"
}

func hourToDateString(hour uint32) string {
	unixSeconds := int64(hour) * 3600
	return formatUnixTime(unixSeconds)
}

func formatUnixTime(unixSeconds int64) string {
	t := unixTimeToTime(unixSeconds)
	return t.Format("2006-01-02T15:04:05Z")
}

func unixTimeToTime(unixSeconds int64) interface{ Format(string) string } {
	return timeFromUnix(unixSeconds)
}

type simpleTime struct {
	unix int64
}

func (t simpleTime) Format(layout string) string {
	return formatTimeManual(t.unix)
}

func timeFromUnix(unix int64) simpleTime {
	return simpleTime{unix: unix}
}

func formatTimeManual(unixSeconds int64) string {
	const (
		secondsPerMinute = 60
		secondsPerHour   = 3600
		secondsPerDay    = 86400
	)

	days := unixSeconds / secondsPerDay
	remaining := unixSeconds % secondsPerDay
	hours := remaining / secondsPerHour
	remaining = remaining % secondsPerHour
	minutes := remaining / secondsPerMinute
	seconds := remaining % secondsPerMinute

	year, month, day := daysToDate(int(days) + 719468)

	buf := make([]byte, 0, 20)
	buf = appendInt(buf, year, 4)
	buf = append(buf, '-')
	buf = appendInt(buf, int(month), 2)
	buf = append(buf, '-')
	buf = appendInt(buf, day, 2)
	buf = append(buf, 'T')
	buf = appendInt(buf, int(hours), 2)
	buf = append(buf, ':')
	buf = appendInt(buf, int(minutes), 2)
	buf = append(buf, ':')
	buf = appendInt(buf, int(seconds), 2)
	buf = append(buf, 'Z')

	return string(buf)
}

func daysToDate(days int) (year int, month int, day int) {
	era := days / 146097
	if days < 0 {
		era--
	}
	doe := days - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	year = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	day = doy - (153*mp+2)/5 + 1
	if mp < 10 {
		month = mp + 3
	} else {
		month = mp - 9
	}
	if month <= 2 {
		year++
	}
	return year, month, day
}

func appendInt(buf []byte, n int, width int) []byte {
	if n < 0 {
		buf = append(buf, '-')
		n = -n
	}
	temp := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		temp[i] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, temp...)
}

func HandleDebugBlock(reader corereader.Reader, w http.ResponseWriter, r *http.Request) {
	blockStr := r.URL.Query().Get("block")
	if blockStr == "" {
		http.Error(w, "block parameter required", http.StatusBadRequest)
		return
	}

	blockNum, err := strconv.ParseUint(blockStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid block parameter", http.StatusBadRequest)
		return
	}

	blocks, err := reader.GetRawBlockBatch(uint32(blockNum), uint32(blockNum))
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"block_num": blockNum,
			"error":     err.Error(),
		})
		return
	}

	if len(blocks) == 0 {
		writeJSON(w, map[string]interface{}{
			"block_num": blockNum,
			"error":     "block not found",
		})
		return
	}

	block := blocks[0]

	var minSeq, maxSeq uint64
	for _, act := range block.Actions {
		if minSeq == 0 || act.GlobalSeqUint64 < minSeq {
			minSeq = act.GlobalSeqUint64
		}
		if act.GlobalSeqUint64 > maxSeq {
			maxSeq = act.GlobalSeqUint64
		}
	}

	notifiedAccounts := make([]string, 0, len(block.Notifications))
	for accountID := range block.Notifications {
		notifiedAccounts = append(notifiedAccounts, chain.NameToString(accountID))
	}

	result := map[string]interface{}{
		"block_num":          block.BlockNum,
		"block_time":         block.BlockTime,
		"action_count":       len(block.Actions),
		"notification_count": len(block.Notifications),
		"notified_accounts":  notifiedAccounts,
		"min_seq":            minSeq,
		"max_seq":            maxSeq,
	}

	if len(block.Actions) <= 50 {
		actionSummaries := make([]map[string]interface{}, 0, len(block.Actions))
		for _, act := range block.Actions {
			actionSummaries = append(actionSummaries, map[string]interface{}{
				"global_seq": act.GlobalSeqUint64,
				"contract":   chain.NameToString(act.ContractUint64),
				"action":     chain.NameToString(act.ActionUint64),
				"receiver":   chain.NameToString(act.ReceiverUint64),
				"ordinal":    act.ActionOrdinal,
			})
		}
		result["actions"] = actionSummaries
	} else {
		first5 := make([]map[string]interface{}, 0, 5)
		last5 := make([]map[string]interface{}, 0, 5)
		for i := 0; i < 5 && i < len(block.Actions); i++ {
			act := block.Actions[i]
			first5 = append(first5, map[string]interface{}{
				"global_seq": act.GlobalSeqUint64,
				"contract":   chain.NameToString(act.ContractUint64),
				"action":     chain.NameToString(act.ActionUint64),
				"receiver":   chain.NameToString(act.ReceiverUint64),
			})
		}
		for i := len(block.Actions) - 5; i < len(block.Actions); i++ {
			if i < 0 {
				continue
			}
			act := block.Actions[i]
			last5 = append(last5, map[string]interface{}{
				"global_seq": act.GlobalSeqUint64,
				"contract":   chain.NameToString(act.ContractUint64),
				"action":     chain.NameToString(act.ActionUint64),
				"receiver":   chain.NameToString(act.ReceiverUint64),
			})
		}
		result["first_actions"] = first5
		result["last_actions"] = last5
	}

	writeJSON(w, result)
}

func HandleDebugAction(reader corereader.Reader, w http.ResponseWriter, r *http.Request) {
	seqStr := r.URL.Query().Get("seq")
	if seqStr == "" {
		http.Error(w, "seq parameter required", http.StatusBadRequest)
		return
	}

	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid seq parameter", http.StatusBadRequest)
		return
	}

	actions, timings, err := reader.GetActionsByGlobalSeqs([]uint64{seq})
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"seq":   seq,
			"error": err.Error(),
		})
		return
	}

	if len(actions) == 0 {
		writeJSON(w, map[string]interface{}{
			"seq":   seq,
			"error": "action not found",
		})
		return
	}

	at := actions[0]
	result := map[string]interface{}{
		"seq":        seq,
		"block_num":  at.BlockNum,
		"block_time": at.BlockTime,
		"contract":   at.Act.Account,
		"action":     at.Act.Name,
		"receiver":   at.Receiver,
		"trx_id":     at.TrxID,
		"ordinal":    at.ActionOrdinal,
		"auth":       at.Act.Authorization,
		"data_hex":   at.Act.Data,
	}

	if timings != nil {
		result["timing_ms"] = float64(timings.Total.Microseconds()) / 1000.0
	}

	writeJSON(w, result)
}

func HandleDebugSearchAccountInBlock(reader corereader.Reader, w http.ResponseWriter, r *http.Request) {
	blockStr := r.URL.Query().Get("block")
	account := r.URL.Query().Get("account")

	if blockStr == "" || account == "" {
		http.Error(w, "block and account parameters required", http.StatusBadRequest)
		return
	}

	blockNum, err := strconv.ParseUint(blockStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid block parameter", http.StatusBadRequest)
		return
	}

	accountID := chain.StringToName(account)

	blocks, err := reader.GetRawBlockBatch(uint32(blockNum), uint32(blockNum))
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"block_num": blockNum,
			"account":   account,
			"error":     err.Error(),
		})
		return
	}

	if len(blocks) == 0 {
		writeJSON(w, map[string]interface{}{
			"block_num": blockNum,
			"account":   account,
			"error":     "block not found",
		})
		return
	}

	block := blocks[0]

	notifSeqs, hasNotifs := block.Notifications[accountID]

	matchingActions := make([]map[string]interface{}, 0)
	for _, act := range block.Actions {
		isReceiver := act.ReceiverUint64 == accountID
		isContract := act.ContractUint64 == accountID

		var isInAuth bool
		for _, authIdx := range act.AuthAccountIndexes {
			if authIdx < uint32(len(block.NamesInBlock)) && block.NamesInBlock[authIdx] == accountID {
				isInAuth = true
				break
			}
		}

		if isReceiver || isContract || isInAuth {
			matchingActions = append(matchingActions, map[string]interface{}{
				"global_seq":  act.GlobalSeqUint64,
				"contract":    chain.NameToString(act.ContractUint64),
				"action":      chain.NameToString(act.ActionUint64),
				"receiver":    chain.NameToString(act.ReceiverUint64),
				"ordinal":     act.ActionOrdinal,
				"is_receiver": isReceiver,
				"is_contract": isContract,
				"is_in_auth":  isInAuth,
			})
		}
	}

	result := map[string]interface{}{
		"block_num":        blockNum,
		"account":          account,
		"account_id":       accountID,
		"has_notification": hasNotifs,
		"matching_actions": matchingActions,
		"total_in_block":   len(block.Actions),
	}

	if hasNotifs {
		result["notification_seqs"] = notifSeqs
	}

	writeJSON(w, result)
}

func HandleDebugFilterBlock(reader corereader.Reader, w http.ResponseWriter, r *http.Request) {
	blockStr := r.URL.Query().Get("block")
	account := r.URL.Query().Get("account")

	if blockStr == "" {
		http.Error(w, "block parameter required (account optional)", http.StatusBadRequest)
		return
	}

	blockNum, err := strconv.ParseUint(blockStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid block parameter", http.StatusBadRequest)
		return
	}

	var accountID uint64
	if account != "" {
		accountID = chain.StringToName(account)
	}

	blocks, err := reader.GetRawBlockBatch(uint32(blockNum), uint32(blockNum))
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"block_num": blockNum,
			"error":     err.Error(),
		})
		return
	}

	if len(blocks) == 0 {
		writeJSON(w, map[string]interface{}{
			"block_num": blockNum,
			"error":     "block not found",
		})
		return
	}

	rawBlock := blocks[0]

	filtered := corereader.FilterRawBlock(rawBlock, nil)

	result := map[string]interface{}{
		"block_num":           blockNum,
		"block_time":          filtered.BlockTime,
		"raw_action_count":    len(rawBlock.Actions),
		"raw_notif_accounts":  len(rawBlock.Notifications),
		"filtered_min_seq":    filtered.MinSeq,
		"filtered_max_seq":    filtered.MaxSeq,
		"filtered_executions": len(filtered.Executions),
	}

	if account == "" {
		accountActions := make(map[string]int)
		for _, act := range filtered.Actions {
			name := chain.NameToString(act.Account)
			accountActions[name]++
		}
		result["filtered_total_actions"] = len(filtered.Actions)
		result["filtered_actions_by_account"] = accountActions
	} else {
		var accountFiltered []map[string]interface{}
		for _, act := range filtered.Actions {
			if act.Account == accountID {
				accountFiltered = append(accountFiltered, map[string]interface{}{
					"global_seq": act.GlobalSeq,
					"contract":   chain.NameToString(act.Contract),
					"action":     chain.NameToString(act.Action),
					"trx_index":  act.TrxIndex,
				})
			}
		}
		result["account"] = account
		result["filtered_for_account"] = len(accountFiltered)
		result["filtered_actions"] = accountFiltered

		notifSeqs, hasNotif := rawBlock.Notifications[accountID]
		result["raw_has_notification"] = hasNotif
		if hasNotif {
			result["raw_notification_seqs"] = notifSeqs
		}

		var rawMatching []map[string]interface{}
		for _, act := range rawBlock.Actions {
			isReceiver := act.ReceiverUint64 == accountID
			isContract := act.ContractUint64 == accountID
			var isInAuth bool
			for _, authIdx := range act.AuthAccountIndexes {
				if authIdx < uint32(len(rawBlock.NamesInBlock)) && rawBlock.NamesInBlock[authIdx] == accountID {
					isInAuth = true
					break
				}
			}
			if isReceiver || isContract || isInAuth {
				rawMatching = append(rawMatching, map[string]interface{}{
					"global_seq":  act.GlobalSeqUint64,
					"contract":    chain.NameToString(act.ContractUint64),
					"action":      chain.NameToString(act.ActionUint64),
					"receiver":    chain.NameToString(act.ReceiverUint64),
					"is_receiver": isReceiver,
					"is_contract": isContract,
					"is_in_auth":  isInAuth,
				})
			}
		}
		result["raw_matching_for_account"] = len(rawMatching)
		result["raw_matching_actions"] = rawMatching

		if len(rawMatching) != len(accountFiltered) {
			var missingSeqs []uint64
			filteredSeqs := make(map[uint64]bool)
			for _, act := range accountFiltered {
				filteredSeqs[act["global_seq"].(uint64)] = true
			}
			for _, act := range rawMatching {
				seq := act["global_seq"].(uint64)
				if !filteredSeqs[seq] {
					missingSeqs = append(missingSeqs, seq)
				}
			}
			result["missing_seqs"] = missingSeqs
			result["filter_discrepancy"] = len(rawMatching) - len(accountFiltered)
		}
	}

	writeJSON(w, result)
}
