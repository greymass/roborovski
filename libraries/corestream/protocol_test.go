package corestream

import (
	"bytes"
	"reflect"
	"testing"
)

func TestQueryStateMessage(t *testing.T) {
	msg := &QueryStateMessage{RequestID: 12345}
	encoded := EncodeQueryStateMessage(msg)
	decoded, err := DecodeQueryStateMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
}

func TestQueryStateResponseMessage(t *testing.T) {
	msg := &QueryStateResponseMessage{
		RequestID: 12345,
		HEAD:      100000,
		LIB:       99000,
	}
	encoded := EncodeQueryStateResponseMessage(msg)
	decoded, err := DecodeQueryStateResponseMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.HEAD != msg.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, msg.HEAD)
	}
	if decoded.LIB != msg.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, msg.LIB)
	}
}

func TestQueryBlockMessage(t *testing.T) {
	msg := &QueryBlockMessage{
		RequestID: 54321,
		BlockNum:  1000000,
	}
	encoded := EncodeQueryBlockMessage(msg)
	decoded, err := DecodeQueryBlockMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.BlockNum != msg.BlockNum {
		t.Errorf("BlockNum mismatch: got %d, want %d", decoded.BlockNum, msg.BlockNum)
	}
}

func TestQueryBlockResponseMessage(t *testing.T) {
	msg := &QueryBlockResponseMessage{
		RequestID: 54321,
		BlockNum:  1000000,
		Data:      []byte("test block data"),
	}
	for i := 0; i < 32; i++ {
		msg.BlockID[i] = byte(i)
		msg.Previous[i] = byte(31 - i)
	}

	encoded := EncodeQueryBlockResponseMessage(msg)
	decoded, err := DecodeQueryBlockResponseMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.BlockNum != msg.BlockNum {
		t.Errorf("BlockNum mismatch: got %d, want %d", decoded.BlockNum, msg.BlockNum)
	}
	if decoded.BlockID != msg.BlockID {
		t.Errorf("BlockID mismatch")
	}
	if decoded.Previous != msg.Previous {
		t.Errorf("Previous mismatch")
	}
	if !bytes.Equal(decoded.Data, msg.Data) {
		t.Errorf("Data mismatch: got %v, want %v", decoded.Data, msg.Data)
	}
}

func TestQueryBlockBatchMessage(t *testing.T) {
	msg := &QueryBlockBatchMessage{
		RequestID:  99999,
		StartBlock: 1000,
		EndBlock:   2000,
	}
	encoded := EncodeQueryBlockBatchMessage(msg)
	decoded, err := DecodeQueryBlockBatchMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.StartBlock != msg.StartBlock {
		t.Errorf("StartBlock mismatch: got %d, want %d", decoded.StartBlock, msg.StartBlock)
	}
	if decoded.EndBlock != msg.EndBlock {
		t.Errorf("EndBlock mismatch: got %d, want %d", decoded.EndBlock, msg.EndBlock)
	}
}

func TestQueryBlockBatchStartMessage(t *testing.T) {
	msg := &QueryBlockBatchStartMessage{
		RequestID:   99999,
		TotalBlocks: 1001,
	}
	encoded := EncodeQueryBlockBatchStartMessage(msg)
	decoded, err := DecodeQueryBlockBatchStartMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.TotalBlocks != msg.TotalBlocks {
		t.Errorf("TotalBlocks mismatch: got %d, want %d", decoded.TotalBlocks, msg.TotalBlocks)
	}
}

func TestQueryBlockBatchBlockMessage(t *testing.T) {
	msg := &QueryBlockBatchBlockMessage{
		RequestID: 99999,
		BlockNum:  1500,
		Data:      []byte("block 1500 data"),
	}
	encoded := EncodeQueryBlockBatchBlockMessage(msg)
	decoded, err := DecodeQueryBlockBatchBlockMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.BlockNum != msg.BlockNum {
		t.Errorf("BlockNum mismatch: got %d, want %d", decoded.BlockNum, msg.BlockNum)
	}
	if !bytes.Equal(decoded.Data, msg.Data) {
		t.Errorf("Data mismatch: got %v, want %v", decoded.Data, msg.Data)
	}
}

func TestQueryBlockBatchCompleteMessage(t *testing.T) {
	msg := &QueryBlockBatchCompleteMessage{RequestID: 99999}
	encoded := EncodeQueryBlockBatchCompleteMessage(msg)
	decoded, err := DecodeQueryBlockBatchCompleteMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
}

func TestQueryGlobsMessage(t *testing.T) {
	msg := &QueryGlobsMessage{
		RequestID:  77777,
		GlobalSeqs: []uint64{100, 200, 300, 400, 500},
	}
	encoded := EncodeQueryGlobsMessage(msg)
	decoded, err := DecodeQueryGlobsMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if !reflect.DeepEqual(decoded.GlobalSeqs, msg.GlobalSeqs) {
		t.Errorf("GlobalSeqs mismatch: got %v, want %v", decoded.GlobalSeqs, msg.GlobalSeqs)
	}
}

func TestQueryGlobsMessageEmpty(t *testing.T) {
	msg := &QueryGlobsMessage{
		RequestID:  77777,
		GlobalSeqs: []uint64{},
	}
	encoded := EncodeQueryGlobsMessage(msg)
	decoded, err := DecodeQueryGlobsMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if len(decoded.GlobalSeqs) != 0 {
		t.Errorf("GlobalSeqs should be empty, got %v", decoded.GlobalSeqs)
	}
}

func TestQueryGlobsResponseMessage(t *testing.T) {
	msg := &QueryGlobsResponseMessage{
		RequestID: 77777,
		LIB:       99000,
		HEAD:      100000,
		Actions: []ActionData{
			{GlobalSeq: 100, BlockNum: 50000, BlockTime: 1700000000, Data: []byte("action1")},
			{GlobalSeq: 200, BlockNum: 50001, BlockTime: 1700000001, Data: []byte("action2")},
		},
	}
	encoded := EncodeQueryGlobsResponseMessage(msg)
	decoded, err := DecodeQueryGlobsResponseMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.LIB != msg.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, msg.LIB)
	}
	if decoded.HEAD != msg.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, msg.HEAD)
	}
	if len(decoded.Actions) != len(msg.Actions) {
		t.Fatalf("Actions length mismatch: got %d, want %d", len(decoded.Actions), len(msg.Actions))
	}
	for i, action := range decoded.Actions {
		if action.GlobalSeq != msg.Actions[i].GlobalSeq {
			t.Errorf("Action[%d] GlobalSeq mismatch: got %d, want %d", i, action.GlobalSeq, msg.Actions[i].GlobalSeq)
		}
		if action.BlockNum != msg.Actions[i].BlockNum {
			t.Errorf("Action[%d] BlockNum mismatch: got %d, want %d", i, action.BlockNum, msg.Actions[i].BlockNum)
		}
		if action.BlockTime != msg.Actions[i].BlockTime {
			t.Errorf("Action[%d] BlockTime mismatch: got %d, want %d", i, action.BlockTime, msg.Actions[i].BlockTime)
		}
		if !bytes.Equal(action.Data, msg.Actions[i].Data) {
			t.Errorf("Action[%d] Data mismatch", i)
		}
	}
}

func TestQueryGlobsResponseMessageEmpty(t *testing.T) {
	msg := &QueryGlobsResponseMessage{
		RequestID: 77777,
		LIB:       99000,
		HEAD:      100000,
		Actions:   []ActionData{},
	}
	encoded := EncodeQueryGlobsResponseMessage(msg)
	decoded, err := DecodeQueryGlobsResponseMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(decoded.Actions) != 0 {
		t.Errorf("Actions should be empty, got %v", decoded.Actions)
	}
}

func TestQueryErrorMessage(t *testing.T) {
	msg := &QueryErrorMessage{
		RequestID: 88888,
		Code:      ErrorCodeBlockNotFound,
		Message:   "block 12345 not found",
	}
	encoded := EncodeQueryErrorMessage(msg)
	decoded, err := DecodeQueryErrorMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.RequestID != msg.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", decoded.RequestID, msg.RequestID)
	}
	if decoded.Code != msg.Code {
		t.Errorf("Code mismatch: got %d, want %d", decoded.Code, msg.Code)
	}
	if decoded.Message != msg.Message {
		t.Errorf("Message mismatch: got %q, want %q", decoded.Message, msg.Message)
	}
}

func TestDecodeErrorsOnShortPayload(t *testing.T) {
	tests := []struct {
		name   string
		decode func([]byte) error
	}{
		{"QueryStateMessage", func(b []byte) error { _, err := DecodeQueryStateMessage(b); return err }},
		{"QueryStateResponseMessage", func(b []byte) error { _, err := DecodeQueryStateResponseMessage(b); return err }},
		{"QueryBlockMessage", func(b []byte) error { _, err := DecodeQueryBlockMessage(b); return err }},
		{"QueryBlockResponseMessage", func(b []byte) error { _, err := DecodeQueryBlockResponseMessage(b); return err }},
		{"QueryBlockBatchMessage", func(b []byte) error { _, err := DecodeQueryBlockBatchMessage(b); return err }},
		{"QueryBlockBatchStartMessage", func(b []byte) error { _, err := DecodeQueryBlockBatchStartMessage(b); return err }},
		{"QueryBlockBatchBlockMessage", func(b []byte) error { _, err := DecodeQueryBlockBatchBlockMessage(b); return err }},
		{"QueryBlockBatchCompleteMessage", func(b []byte) error { _, err := DecodeQueryBlockBatchCompleteMessage(b); return err }},
		{"QueryGlobsMessage", func(b []byte) error { _, err := DecodeQueryGlobsMessage(b); return err }},
		{"QueryGlobsResponseMessage", func(b []byte) error { _, err := DecodeQueryGlobsResponseMessage(b); return err }},
		{"QueryErrorMessage", func(b []byte) error { _, err := DecodeQueryErrorMessage(b); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.decode([]byte{})
			if err == nil {
				t.Errorf("expected error on empty payload, got nil")
			}
			err = tt.decode([]byte{1, 2, 3})
			if err == nil {
				t.Errorf("expected error on short payload, got nil")
			}
		})
	}
}

func TestWriteAndReadMessage(t *testing.T) {
	testCases := []struct {
		name    string
		msgType uint8
		payload []byte
	}{
		{"empty payload", MsgTypeHeartbeat, nil},
		{"small payload", MsgTypeBlock, []byte("hello")},
		{"large payload", MsgTypeBlock, bytes.Repeat([]byte("x"), 10000)},
		{"all message types block", MsgTypeBlock, []byte{1, 2, 3}},
		{"all message types heartbeat", MsgTypeHeartbeat, []byte{4, 5, 6}},
		{"all message types ack", MsgTypeAck, []byte{7, 8, 9}},
		{"all message types error", MsgTypeError, []byte{10, 11, 12}},
		{"all message types request", MsgTypeRequest, []byte{13, 14, 15}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			err := WriteMessage(&buf, tc.msgType, tc.payload)
			if err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			gotType, gotPayload, err := ReadMessage(&buf)
			if err != nil {
				t.Fatalf("ReadMessage failed: %v", err)
			}

			if gotType != tc.msgType {
				t.Errorf("msgType mismatch: got %d, want %d", gotType, tc.msgType)
			}
			if !bytes.Equal(gotPayload, tc.payload) {
				t.Errorf("payload mismatch: got %v, want %v", gotPayload, tc.payload)
			}
		})
	}
}

func TestReadMessageTooLarge(t *testing.T) {
	var buf bytes.Buffer

	header := make([]byte, 5)
	tooLarge := uint32(MaxMessageSize + 1)
	header[0] = byte(tooLarge >> 24)
	header[1] = byte(tooLarge >> 16)
	header[2] = byte(tooLarge >> 8)
	header[3] = byte(tooLarge)
	header[4] = MsgTypeBlock
	buf.Write(header)

	_, _, err := ReadMessage(&buf)
	if err == nil {
		t.Error("expected error for message too large")
	}
	if err.Error() != "message too large" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestReadMessageTruncated(t *testing.T) {
	t.Run("truncated header", func(t *testing.T) {
		buf := bytes.NewReader([]byte{0, 0, 0})
		_, _, err := ReadMessage(buf)
		if err == nil {
			t.Error("expected error for truncated header")
		}
	})

	t.Run("truncated payload", func(t *testing.T) {
		var buf bytes.Buffer
		header := make([]byte, 5)
		header[0] = 0
		header[1] = 0
		header[2] = 0
		header[3] = 100
		header[4] = MsgTypeBlock
		buf.Write(header)
		buf.Write([]byte{1, 2, 3})

		_, _, err := ReadMessage(&buf)
		if err == nil {
			t.Error("expected error for truncated payload")
		}
	})
}

func TestBlockMessage(t *testing.T) {
	msg := &BlockMessage{
		BlockNum: 1000000,
		LIB:      999000,
		HEAD:     1000050,
		Data:     []byte("compressed block data"),
	}

	encoded := EncodeBlockMessage(msg)
	decoded, err := DecodeBlockMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.BlockNum != msg.BlockNum {
		t.Errorf("BlockNum mismatch: got %d, want %d", decoded.BlockNum, msg.BlockNum)
	}
	if decoded.LIB != msg.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, msg.LIB)
	}
	if decoded.HEAD != msg.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, msg.HEAD)
	}
	if !bytes.Equal(decoded.Data, msg.Data) {
		t.Errorf("Data mismatch: got %v, want %v", decoded.Data, msg.Data)
	}
}

func TestBlockMessageEmptyData(t *testing.T) {
	msg := &BlockMessage{
		BlockNum: 1,
		LIB:      0,
		HEAD:     1,
		Data:     nil,
	}

	encoded := EncodeBlockMessage(msg)
	decoded, err := DecodeBlockMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Data) != 0 {
		t.Errorf("expected empty data, got %v", decoded.Data)
	}
}

func TestBlockMessageShortPayload(t *testing.T) {
	_, err := DecodeBlockMessage([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestHeartbeatMessage(t *testing.T) {
	msg := &HeartbeatMessage{
		LIB:  100000,
		HEAD: 100500,
	}

	encoded := EncodeHeartbeatMessage(msg)
	decoded, err := DecodeHeartbeatMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.LIB != msg.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, msg.LIB)
	}
	if decoded.HEAD != msg.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, msg.HEAD)
	}
}

func TestHeartbeatMessageShortPayload(t *testing.T) {
	_, err := DecodeHeartbeatMessage([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestAckMessage(t *testing.T) {
	msg := &AckMessage{BlockNum: 12345}

	encoded := EncodeAckMessage(msg)
	decoded, err := DecodeAckMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.BlockNum != msg.BlockNum {
		t.Errorf("BlockNum mismatch: got %d, want %d", decoded.BlockNum, msg.BlockNum)
	}
}

func TestAckMessageShortPayload(t *testing.T) {
	_, err := DecodeAckMessage([]byte{1, 2})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestErrorMessage(t *testing.T) {
	msg := &ErrorMessage{
		Code:    ErrorCodeBlockNotFound,
		Message: "block 99999 not found in store",
	}

	encoded := EncodeErrorMessage(msg)
	decoded, err := DecodeErrorMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Code != msg.Code {
		t.Errorf("Code mismatch: got %d, want %d", decoded.Code, msg.Code)
	}
	if decoded.Message != msg.Message {
		t.Errorf("Message mismatch: got %q, want %q", decoded.Message, msg.Message)
	}
}

func TestErrorMessageEmptyMessage(t *testing.T) {
	msg := &ErrorMessage{
		Code:    ErrorCodeServerError,
		Message: "",
	}

	encoded := EncodeErrorMessage(msg)
	decoded, err := DecodeErrorMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Message != "" {
		t.Errorf("expected empty message, got %q", decoded.Message)
	}
}

func TestErrorMessageShortPayload(t *testing.T) {
	_, err := DecodeErrorMessage([]byte{1})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestRequestMessage(t *testing.T) {
	msg := &RequestMessage{StartBlock: 500000}

	encoded := EncodeRequestMessage(msg)
	decoded, err := DecodeRequestMessage(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.StartBlock != msg.StartBlock {
		t.Errorf("StartBlock mismatch: got %d, want %d", decoded.StartBlock, msg.StartBlock)
	}
}

func TestRequestMessageShortPayload(t *testing.T) {
	_, err := DecodeRequestMessage([]byte{1, 2})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestGetBlockResponse(t *testing.T) {
	resp := &GetBlockResponse{
		BlockNum: 1000000,
		LIB:      999000,
		HEAD:     1000050,
		Data:     []byte("raw block content"),
	}
	for i := 0; i < 32; i++ {
		resp.BlockID[i] = byte(i)
		resp.Previous[i] = byte(255 - i)
	}

	encoded := EncodeGetBlockResponse(resp)
	decoded, err := DecodeGetBlockResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.BlockNum != resp.BlockNum {
		t.Errorf("BlockNum mismatch: got %d, want %d", decoded.BlockNum, resp.BlockNum)
	}
	if decoded.LIB != resp.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, resp.LIB)
	}
	if decoded.HEAD != resp.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, resp.HEAD)
	}
	if decoded.BlockID != resp.BlockID {
		t.Error("BlockID mismatch")
	}
	if decoded.Previous != resp.Previous {
		t.Error("Previous mismatch")
	}
	if !bytes.Equal(decoded.Data, resp.Data) {
		t.Errorf("Data mismatch")
	}
}

func TestGetBlockResponseEmptyData(t *testing.T) {
	resp := &GetBlockResponse{
		BlockNum: 1,
		LIB:      0,
		HEAD:     1,
		Data:     nil,
	}

	encoded := EncodeGetBlockResponse(resp)
	decoded, err := DecodeGetBlockResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Data) != 0 {
		t.Errorf("expected empty data")
	}
}

func TestGetBlockResponseShortPayload(t *testing.T) {
	_, err := DecodeGetBlockResponse(make([]byte, 75))
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestGetStateResponse(t *testing.T) {
	resp := &GetStateResponse{
		HEAD: 1000000,
		LIB:  999500,
	}

	encoded := EncodeGetStateResponse(resp)
	decoded, err := DecodeGetStateResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.HEAD != resp.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, resp.HEAD)
	}
	if decoded.LIB != resp.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, resp.LIB)
	}
}

func TestGetStateResponseShortPayload(t *testing.T) {
	_, err := DecodeGetStateResponse([]byte{1, 2, 3, 4, 5, 6, 7})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestGetActionsByGlobsResponse(t *testing.T) {
	resp := &GetActionsByGlobsResponse{
		LIB:  99000,
		HEAD: 100000,
		Actions: []ActionData{
			{GlobalSeq: 1001, BlockNum: 50000, BlockTime: 1700000000, Data: []byte("action one")},
			{GlobalSeq: 1002, BlockNum: 50001, BlockTime: 1700000001, Data: []byte("action two")},
			{GlobalSeq: 1003, BlockNum: 50002, BlockTime: 1700000002, Data: []byte("action three")},
		},
	}

	encoded := EncodeGetActionsByGlobsResponse(resp)
	decoded, err := DecodeGetActionsByGlobsResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.LIB != resp.LIB {
		t.Errorf("LIB mismatch: got %d, want %d", decoded.LIB, resp.LIB)
	}
	if decoded.HEAD != resp.HEAD {
		t.Errorf("HEAD mismatch: got %d, want %d", decoded.HEAD, resp.HEAD)
	}
	if len(decoded.Actions) != len(resp.Actions) {
		t.Fatalf("Actions length mismatch: got %d, want %d", len(decoded.Actions), len(resp.Actions))
	}

	for i, act := range decoded.Actions {
		if act.GlobalSeq != resp.Actions[i].GlobalSeq {
			t.Errorf("Action[%d] GlobalSeq mismatch", i)
		}
		if act.BlockNum != resp.Actions[i].BlockNum {
			t.Errorf("Action[%d] BlockNum mismatch", i)
		}
		if act.BlockTime != resp.Actions[i].BlockTime {
			t.Errorf("Action[%d] BlockTime mismatch", i)
		}
		if !bytes.Equal(act.Data, resp.Actions[i].Data) {
			t.Errorf("Action[%d] Data mismatch", i)
		}
	}
}

func TestGetActionsByGlobsResponseEmpty(t *testing.T) {
	resp := &GetActionsByGlobsResponse{
		LIB:     1000,
		HEAD:    1500,
		Actions: []ActionData{},
	}

	encoded := EncodeGetActionsByGlobsResponse(resp)
	decoded, err := DecodeGetActionsByGlobsResponse(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Actions) != 0 {
		t.Errorf("expected empty actions")
	}
}

func TestGetActionsByGlobsResponseShortPayload(t *testing.T) {
	_, err := DecodeGetActionsByGlobsResponse([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short payload")
	}
}

func TestGetActionsByGlobsResponseTruncated(t *testing.T) {
	resp := &GetActionsByGlobsResponse{
		LIB:  1000,
		HEAD: 1500,
		Actions: []ActionData{
			{GlobalSeq: 100, BlockNum: 500, BlockTime: 1700000000, Data: []byte("test")},
		},
	}

	encoded := EncodeGetActionsByGlobsResponse(resp)

	_, err := DecodeGetActionsByGlobsResponse(encoded[:15])
	if err == nil {
		t.Error("expected error for truncated action header")
	}

	_, err = DecodeGetActionsByGlobsResponse(encoded[:len(encoded)-2])
	if err == nil {
		t.Error("expected error for truncated action data")
	}
}

func TestQueryGlobsMessageTruncated(t *testing.T) {
	msg := &QueryGlobsMessage{
		RequestID:  77777,
		GlobalSeqs: []uint64{100, 200, 300},
	}
	encoded := EncodeQueryGlobsMessage(msg)

	_, err := DecodeQueryGlobsMessage(encoded[:20])
	if err == nil {
		t.Error("expected error for truncated globs")
	}
}

func TestQueryGlobsResponseTruncated(t *testing.T) {
	msg := &QueryGlobsResponseMessage{
		RequestID: 77777,
		LIB:       99000,
		HEAD:      100000,
		Actions: []ActionData{
			{GlobalSeq: 100, BlockNum: 50000, BlockTime: 1700000000, Data: []byte("action data")},
		},
	}
	encoded := EncodeQueryGlobsResponseMessage(msg)

	_, err := DecodeQueryGlobsResponseMessage(encoded[:25])
	if err == nil {
		t.Error("expected error for truncated action header")
	}

	_, err = DecodeQueryGlobsResponseMessage(encoded[:len(encoded)-3])
	if err == nil {
		t.Error("expected error for truncated action data")
	}
}
