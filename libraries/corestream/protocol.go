package corestream

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	MsgTypeBlock     uint8 = 0x01
	MsgTypeHeartbeat uint8 = 0x02
	MsgTypeAck       uint8 = 0x03
	MsgTypeError     uint8 = 0x04
	MsgTypeRequest   uint8 = 0x05

	MsgTypeQueryState         uint8 = 0x10
	MsgTypeQueryStateResponse uint8 = 0x11

	MsgTypeQueryBlock         uint8 = 0x12
	MsgTypeQueryBlockResponse uint8 = 0x13

	MsgTypeQueryBlockBatch         uint8 = 0x14
	MsgTypeQueryBlockBatchStart    uint8 = 0x15
	MsgTypeQueryBlockBatchBlock    uint8 = 0x16
	MsgTypeQueryBlockBatchComplete uint8 = 0x17

	MsgTypeQueryGlobs         uint8 = 0x18
	MsgTypeQueryGlobsResponse uint8 = 0x19

	MsgTypeQueryError uint8 = 0x20

	MaxMessageSize = 100 * 1024 * 1024

	ErrorCodeBlockNotFound   uint16 = 1
	ErrorCodeInvalidRequest  uint16 = 2
	ErrorCodeServerError     uint16 = 3
	ErrorCodeMaxClientsReach uint16 = 4
	ErrorCodeServerSyncing   uint16 = 5
	ErrorCodeTooManyGlobs    uint16 = 6
)

type BlockMessage struct {
	BlockNum uint32
	LIB      uint32
	HEAD     uint32
	Data     []byte
}

type HeartbeatMessage struct {
	LIB  uint32
	HEAD uint32
}

type AckMessage struct {
	BlockNum uint32
}

type ErrorMessage struct {
	Code    uint16
	Message string
}

type RequestMessage struct {
	StartBlock uint32
}

type QueryStateMessage struct {
	RequestID uint64
}

type QueryStateResponseMessage struct {
	RequestID uint64
	HEAD      uint32
	LIB       uint32
}

type QueryBlockMessage struct {
	RequestID uint64
	BlockNum  uint32
}

type QueryBlockResponseMessage struct {
	RequestID uint64
	BlockNum  uint32
	BlockID   [32]byte
	Previous  [32]byte
	Data      []byte
}

type QueryBlockBatchMessage struct {
	RequestID  uint64
	StartBlock uint32
	EndBlock   uint32
}

type QueryBlockBatchStartMessage struct {
	RequestID   uint64
	TotalBlocks uint32
}

type QueryBlockBatchBlockMessage struct {
	RequestID uint64
	BlockNum  uint32
	Data      []byte
}

type QueryBlockBatchCompleteMessage struct {
	RequestID uint64
}

type QueryGlobsMessage struct {
	RequestID  uint64
	GlobalSeqs []uint64
}

type QueryGlobsResponseMessage struct {
	RequestID uint64
	LIB       uint32
	HEAD      uint32
	Actions   []ActionData
}

type QueryErrorMessage struct {
	RequestID uint64
	Code      uint16
	Message   string
}

func WriteMessage(w io.Writer, msgType uint8, payload []byte) error {
	length := uint32(len(payload) + 1)

	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], length)
	header[4] = msgType

	if _, err := w.Write(header); err != nil {
		return err
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	return nil
}

func ReadMessage(r io.Reader) (uint8, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	msgType := header[4]

	if length > MaxMessageSize {
		return 0, nil, errors.New("message too large")
	}

	payloadLen := length - 1
	if payloadLen == 0 {
		return msgType, nil, nil
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}

func EncodeBlockMessage(msg *BlockMessage) []byte {
	payload := make([]byte, 12+len(msg.Data))
	binary.LittleEndian.PutUint32(payload[0:4], msg.BlockNum)
	binary.LittleEndian.PutUint32(payload[4:8], msg.LIB)
	binary.LittleEndian.PutUint32(payload[8:12], msg.HEAD)
	copy(payload[12:], msg.Data)
	return payload
}

func DecodeBlockMessage(payload []byte) (*BlockMessage, error) {
	if len(payload) < 12 {
		return nil, errors.New("block message too short")
	}

	return &BlockMessage{
		BlockNum: binary.LittleEndian.Uint32(payload[0:4]),
		LIB:      binary.LittleEndian.Uint32(payload[4:8]),
		HEAD:     binary.LittleEndian.Uint32(payload[8:12]),
		Data:     payload[12:],
	}, nil
}

func EncodeHeartbeatMessage(msg *HeartbeatMessage) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], msg.LIB)
	binary.LittleEndian.PutUint32(payload[4:8], msg.HEAD)
	return payload
}

func DecodeHeartbeatMessage(payload []byte) (*HeartbeatMessage, error) {
	if len(payload) < 8 {
		return nil, errors.New("heartbeat message too short")
	}

	return &HeartbeatMessage{
		LIB:  binary.LittleEndian.Uint32(payload[0:4]),
		HEAD: binary.LittleEndian.Uint32(payload[4:8]),
	}, nil
}

func EncodeAckMessage(msg *AckMessage) []byte {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload[0:4], msg.BlockNum)
	return payload
}

func DecodeAckMessage(payload []byte) (*AckMessage, error) {
	if len(payload) < 4 {
		return nil, errors.New("ack message too short")
	}

	return &AckMessage{
		BlockNum: binary.LittleEndian.Uint32(payload[0:4]),
	}, nil
}

func EncodeErrorMessage(msg *ErrorMessage) []byte {
	msgBytes := []byte(msg.Message)
	payload := make([]byte, 2+len(msgBytes))
	binary.LittleEndian.PutUint16(payload[0:2], msg.Code)
	copy(payload[2:], msgBytes)
	return payload
}

func DecodeErrorMessage(payload []byte) (*ErrorMessage, error) {
	if len(payload) < 2 {
		return nil, errors.New("error message too short")
	}

	return &ErrorMessage{
		Code:    binary.LittleEndian.Uint16(payload[0:2]),
		Message: string(payload[2:]),
	}, nil
}

func EncodeRequestMessage(msg *RequestMessage) []byte {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload[0:4], msg.StartBlock)
	return payload
}

func DecodeRequestMessage(payload []byte) (*RequestMessage, error) {
	if len(payload) < 4 {
		return nil, errors.New("request message too short")
	}

	return &RequestMessage{
		StartBlock: binary.LittleEndian.Uint32(payload[0:4]),
	}, nil
}

func EncodeQueryStateMessage(msg *QueryStateMessage) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	return payload
}

func DecodeQueryStateMessage(payload []byte) (*QueryStateMessage, error) {
	if len(payload) < 8 {
		return nil, errors.New("query state message too short")
	}
	return &QueryStateMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
	}, nil
}

func EncodeQueryStateResponseMessage(msg *QueryStateResponseMessage) []byte {
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.HEAD)
	binary.LittleEndian.PutUint32(payload[12:16], msg.LIB)
	return payload
}

func DecodeQueryStateResponseMessage(payload []byte) (*QueryStateResponseMessage, error) {
	if len(payload) < 16 {
		return nil, errors.New("query state response too short")
	}
	return &QueryStateResponseMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
		HEAD:      binary.LittleEndian.Uint32(payload[8:12]),
		LIB:       binary.LittleEndian.Uint32(payload[12:16]),
	}, nil
}

func EncodeQueryBlockMessage(msg *QueryBlockMessage) []byte {
	payload := make([]byte, 12)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.BlockNum)
	return payload
}

func DecodeQueryBlockMessage(payload []byte) (*QueryBlockMessage, error) {
	if len(payload) < 12 {
		return nil, errors.New("query block message too short")
	}
	return &QueryBlockMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
		BlockNum:  binary.LittleEndian.Uint32(payload[8:12]),
	}, nil
}

func EncodeQueryBlockResponseMessage(msg *QueryBlockResponseMessage) []byte {
	payload := make([]byte, 76+len(msg.Data))
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.BlockNum)
	copy(payload[12:44], msg.BlockID[:])
	copy(payload[44:76], msg.Previous[:])
	copy(payload[76:], msg.Data)
	return payload
}

func DecodeQueryBlockResponseMessage(payload []byte) (*QueryBlockResponseMessage, error) {
	if len(payload) < 76 {
		return nil, errors.New("query block response too short")
	}
	msg := &QueryBlockResponseMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
		BlockNum:  binary.LittleEndian.Uint32(payload[8:12]),
		Data:      payload[76:],
	}
	copy(msg.BlockID[:], payload[12:44])
	copy(msg.Previous[:], payload[44:76])
	return msg, nil
}

func EncodeQueryBlockBatchMessage(msg *QueryBlockBatchMessage) []byte {
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.StartBlock)
	binary.LittleEndian.PutUint32(payload[12:16], msg.EndBlock)
	return payload
}

func DecodeQueryBlockBatchMessage(payload []byte) (*QueryBlockBatchMessage, error) {
	if len(payload) < 16 {
		return nil, errors.New("query block batch message too short")
	}
	return &QueryBlockBatchMessage{
		RequestID:  binary.LittleEndian.Uint64(payload[0:8]),
		StartBlock: binary.LittleEndian.Uint32(payload[8:12]),
		EndBlock:   binary.LittleEndian.Uint32(payload[12:16]),
	}, nil
}

func EncodeQueryBlockBatchStartMessage(msg *QueryBlockBatchStartMessage) []byte {
	payload := make([]byte, 12)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.TotalBlocks)
	return payload
}

func DecodeQueryBlockBatchStartMessage(payload []byte) (*QueryBlockBatchStartMessage, error) {
	if len(payload) < 12 {
		return nil, errors.New("query block batch start message too short")
	}
	return &QueryBlockBatchStartMessage{
		RequestID:   binary.LittleEndian.Uint64(payload[0:8]),
		TotalBlocks: binary.LittleEndian.Uint32(payload[8:12]),
	}, nil
}

func EncodeQueryBlockBatchBlockMessage(msg *QueryBlockBatchBlockMessage) []byte {
	payload := make([]byte, 12+len(msg.Data))
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.BlockNum)
	copy(payload[12:], msg.Data)
	return payload
}

func DecodeQueryBlockBatchBlockMessage(payload []byte) (*QueryBlockBatchBlockMessage, error) {
	if len(payload) < 12 {
		return nil, errors.New("query block batch block message too short")
	}
	return &QueryBlockBatchBlockMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
		BlockNum:  binary.LittleEndian.Uint32(payload[8:12]),
		Data:      payload[12:],
	}, nil
}

func EncodeQueryBlockBatchCompleteMessage(msg *QueryBlockBatchCompleteMessage) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	return payload
}

func DecodeQueryBlockBatchCompleteMessage(payload []byte) (*QueryBlockBatchCompleteMessage, error) {
	if len(payload) < 8 {
		return nil, errors.New("query block batch complete message too short")
	}
	return &QueryBlockBatchCompleteMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
	}, nil
}

func EncodeQueryGlobsMessage(msg *QueryGlobsMessage) []byte {
	payload := make([]byte, 12+8*len(msg.GlobalSeqs))
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(msg.GlobalSeqs)))
	for i, seq := range msg.GlobalSeqs {
		binary.LittleEndian.PutUint64(payload[12+i*8:12+(i+1)*8], seq)
	}
	return payload
}

func DecodeQueryGlobsMessage(payload []byte) (*QueryGlobsMessage, error) {
	if len(payload) < 12 {
		return nil, errors.New("query globs message too short")
	}
	count := binary.LittleEndian.Uint32(payload[8:12])
	if len(payload) < 12+int(count)*8 {
		return nil, errors.New("query globs message truncated")
	}
	seqs := make([]uint64, count)
	for i := uint32(0); i < count; i++ {
		seqs[i] = binary.LittleEndian.Uint64(payload[12+i*8 : 12+(i+1)*8])
	}
	return &QueryGlobsMessage{
		RequestID:  binary.LittleEndian.Uint64(payload[0:8]),
		GlobalSeqs: seqs,
	}, nil
}

func EncodeQueryGlobsResponseMessage(msg *QueryGlobsResponseMessage) []byte {
	totalSize := 20
	for _, a := range msg.Actions {
		totalSize += 20 + len(a.Data)
	}
	payload := make([]byte, totalSize)
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint32(payload[8:12], msg.LIB)
	binary.LittleEndian.PutUint32(payload[12:16], msg.HEAD)
	binary.LittleEndian.PutUint32(payload[16:20], uint32(len(msg.Actions)))

	offset := 20
	for _, a := range msg.Actions {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], a.GlobalSeq)
		binary.LittleEndian.PutUint32(payload[offset+8:offset+12], a.BlockNum)
		binary.LittleEndian.PutUint32(payload[offset+12:offset+16], a.BlockTime)
		binary.LittleEndian.PutUint32(payload[offset+16:offset+20], uint32(len(a.Data)))
		copy(payload[offset+20:offset+20+len(a.Data)], a.Data)
		offset += 20 + len(a.Data)
	}
	return payload
}

func DecodeQueryGlobsResponseMessage(payload []byte) (*QueryGlobsResponseMessage, error) {
	if len(payload) < 20 {
		return nil, errors.New("query globs response too short")
	}
	msg := &QueryGlobsResponseMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
		LIB:       binary.LittleEndian.Uint32(payload[8:12]),
		HEAD:      binary.LittleEndian.Uint32(payload[12:16]),
	}
	count := binary.LittleEndian.Uint32(payload[16:20])
	msg.Actions = make([]ActionData, 0, count)

	offset := 20
	for i := uint32(0); i < count; i++ {
		if offset+20 > len(payload) {
			return nil, errors.New("query globs response truncated")
		}
		globalSeq := binary.LittleEndian.Uint64(payload[offset : offset+8])
		blockNum := binary.LittleEndian.Uint32(payload[offset+8 : offset+12])
		blockTime := binary.LittleEndian.Uint32(payload[offset+12 : offset+16])
		dataLen := binary.LittleEndian.Uint32(payload[offset+16 : offset+20])
		offset += 20

		if offset+int(dataLen) > len(payload) {
			return nil, errors.New("query globs response action data truncated")
		}
		data := make([]byte, dataLen)
		copy(data, payload[offset:offset+int(dataLen)])
		offset += int(dataLen)

		msg.Actions = append(msg.Actions, ActionData{
			GlobalSeq: globalSeq,
			BlockNum:  blockNum,
			BlockTime: blockTime,
			Data:      data,
		})
	}
	return msg, nil
}

func EncodeQueryErrorMessage(msg *QueryErrorMessage) []byte {
	msgBytes := []byte(msg.Message)
	payload := make([]byte, 10+len(msgBytes))
	binary.LittleEndian.PutUint64(payload[0:8], msg.RequestID)
	binary.LittleEndian.PutUint16(payload[8:10], msg.Code)
	copy(payload[10:], msgBytes)
	return payload
}

func DecodeQueryErrorMessage(payload []byte) (*QueryErrorMessage, error) {
	if len(payload) < 10 {
		return nil, errors.New("query error message too short")
	}
	return &QueryErrorMessage{
		RequestID: binary.LittleEndian.Uint64(payload[0:8]),
		Code:      binary.LittleEndian.Uint16(payload[8:10]),
		Message:   string(payload[10:]),
	}, nil
}

type GetBlockResponse struct {
	BlockNum uint32
	LIB      uint32
	HEAD     uint32
	BlockID  [32]byte
	Previous [32]byte
	Data     []byte
}

func EncodeGetBlockResponse(resp *GetBlockResponse) []byte {
	payload := make([]byte, 76+len(resp.Data))
	binary.LittleEndian.PutUint32(payload[0:4], resp.BlockNum)
	binary.LittleEndian.PutUint32(payload[4:8], resp.LIB)
	binary.LittleEndian.PutUint32(payload[8:12], resp.HEAD)
	copy(payload[12:44], resp.BlockID[:])
	copy(payload[44:76], resp.Previous[:])
	copy(payload[76:], resp.Data)
	return payload
}

func DecodeGetBlockResponse(payload []byte) (*GetBlockResponse, error) {
	if len(payload) < 76 {
		return nil, errors.New("get_block response too short")
	}

	resp := &GetBlockResponse{
		BlockNum: binary.LittleEndian.Uint32(payload[0:4]),
		LIB:      binary.LittleEndian.Uint32(payload[4:8]),
		HEAD:     binary.LittleEndian.Uint32(payload[8:12]),
		Data:     payload[76:],
	}
	copy(resp.BlockID[:], payload[12:44])
	copy(resp.Previous[:], payload[44:76])
	return resp, nil
}

type GetActionsByGlobsResponse struct {
	LIB     uint32
	HEAD    uint32
	Actions []ActionData
}

type ActionData struct {
	GlobalSeq uint64
	BlockNum  uint32
	BlockTime uint32
	Data      []byte
}

func EncodeGetActionsByGlobsResponse(resp *GetActionsByGlobsResponse) []byte {
	totalSize := 12
	for _, a := range resp.Actions {
		totalSize += 20 + len(a.Data)
	}

	payload := make([]byte, totalSize)
	binary.LittleEndian.PutUint32(payload[0:4], resp.LIB)
	binary.LittleEndian.PutUint32(payload[4:8], resp.HEAD)
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(resp.Actions)))

	offset := 12
	for _, a := range resp.Actions {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], a.GlobalSeq)
		binary.LittleEndian.PutUint32(payload[offset+8:offset+12], a.BlockNum)
		binary.LittleEndian.PutUint32(payload[offset+12:offset+16], a.BlockTime)
		binary.LittleEndian.PutUint32(payload[offset+16:offset+20], uint32(len(a.Data)))
		copy(payload[offset+20:offset+20+len(a.Data)], a.Data)
		offset += 20 + len(a.Data)
	}

	return payload
}

func DecodeGetActionsByGlobsResponse(payload []byte) (*GetActionsByGlobsResponse, error) {
	if len(payload) < 12 {
		return nil, errors.New("get_actions_by_globs response too short")
	}

	resp := &GetActionsByGlobsResponse{
		LIB:  binary.LittleEndian.Uint32(payload[0:4]),
		HEAD: binary.LittleEndian.Uint32(payload[4:8]),
	}
	count := binary.LittleEndian.Uint32(payload[8:12])
	resp.Actions = make([]ActionData, 0, count)

	offset := 12
	for i := uint32(0); i < count; i++ {
		if offset+20 > len(payload) {
			return nil, errors.New("get_actions_by_globs response truncated")
		}
		globalSeq := binary.LittleEndian.Uint64(payload[offset : offset+8])
		blockNum := binary.LittleEndian.Uint32(payload[offset+8 : offset+12])
		blockTime := binary.LittleEndian.Uint32(payload[offset+12 : offset+16])
		dataLen := binary.LittleEndian.Uint32(payload[offset+16 : offset+20])
		offset += 20

		if offset+int(dataLen) > len(payload) {
			return nil, errors.New("get_actions_by_globs action data truncated")
		}
		data := make([]byte, dataLen)
		copy(data, payload[offset:offset+int(dataLen)])
		offset += int(dataLen)

		resp.Actions = append(resp.Actions, ActionData{
			GlobalSeq: globalSeq,
			BlockNum:  blockNum,
			BlockTime: blockTime,
			Data:      data,
		})
	}

	return resp, nil
}

type GetStateResponse struct {
	HEAD uint32
	LIB  uint32
}

func EncodeGetStateResponse(resp *GetStateResponse) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], resp.HEAD)
	binary.LittleEndian.PutUint32(payload[4:8], resp.LIB)
	return payload
}

func DecodeGetStateResponse(payload []byte) (*GetStateResponse, error) {
	if len(payload) < 8 {
		return nil, errors.New("get_state response too short")
	}
	return &GetStateResponse{
		HEAD: binary.LittleEndian.Uint32(payload[0:4]),
		LIB:  binary.LittleEndian.Uint32(payload[4:8]),
	}, nil
}
