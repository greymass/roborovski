package chain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
)

type Action struct {
	Account       string            `json:"account"`
	Name          string            `json:"name"`
	Authorization []PermissionLevel `json:"authorization"`
	Data          string            `json:"data"` // Hex-encoded action data
}

func (act *Action) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, StringToName(act.Account))
	binary.Write(buf, binary.LittleEndian, StringToName(act.Name))

	putAsUVarint(buf, uint64(len(act.Authorization)))
	for i := 0; i < len(act.Authorization); i++ {
		binary.Write(buf, binary.LittleEndian, StringToName(act.Authorization[i].Actor))
		binary.Write(buf, binary.LittleEndian, StringToName(act.Authorization[i].Permission))
	}

	bdata, err := hex.DecodeString(act.Data)
	if err != nil {
		panic(err) // Caller should use enforce pattern
	}
	putAsUVarint(buf, uint64(len(bdata)))
	buf.Write(bdata[:])

	return buf.Bytes()
}

func (act *Action) Digest() string {
	hash := sha256.Sum256(act.Bytes())
	return hex.EncodeToString(hash[:])
}

type ActionReceipt struct {
	Receiver       string          `json:"receiver"`
	ActDigest      string          `json:"act_digest"`
	GlobalSequence json.Number     `json:"global_sequence"`
	RecvSequence   json.Number     `json:"recv_sequence"`
	AuthSequence   [][]interface{} `json:"auth_sequence"` // Array of [account, sequence] pairs
	CodeSequence   uint32          `json:"code_sequence"`
	AbiSequence    uint32          `json:"abi_sequence"`
}

type ActionTrace struct {
	ActionOrdinal    uint32         `json:"action_ordinal"`
	CreatorAO        uint32         `json:"creator_action_ordinal"`
	ClosestUAAO      uint32         `json:"closest_unnotified_ancestor_action_ordinal"`
	Receipt          ActionReceipt  `json:"receipt"`
	Receiver         string         `json:"receiver"`
	Act              Action         `json:"act"`
	ContextFree      bool           `json:"context_free"`
	Elapsed          int64          `json:"elapsed"`
	TrxID            string         `json:"trx_id"`
	BlockNum         uint32         `json:"block_num"`
	BlockTime        string         `json:"block_time"`
	ProducerBlockID  string         `json:"producer_block_id"`
	AccountRAMDeltas []AccountDelta `json:"account_ram_deltas"`

	GlobalSeqUint64 uint64 `json:"-"`
}

func putAsUVarint(buf *bytes.Buffer, val uint64) {
	for val >= 0x80 {
		buf.WriteByte(byte(val) | 0x80)
		val >>= 7
	}
	buf.WriteByte(byte(val))
}
