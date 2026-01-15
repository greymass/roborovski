package abicache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/greymass/go-eosio/pkg/abi"
	goeosio "github.com/greymass/go-eosio/pkg/chain"
	"github.com/greymass/roborovski/libraries/chain"
)

var (
	eosioName  = chain.StringToName("eosio")
	setabiName = chain.StringToName("setabi")
)

func IsSetabi(contract, action uint64) bool {
	return contract == eosioName && action == setabiName
}

func ParseSetabi(actionData []byte) (contract uint64, abiJSON []byte, err error) {
	if len(actionData) < 8 {
		return 0, nil, fmt.Errorf("action data too short")
	}

	contract = binary.LittleEndian.Uint64(actionData[0:8])

	offset := 8
	if offset >= len(actionData) {
		return 0, nil, fmt.Errorf("missing ABI length")
	}

	abiLen, bytesRead := readVarUint32(actionData[offset:])
	if bytesRead == 0 {
		return 0, nil, fmt.Errorf("could not read ABI length")
	}
	offset += bytesRead

	if offset+int(abiLen) > len(actionData) {
		return 0, nil, fmt.Errorf("ABI length exceeds data")
	}

	if abiLen == 0 {
		return contract, nil, nil
	}

	abiBytes := actionData[offset : offset+int(abiLen)]

	abiJSON, err = unpackBinaryABI(abiBytes)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to unpack ABI: %w", err)
	}

	return contract, abiJSON, nil
}

func readVarUint32(data []byte) (uint32, int) {
	var result uint32
	var shift uint
	bytesRead := 0

	for {
		if bytesRead >= len(data) {
			return 0, 0
		}

		b := data[bytesRead]
		bytesRead++

		result |= uint32(b&0x7f) << shift
		shift += 7

		if (b & 0x80) == 0 {
			break
		}
	}

	return result, bytesRead
}

func unpackBinaryABI(binaryABI []byte) ([]byte, error) {
	reader := bytes.NewReader(binaryABI)
	decoder := abi.NewDecoder(reader, func(dec *abi.Decoder, v interface{}) (done bool, err error) {
		return false, nil
	})

	var abiStruct goeosio.Abi
	if err := decoder.Decode(&abiStruct); err != nil {
		return nil, fmt.Errorf("failed to decode binary ABI: %w", err)
	}

	jsonBytes, err := json.Marshal(abiStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ABI to JSON: %w", err)
	}

	return jsonBytes, nil
}
