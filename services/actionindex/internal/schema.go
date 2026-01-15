package internal

import (
	"encoding/binary"
)

const (
	PrefixLegacyAccountActions = 0x10
	PrefixAccountActions       = 0x20
	PrefixContractAction       = 0x21
	PrefixContractWildcard     = 0x22
	PrefixProperties           = 0x90
	PrefixWAL                  = 0x91
	PrefixTimeMap              = 0x92

	PrefixLegacyTimeMap    = 0x13
	PrefixLegacyWAL        = 0x14
	PrefixLegacyProperties = 0x15
)

func makeLegacyAccountActionsKey(account uint64, chunkID uint32) []byte {
	buf := make([]byte, 13)
	buf[0] = PrefixLegacyAccountActions
	binary.BigEndian.PutUint64(buf[1:9], account)
	binary.BigEndian.PutUint32(buf[9:13], chunkID)
	return buf
}

func makeLegacyAccountActionsPrefix(account uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = PrefixLegacyAccountActions
	binary.BigEndian.PutUint64(buf[1:9], account)
	return buf
}

func parseLegacyAccountActionsKey(key []byte) (account uint64, chunkID uint32, ok bool) {
	if len(key) != 13 || key[0] != PrefixLegacyAccountActions {
		return 0, 0, false
	}
	account = binary.BigEndian.Uint64(key[1:9])
	chunkID = binary.BigEndian.Uint32(key[9:13])
	return account, chunkID, true
}

func makeTimeMapKey(hour uint32) []byte {
	buf := make([]byte, 5)
	buf[0] = PrefixTimeMap
	binary.BigEndian.PutUint32(buf[1:5], hour)
	return buf
}

func parseTimeMapKey(key []byte) (hour uint32, ok bool) {
	if len(key) != 5 || key[0] != PrefixTimeMap {
		return 0, false
	}
	hour = binary.BigEndian.Uint32(key[1:5])
	return hour, true
}

func makeTimeMapValue(minSeq, maxSeq uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], minSeq)
	binary.BigEndian.PutUint64(buf[8:16], maxSeq)
	return buf
}

func parseTimeMapValue(val []byte) (minSeq, maxSeq uint64, ok bool) {
	if len(val) != 16 {
		return 0, 0, false
	}
	minSeq = binary.BigEndian.Uint64(val[0:8])
	maxSeq = binary.BigEndian.Uint64(val[8:16])
	return minSeq, maxSeq, true
}

func makeWALKey(globalSeq uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = PrefixWAL
	binary.BigEndian.PutUint64(buf[1:9], globalSeq)
	return buf
}

func parseWALKey(key []byte) (globalSeq uint64, ok bool) {
	if len(key) != 9 || key[0] != PrefixWAL {
		return 0, false
	}
	globalSeq = binary.BigEndian.Uint64(key[1:9])
	return globalSeq, true
}

func makeWALValue(account, contract, action uint64) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], account)
	binary.BigEndian.PutUint64(buf[8:16], contract)
	binary.BigEndian.PutUint64(buf[16:24], action)
	return buf
}

func parseWALValue(val []byte) (account, contract, action uint64, ok bool) {
	if len(val) != 24 {
		return 0, 0, 0, false
	}
	account = binary.BigEndian.Uint64(val[0:8])
	contract = binary.BigEndian.Uint64(val[8:16])
	action = binary.BigEndian.Uint64(val[16:24])
	return account, contract, action, true
}

func makePropertiesKey() []byte {
	return []byte{PrefixProperties}
}

func makePropertiesValue(libNum, headNum uint32) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], libNum)
	binary.BigEndian.PutUint32(buf[4:8], headNum)
	return buf
}

func parsePropertiesValue(val []byte) (libNum, headNum uint32, ok bool) {
	if len(val) != 8 {
		return 0, 0, false
	}
	libNum = binary.BigEndian.Uint32(val[0:4])
	headNum = binary.BigEndian.Uint32(val[4:8])
	return libNum, headNum, true
}

func makeAccountActionsKey(account, baseSeq uint64) []byte {
	buf := make([]byte, 17)
	buf[0] = PrefixAccountActions
	binary.BigEndian.PutUint64(buf[1:9], account)
	binary.BigEndian.PutUint64(buf[9:17], baseSeq)
	return buf
}

func makeAccountActionsPrefix(account uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = PrefixAccountActions
	binary.BigEndian.PutUint64(buf[1:9], account)
	return buf
}

func parseAccountActionsKey(key []byte) (account, baseSeq uint64, ok bool) {
	if len(key) != 17 || key[0] != PrefixAccountActions {
		return 0, 0, false
	}
	account = binary.BigEndian.Uint64(key[1:9])
	baseSeq = binary.BigEndian.Uint64(key[9:17])
	return account, baseSeq, true
}

func makeContractActionKey(account, contract, action, baseSeq uint64) []byte {
	buf := make([]byte, 33)
	buf[0] = PrefixContractAction
	binary.BigEndian.PutUint64(buf[1:9], account)
	binary.BigEndian.PutUint64(buf[9:17], contract)
	binary.BigEndian.PutUint64(buf[17:25], action)
	binary.BigEndian.PutUint64(buf[25:33], baseSeq)
	return buf
}

func makeContractActionPrefix(account, contract, action uint64) []byte {
	buf := make([]byte, 25)
	buf[0] = PrefixContractAction
	binary.BigEndian.PutUint64(buf[1:9], account)
	binary.BigEndian.PutUint64(buf[9:17], contract)
	binary.BigEndian.PutUint64(buf[17:25], action)
	return buf
}

func parseContractActionKey(key []byte) (account, contract, action, baseSeq uint64, ok bool) {
	if len(key) != 33 || key[0] != PrefixContractAction {
		return 0, 0, 0, 0, false
	}
	account = binary.BigEndian.Uint64(key[1:9])
	contract = binary.BigEndian.Uint64(key[9:17])
	action = binary.BigEndian.Uint64(key[17:25])
	baseSeq = binary.BigEndian.Uint64(key[25:33])
	return account, contract, action, baseSeq, true
}

func makeContractWildcardKey(account, contract, baseSeq uint64) []byte {
	buf := make([]byte, 25)
	buf[0] = PrefixContractWildcard
	binary.BigEndian.PutUint64(buf[1:9], account)
	binary.BigEndian.PutUint64(buf[9:17], contract)
	binary.BigEndian.PutUint64(buf[17:25], baseSeq)
	return buf
}

func makeContractWildcardPrefix(account, contract uint64) []byte {
	buf := make([]byte, 17)
	buf[0] = PrefixContractWildcard
	binary.BigEndian.PutUint64(buf[1:9], account)
	binary.BigEndian.PutUint64(buf[9:17], contract)
	return buf
}

func parseContractWildcardKey(key []byte) (account, contract, baseSeq uint64, ok bool) {
	if len(key) != 25 || key[0] != PrefixContractWildcard {
		return 0, 0, 0, false
	}
	account = binary.BigEndian.Uint64(key[1:9])
	contract = binary.BigEndian.Uint64(key[9:17])
	baseSeq = binary.BigEndian.Uint64(key[17:25])
	return account, contract, baseSeq, true
}

func makeLegacyPropertiesKey() []byte {
	return []byte{PrefixLegacyProperties}
}

func parseLegacyWALKey(key []byte) (globalSeq uint64, ok bool) {
	if len(key) != 9 || key[0] != PrefixLegacyWAL {
		return 0, false
	}
	globalSeq = binary.BigEndian.Uint64(key[1:9])
	return globalSeq, true
}

func parseLegacyTimeMapKey(key []byte) (hour uint32, ok bool) {
	if len(key) != 5 || key[0] != PrefixLegacyTimeMap {
		return 0, false
	}
	hour = binary.BigEndian.Uint32(key[1:5])
	return hour, true
}
