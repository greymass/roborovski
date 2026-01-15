package internal

import (
	"bytes"
	"testing"
)

func TestLegacyAccountActionsKey(t *testing.T) {
	account := uint64(0x123456789ABCDEF0)
	chunkID := uint32(42)

	key := makeLegacyAccountActionsKey(account, chunkID)

	if key[0] != PrefixLegacyAccountActions {
		t.Errorf("prefix: got %x, want %x", key[0], PrefixLegacyAccountActions)
	}

	parsedAccount, parsedChunkID, ok := parseLegacyAccountActionsKey(key)
	if !ok {
		t.Fatal("parse failed")
	}
	if parsedAccount != account {
		t.Errorf("account: got %x, want %x", parsedAccount, account)
	}
	if parsedChunkID != chunkID {
		t.Errorf("chunkID: got %d, want %d", parsedChunkID, chunkID)
	}
}

func TestLegacyAccountActionsPrefix(t *testing.T) {
	account := uint64(0x123456789ABCDEF0)

	prefix := makeLegacyAccountActionsPrefix(account)
	key0 := makeLegacyAccountActionsKey(account, 0)
	key100 := makeLegacyAccountActionsKey(account, 100)

	if !bytes.HasPrefix(key0, prefix) {
		t.Error("key0 should have prefix")
	}
	if !bytes.HasPrefix(key100, prefix) {
		t.Error("key100 should have prefix")
	}

	otherKey := makeLegacyAccountActionsKey(account+1, 0)
	if bytes.HasPrefix(otherKey, prefix) {
		t.Error("other account key should not have prefix")
	}
}

func TestAccountActionsKey(t *testing.T) {
	account := uint64(0x123456789ABCDEF0)
	baseSeq := uint64(1000000)

	key := makeAccountActionsKey(account, baseSeq)

	if key[0] != PrefixAccountActions {
		t.Errorf("prefix: got %x, want %x", key[0], PrefixAccountActions)
	}

	parsedAccount, parsedBaseSeq, ok := parseAccountActionsKey(key)
	if !ok {
		t.Fatal("parse failed")
	}
	if parsedAccount != account {
		t.Errorf("account: got %x, want %x", parsedAccount, account)
	}
	if parsedBaseSeq != baseSeq {
		t.Errorf("baseSeq: got %d, want %d", parsedBaseSeq, baseSeq)
	}
}

func TestContractActionKey(t *testing.T) {
	account := uint64(0x1111111111111111)
	contract := uint64(0x2222222222222222)
	action := uint64(0x3333333333333333)
	baseSeq := uint64(1000000)

	key := makeContractActionKey(account, contract, action, baseSeq)

	if key[0] != PrefixContractAction {
		t.Errorf("prefix: got %x, want %x", key[0], PrefixContractAction)
	}

	pAccount, pContract, pAction, pBaseSeq, ok := parseContractActionKey(key)
	if !ok {
		t.Fatal("parse failed")
	}
	if pAccount != account {
		t.Errorf("account: got %x, want %x", pAccount, account)
	}
	if pContract != contract {
		t.Errorf("contract: got %x, want %x", pContract, contract)
	}
	if pAction != action {
		t.Errorf("action: got %x, want %x", pAction, action)
	}
	if pBaseSeq != baseSeq {
		t.Errorf("baseSeq: got %d, want %d", pBaseSeq, baseSeq)
	}
}

func TestContractWildcardKey(t *testing.T) {
	account := uint64(0x1111111111111111)
	contract := uint64(0x2222222222222222)
	baseSeq := uint64(1000000)

	key := makeContractWildcardKey(account, contract, baseSeq)

	if key[0] != PrefixContractWildcard {
		t.Errorf("prefix: got %x, want %x", key[0], PrefixContractWildcard)
	}

	pAccount, pContract, pBaseSeq, ok := parseContractWildcardKey(key)
	if !ok {
		t.Fatal("parse failed")
	}
	if pAccount != account {
		t.Errorf("account: got %x, want %x", pAccount, account)
	}
	if pContract != contract {
		t.Errorf("contract: got %x, want %x", pContract, contract)
	}
	if pBaseSeq != baseSeq {
		t.Errorf("baseSeq: got %d, want %d", pBaseSeq, baseSeq)
	}
}

func TestTimeMapKey(t *testing.T) {
	hour := uint32(473567)

	key := makeTimeMapKey(hour)

	if key[0] != PrefixTimeMap {
		t.Errorf("prefix: got %x, want %x", key[0], PrefixTimeMap)
	}

	parsedHour, ok := parseTimeMapKey(key)
	if !ok {
		t.Fatal("parse failed")
	}
	if parsedHour != hour {
		t.Errorf("hour: got %d, want %d", parsedHour, hour)
	}
}

func TestTimeMapValue(t *testing.T) {
	minSeq := uint64(1000000)
	maxSeq := uint64(2000000)

	val := makeTimeMapValue(minSeq, maxSeq)

	pMinSeq, pMaxSeq, ok := parseTimeMapValue(val)
	if !ok {
		t.Fatal("parse failed")
	}
	if pMinSeq != minSeq {
		t.Errorf("minSeq: got %d, want %d", pMinSeq, minSeq)
	}
	if pMaxSeq != maxSeq {
		t.Errorf("maxSeq: got %d, want %d", pMaxSeq, maxSeq)
	}
}

func TestWALKey(t *testing.T) {
	globalSeq := uint64(123456789012345)

	key := makeWALKey(globalSeq)

	if key[0] != PrefixWAL {
		t.Errorf("prefix: got %x, want %x", key[0], PrefixWAL)
	}

	parsedSeq, ok := parseWALKey(key)
	if !ok {
		t.Fatal("parse failed")
	}
	if parsedSeq != globalSeq {
		t.Errorf("globalSeq: got %d, want %d", parsedSeq, globalSeq)
	}
}

func TestWALValue(t *testing.T) {
	account := uint64(0x1111111111111111)
	contract := uint64(0x2222222222222222)
	action := uint64(0x3333333333333333)

	val := makeWALValue(account, contract, action)

	pAccount, pContract, pAction, ok := parseWALValue(val)
	if !ok {
		t.Fatal("parse failed")
	}
	if pAccount != account {
		t.Errorf("account: got %x, want %x", pAccount, account)
	}
	if pContract != contract {
		t.Errorf("contract: got %x, want %x", pContract, contract)
	}
	if pAction != action {
		t.Errorf("action: got %x, want %x", pAction, action)
	}
}

func TestPropertiesValue(t *testing.T) {
	libNum := uint32(100000)
	headNum := uint32(100050)

	val := makePropertiesValue(libNum, headNum)

	pLibNum, pHeadNum, ok := parsePropertiesValue(val)
	if !ok {
		t.Fatal("parse failed")
	}
	if pLibNum != libNum {
		t.Errorf("libNum: got %d, want %d", pLibNum, libNum)
	}
	if pHeadNum != headNum {
		t.Errorf("headNum: got %d, want %d", pHeadNum, headNum)
	}
}

func TestLegacyKeyOrdering(t *testing.T) {
	key0 := makeLegacyAccountActionsKey(100, 0)
	key1 := makeLegacyAccountActionsKey(100, 1)
	key2 := makeLegacyAccountActionsKey(100, 2)

	if bytes.Compare(key0, key1) >= 0 {
		t.Error("key0 should be < key1")
	}
	if bytes.Compare(key1, key2) >= 0 {
		t.Error("key1 should be < key2")
	}

	keyOther := makeLegacyAccountActionsKey(101, 0)
	if bytes.Compare(key2, keyOther) >= 0 {
		t.Error("key2 should be < keyOther (different account)")
	}
}

func TestNewKeyOrdering(t *testing.T) {
	key0 := makeAccountActionsKey(100, 1000)
	key1 := makeAccountActionsKey(100, 2000)
	key2 := makeAccountActionsKey(100, 3000)

	if bytes.Compare(key0, key1) >= 0 {
		t.Error("key0 should be < key1")
	}
	if bytes.Compare(key1, key2) >= 0 {
		t.Error("key1 should be < key2")
	}

	keyOther := makeAccountActionsKey(101, 1000)
	if bytes.Compare(key2, keyOther) >= 0 {
		t.Error("key2 should be < keyOther (different account)")
	}
}
