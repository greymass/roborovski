package abicache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

func TestWriterReader(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract1 := chain.StringToName("eosio.token")
	contract2 := chain.StringToName("eosio")
	abi1 := []byte(`{"version":"eosio::abi/1.1","types":[],"structs":[],"actions":[]}`)
	abi2 := []byte(`{"version":"eosio::abi/1.2","types":[],"structs":[],"actions":[]}`)
	abi3 := []byte(`{"version":"eosio::abi/1.0","types":[],"structs":[],"actions":[]}`)

	if err := w.Write(10, contract1, abi1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(50, contract1, abi2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(30, contract2, abi3); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	got, err := r.Get(contract1, 40)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(abi1) {
		t.Errorf("Get(contract1, 40) = %s, want %s", got, abi1)
	}

	got, err = r.Get(contract1, 50)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(abi2) {
		t.Errorf("Get(contract1, 50) = %s, want %s", got, abi2)
	}

	got, err = r.Get(contract1, 100)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(abi2) {
		t.Errorf("Get(contract1, 100) = %s, want %s", got, abi2)
	}

	got, err = r.Get(contract2, 100)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(abi3) {
		t.Errorf("Get(contract2, 100) = %s, want %s", got, abi3)
	}

	_, err = r.Get(contract1, 5)
	if err == nil {
		t.Error("Get(contract1, 5) should have failed")
	}
}

func TestIsSetabi(t *testing.T) {
	eosio := chain.StringToName("eosio")
	setabi := chain.StringToName("setabi")
	transfer := chain.StringToName("transfer")
	token := chain.StringToName("eosio.token")

	if !IsSetabi(eosio, setabi) {
		t.Error("IsSetabi(eosio, setabi) should be true")
	}
	if IsSetabi(eosio, transfer) {
		t.Error("IsSetabi(eosio, transfer) should be false")
	}
	if IsSetabi(token, setabi) {
		t.Error("IsSetabi(token, setabi) should be false")
	}
}

func TestFileFormat(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("test")
	abi := []byte(`{"test":true}`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	indexData, err := os.ReadFile(filepath.Join(dir, "abis.index"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(indexData) != 20 {
		t.Errorf("index size = %d, want 20", len(indexData))
	}

	dataData, err := os.ReadFile(filepath.Join(dir, "abis.dat"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	expectedLen := 16 + len(abi)
	if len(dataData) != expectedLen {
		t.Errorf("data size = %d, want %d", len(dataData), expectedLen)
	}
}

func TestParsedABICacheMultipleContracts(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract1 := chain.StringToName("contract1")
	contract2 := chain.StringToName("contract2")
	contract3 := chain.StringToName("contract3")

	abi1 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"transfer","fields":[{"name":"amount","type":"uint64"}]}],"actions":[{"name":"transfer","type":"transfer"}]}`)
	abi2 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"vote","fields":[{"name":"voter","type":"name"}]}],"actions":[{"name":"vote","type":"vote"}]}`)
	abi3 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"claim","fields":[{"name":"owner","type":"name"}]}],"actions":[{"name":"claim","type":"claim"}]}`)

	if err := w.Write(10, contract1, abi1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(20, contract2, abi2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(30, contract3, abi3); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	got1, err := r.Get(contract1, 100)
	if err != nil {
		t.Fatalf("Get contract1 failed: %v", err)
	}
	if string(got1) != string(abi1) {
		t.Errorf("Get(contract1) mismatch")
	}

	got2, err := r.Get(contract2, 100)
	if err != nil {
		t.Fatalf("Get contract2 failed: %v", err)
	}
	if string(got2) != string(abi2) {
		t.Errorf("Get(contract2) mismatch")
	}

	got3, err := r.Get(contract3, 100)
	if err != nil {
		t.Fatalf("Get contract3 failed: %v", err)
	}
	if string(got3) != string(abi3) {
		t.Errorf("Get(contract3) mismatch")
	}

	if len(r.parsedABIs) != 0 {
		t.Errorf("parsedABIs should be empty before Decode, got %d entries", len(r.parsedABIs))
	}
}

func TestParsedABICacheSameContractMultipleVersions(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("mycontract")

	abiV1 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"action1","fields":[{"name":"val","type":"uint64"}]}],"actions":[{"name":"action1","type":"action1"}]}`)
	abiV2 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"action1","fields":[{"name":"val","type":"uint64"},{"name":"extra","type":"string"}]}],"actions":[{"name":"action1","type":"action1"}]}`)
	abiV3 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"action1","fields":[{"name":"newval","type":"uint128"}]}],"actions":[{"name":"action1","type":"action1"}]}`)

	if err := w.Write(100, contract, abiV1); err != nil {
		t.Fatalf("Write v1 failed: %v", err)
	}
	if err := w.Write(200, contract, abiV2); err != nil {
		t.Fatalf("Write v2 failed: %v", err)
	}
	if err := w.Write(300, contract, abiV3); err != nil {
		t.Fatalf("Write v3 failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	gotV1, err := r.Get(contract, 150)
	if err != nil {
		t.Fatalf("Get at block 150 failed: %v", err)
	}
	if string(gotV1) != string(abiV1) {
		t.Errorf("Get(150) should return v1")
	}

	gotV2, err := r.Get(contract, 250)
	if err != nil {
		t.Fatalf("Get at block 250 failed: %v", err)
	}
	if string(gotV2) != string(abiV2) {
		t.Errorf("Get(250) should return v2")
	}

	gotV3, err := r.Get(contract, 350)
	if err != nil {
		t.Fatalf("Get at block 350 failed: %v", err)
	}
	if string(gotV3) != string(abiV3) {
		t.Errorf("Get(350) should return v3")
	}

	gotV1Again, err := r.Get(contract, 199)
	if err != nil {
		t.Fatalf("Get at block 199 failed: %v", err)
	}
	if string(gotV1Again) != string(abiV1) {
		t.Errorf("Get(199) should return v1")
	}
}

func TestParsedABICacheOffsetDeduplication(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("dedup")
	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"test","fields":[{"name":"id","type":"uint64"}]}],"actions":[{"name":"test","type":"test"}]}`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, offset1, err := r.getWithOffset(contract, 150)
	if err != nil {
		t.Fatalf("getWithOffset(150) failed: %v", err)
	}

	_, offset2, err := r.getWithOffset(contract, 200)
	if err != nil {
		t.Fatalf("getWithOffset(200) failed: %v", err)
	}

	_, offset3, err := r.getWithOffset(contract, 500)
	if err != nil {
		t.Fatalf("getWithOffset(500) failed: %v", err)
	}

	if offset1 != offset2 || offset2 != offset3 {
		t.Errorf("offsets should be identical for same ABI: %d, %d, %d", offset1, offset2, offset3)
	}

	if offset1 < 0 {
		t.Errorf("offset should be non-negative, got %d", offset1)
	}
}

func TestParsedABICacheWithDecode(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("testdecode")
	actionName := chain.StringToName("myaction")

	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"myaction","fields":[{"name":"value","type":"uint64"}]}],"actions":[{"name":"myaction","type":"myaction"}]}`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	if len(r.parsedABIs) != 0 {
		t.Errorf("parsedABIs should start empty, got %d", len(r.parsedABIs))
	}

	actionData := make([]byte, 8)
	actionData[0] = 42
	_, err = r.Decode(contract, actionName, actionData, 150)
	if err != nil {
		t.Fatalf("First Decode failed: %v", err)
	}

	if len(r.parsedABIs) != 1 {
		t.Errorf("parsedABIs should have 1 entry after first Decode, got %d", len(r.parsedABIs))
	}

	_, err = r.Decode(contract, actionName, actionData, 200)
	if err != nil {
		t.Fatalf("Second Decode failed: %v", err)
	}

	if len(r.parsedABIs) != 1 {
		t.Errorf("parsedABIs should still have 1 entry (same offset), got %d", len(r.parsedABIs))
	}

	_, err = r.Decode(contract, actionName, actionData, 300)
	if err != nil {
		t.Fatalf("Third Decode failed: %v", err)
	}

	if len(r.parsedABIs) != 1 {
		t.Errorf("parsedABIs should still have 1 entry, got %d", len(r.parsedABIs))
	}
}

func TestParsedABICacheMultipleABIVersionsDecode(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("multiabi")
	actionName := chain.StringToName("act")

	abiV1 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"act","fields":[{"name":"x","type":"uint64"}]}],"actions":[{"name":"act","type":"act"}]}`)
	abiV2 := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"act","fields":[{"name":"x","type":"uint64"}]}],"actions":[{"name":"act","type":"act"}]}`)

	if err := w.Write(100, contract, abiV1); err != nil {
		t.Fatalf("Write v1 failed: %v", err)
	}
	if err := w.Write(200, contract, abiV2); err != nil {
		t.Fatalf("Write v2 failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	actionData := make([]byte, 8)
	actionData[0] = 99

	_, err = r.Decode(contract, actionName, actionData, 150)
	if err != nil {
		t.Fatalf("Decode at block 150 failed: %v", err)
	}

	if len(r.parsedABIs) != 1 {
		t.Errorf("should have 1 parsed ABI after first decode, got %d", len(r.parsedABIs))
	}

	_, err = r.Decode(contract, actionName, actionData, 250)
	if err != nil {
		t.Fatalf("Decode at block 250 failed: %v", err)
	}

	if len(r.parsedABIs) != 2 {
		t.Errorf("should have 2 parsed ABIs (different offsets), got %d", len(r.parsedABIs))
	}

	_, err = r.Decode(contract, actionName, actionData, 175)
	if err != nil {
		t.Fatalf("Decode at block 175 failed: %v", err)
	}

	if len(r.parsedABIs) != 2 {
		t.Errorf("should still have 2 parsed ABIs (reused v1), got %d", len(r.parsedABIs))
	}
}

func TestGetWithOffsetReturnsCorrectOffset(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract1 := chain.StringToName("first")
	contract2 := chain.StringToName("second")

	abi1 := []byte(`{"version":"eosio::abi/1.1","structs":[],"actions":[]}`)
	abi2 := []byte(`{"version":"eosio::abi/1.2","structs":[],"actions":[]}`)

	if err := w.Write(100, contract1, abi1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(100, contract2, abi2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, offset1First, err := r.getWithOffset(contract1, 200)
	if err != nil {
		t.Fatalf("getWithOffset contract1 failed: %v", err)
	}

	_, offset2First, err := r.getWithOffset(contract2, 200)
	if err != nil {
		t.Fatalf("getWithOffset contract2 failed: %v", err)
	}

	if offset1First == offset2First {
		t.Errorf("different contracts should have different offsets: %d vs %d", offset1First, offset2First)
	}

	_, offset1Again, err := r.getWithOffset(contract1, 200)
	if err != nil {
		t.Fatalf("getWithOffset contract1 again failed: %v", err)
	}

	_, offset2Again, err := r.getWithOffset(contract2, 200)
	if err != nil {
		t.Fatalf("getWithOffset contract2 again failed: %v", err)
	}

	if offset1First != offset1Again {
		t.Errorf("offset for contract1 should be consistent: %d vs %d", offset1First, offset1Again)
	}

	if offset2First != offset2Again {
		t.Errorf("offset for contract2 should be consistent: %d vs %d", offset2First, offset2Again)
	}
}

func TestErrorPathsReturnNegativeOffset(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	existingContract := chain.StringToName("exists")
	missingContract := chain.StringToName("missing")

	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"act","fields":[]}],"actions":[{"name":"act","type":"act"}]}`)

	if err := w.Write(100, existingContract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, offset, err := r.getWithOffset(missingContract, 200)
	if err == nil {
		t.Fatal("expected error for missing contract")
	}
	if offset != -1 {
		t.Errorf("error path should return offset -1, got %d", offset)
	}

	_, offset, err = r.getWithOffset(existingContract, 50)
	if err == nil {
		t.Fatal("expected error for block before ABI exists")
	}
	if offset != -1 {
		t.Errorf("error path should return offset -1, got %d", offset)
	}

	if len(r.parsedABIs) != 0 {
		t.Errorf("parsedABIs should be empty after errors, got %d", len(r.parsedABIs))
	}

	_, offset, err = r.getWithOffset(existingContract, 200)
	if err != nil {
		t.Fatalf("valid lookup failed: %v", err)
	}
	if offset < 0 {
		t.Errorf("valid lookup should return positive offset, got %d", offset)
	}
}

func TestDecodeFailsBeforeUsingNegativeOffset(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	existingContract := chain.StringToName("exists")
	missingContract := chain.StringToName("missing")
	actionName := chain.StringToName("act")

	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"act","fields":[{"name":"id","type":"uint64"}]}],"actions":[{"name":"act","type":"act"}]}`)

	if err := w.Write(100, existingContract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Decode(missingContract, actionName, []byte{}, 200)
	if err == nil {
		t.Fatal("Decode should fail for missing contract")
	}

	if len(r.parsedABIs) != 0 {
		t.Errorf("parsedABIs should be empty after failed Decode, got %d", len(r.parsedABIs))
	}

	_, ok := r.parsedABIs[-1]
	if ok {
		t.Error("parsedABIs should not have entry for offset -1")
	}

	actionData := make([]byte, 8)
	actionData[0] = 1
	_, err = r.Decode(existingContract, actionName, actionData, 200)
	if err != nil {
		t.Fatalf("Decode should succeed for existing contract: %v", err)
	}

	if len(r.parsedABIs) != 1 {
		t.Errorf("parsedABIs should have 1 entry after successful Decode, got %d", len(r.parsedABIs))
	}

	for offset := range r.parsedABIs {
		if offset < 0 {
			t.Errorf("parsedABIs should not contain negative offset, found %d", offset)
		}
	}
}

func TestLiveDataAfterWriterAppends(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract1 := chain.StringToName("first")
	contract2 := chain.StringToName("second")

	abi1 := []byte(`{"version":"eosio::abi/1.1","structs":[],"actions":[]}`)
	abi2 := []byte(`{"version":"eosio::abi/1.2","structs":[],"actions":[]}`)

	if err := w.Write(100, contract1, abi1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	got, err := r.Get(contract1, 200)
	if err != nil {
		t.Fatalf("Get contract1 failed: %v", err)
	}
	if string(got) != string(abi1) {
		t.Errorf("Get(contract1) mismatch")
	}

	_, err = r.Get(contract2, 200)
	if err == nil {
		t.Fatal("Get(contract2) should fail before it's written")
	}

	if err := w.Write(150, contract2, abi2); err != nil {
		t.Fatalf("Write contract2 failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	got, err = r.Get(contract2, 200)
	if err != nil {
		t.Fatalf("Get contract2 after append failed: %v", err)
	}
	if string(got) != string(abi2) {
		t.Errorf("Get(contract2) mismatch after append")
	}

	got, err = r.Get(contract1, 200)
	if err != nil {
		t.Fatalf("Get contract1 still works: %v", err)
	}
	if string(got) != string(abi1) {
		t.Errorf("Get(contract1) should still return correct ABI")
	}

	w.Close()
}

func TestLiveDataNewABIVersionForExistingContract(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("mycontract")

	abiV1 := []byte(`{"version":"eosio::abi/1.1","structs":[],"actions":[]}`)
	abiV2 := []byte(`{"version":"eosio::abi/1.2","structs":[],"actions":[]}`)

	if err := w.Write(100, contract, abiV1); err != nil {
		t.Fatalf("Write v1 failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	got, err := r.Get(contract, 150)
	if err != nil {
		t.Fatalf("Get at block 150 failed: %v", err)
	}
	if string(got) != string(abiV1) {
		t.Errorf("should get v1 at block 150")
	}

	got, err = r.Get(contract, 250)
	if err != nil {
		t.Fatalf("Get at block 250 failed: %v", err)
	}
	if string(got) != string(abiV1) {
		t.Errorf("should get v1 at block 250 (v2 not written yet)")
	}

	if err := w.Write(200, contract, abiV2); err != nil {
		t.Fatalf("Write v2 failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	got, err = r.Get(contract, 250)
	if err != nil {
		t.Fatalf("Get at block 250 after v2 written failed: %v", err)
	}
	if string(got) != string(abiV2) {
		t.Errorf("should get v2 at block 250 after append, got: %s", string(got))
	}

	got, err = r.Get(contract, 150)
	if err != nil {
		t.Fatalf("Get at block 150 should still work: %v", err)
	}
	if string(got) != string(abiV1) {
		t.Errorf("should still get v1 at block 150")
	}

	w.Close()
}

func TestDecodeHex(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("hextest")
	actionName := chain.StringToName("myaction")

	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"myaction","fields":[{"name":"value","type":"uint64"}]}],"actions":[{"name":"myaction","type":"myaction"}]}`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	result, err := r.DecodeHex(contract, actionName, "2a00000000000000", 150)
	if err != nil {
		t.Fatalf("DecodeHex failed: %v", err)
	}

	val, ok := result["value"]
	if !ok {
		t.Fatal("decoded result missing 'value' field")
	}
	switch v := val.(type) {
	case uint64:
		if v != 42 {
			t.Errorf("DecodeHex value = %d, want 42", v)
		}
	case float64:
		if v != 42 {
			t.Errorf("DecodeHex value = %f, want 42", v)
		}
	default:
		t.Logf("DecodeHex value type = %T, value = %v", val, val)
	}

	_, err = r.DecodeHex(contract, actionName, "invalid-hex!", 150)
	if err == nil {
		t.Error("DecodeHex should fail with invalid hex")
	}

	_, err = r.DecodeHex(contract, actionName, "zzzz", 150)
	if err == nil {
		t.Error("DecodeHex should fail with non-hex characters")
	}
}

func TestReadVarUint32(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		wantValue uint32
		wantBytes int
	}{
		{"single byte zero", []byte{0x00}, 0, 1},
		{"single byte 1", []byte{0x01}, 1, 1},
		{"single byte 127", []byte{0x7f}, 127, 1},
		{"two bytes 128", []byte{0x80, 0x01}, 128, 2},
		{"two bytes 255", []byte{0xff, 0x01}, 255, 2},
		{"two bytes 300", []byte{0xac, 0x02}, 300, 2},
		{"three bytes", []byte{0x80, 0x80, 0x01}, 16384, 3},
		{"empty input", []byte{}, 0, 0},
		{"truncated varint", []byte{0x80}, 0, 0},
		{"truncated varint 2", []byte{0x80, 0x80}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotBytes := readVarUint32(tt.input)
			if gotValue != tt.wantValue {
				t.Errorf("readVarUint32(%v) value = %d, want %d", tt.input, gotValue, tt.wantValue)
			}
			if gotBytes != tt.wantBytes {
				t.Errorf("readVarUint32(%v) bytes = %d, want %d", tt.input, gotBytes, tt.wantBytes)
			}
		})
	}
}

func TestParseSetabi(t *testing.T) {
	t.Run("data too short", func(t *testing.T) {
		_, _, err := ParseSetabi([]byte{0x01, 0x02, 0x03})
		if err == nil {
			t.Error("expected error for short data")
		}
	})

	t.Run("missing ABI length", func(t *testing.T) {
		data := make([]byte, 8)
		_, _, err := ParseSetabi(data)
		if err == nil {
			t.Error("expected error for missing ABI length")
		}
	})

	t.Run("could not read ABI length (truncated varint)", func(t *testing.T) {
		data := make([]byte, 9)
		data[8] = 0x80
		_, _, err := ParseSetabi(data)
		if err == nil {
			t.Error("expected error for truncated varint")
		}
	})

	t.Run("ABI length exceeds data", func(t *testing.T) {
		data := make([]byte, 10)
		data[8] = 0xFF
		data[9] = 0x01
		_, _, err := ParseSetabi(data)
		if err == nil {
			t.Error("expected error when ABI length exceeds data")
		}
	})

	t.Run("zero length ABI (clear ABI)", func(t *testing.T) {
		data := make([]byte, 9)
		contract := chain.StringToName("testcontract")
		data[0] = byte(contract)
		data[1] = byte(contract >> 8)
		data[2] = byte(contract >> 16)
		data[3] = byte(contract >> 24)
		data[4] = byte(contract >> 32)
		data[5] = byte(contract >> 40)
		data[6] = byte(contract >> 48)
		data[7] = byte(contract >> 56)
		data[8] = 0x00

		gotContract, gotABI, err := ParseSetabi(data)
		if err != nil {
			t.Fatalf("ParseSetabi failed: %v", err)
		}
		if gotContract != contract {
			t.Errorf("contract = %d, want %d", gotContract, contract)
		}
		if gotABI != nil {
			t.Errorf("ABI should be nil for zero-length, got %v", gotABI)
		}
	})

	t.Run("invalid binary ABI", func(t *testing.T) {
		data := make([]byte, 15)
		contract := chain.StringToName("badabi")
		data[0] = byte(contract)
		data[1] = byte(contract >> 8)
		data[2] = byte(contract >> 16)
		data[3] = byte(contract >> 24)
		data[4] = byte(contract >> 32)
		data[5] = byte(contract >> 40)
		data[6] = byte(contract >> 48)
		data[7] = byte(contract >> 56)
		data[8] = 0x05
		data[9] = 0xFF
		data[10] = 0xFF
		data[11] = 0xFF
		data[12] = 0xFF
		data[13] = 0xFF

		_, _, err := ParseSetabi(data)
		if err == nil {
			t.Error("expected error for invalid binary ABI")
		}
	})
}

func TestNewReaderErrors(t *testing.T) {
	t.Run("missing data file", func(t *testing.T) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "abis.index"), []byte{}, 0644)

		_, err := NewReader(dir)
		if err == nil {
			t.Error("expected error when data file missing")
		}
	})

	t.Run("missing index file", func(t *testing.T) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "abis.dat"), []byte{}, 0644)

		_, err := NewReader(dir)
		if err == nil {
			t.Error("expected error when index file missing")
		}
	})

	t.Run("corrupted index file (wrong size)", func(t *testing.T) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "abis.dat"), []byte{}, 0644)
		os.WriteFile(filepath.Join(dir, "abis.index"), []byte{0x01, 0x02, 0x03}, 0644)

		_, err := NewReader(dir)
		if err == nil {
			t.Error("expected error for corrupted index (size not multiple of 20)")
		}
	})

	t.Run("non-existent directory", func(t *testing.T) {
		_, err := NewReader("/nonexistent/path/to/abicache")
		if err == nil {
			t.Error("expected error for non-existent directory")
		}
	})
}

func TestNewWriterErrors(t *testing.T) {
	t.Run("cannot create directory (file exists with same name)", func(t *testing.T) {
		dir := t.TempDir()
		filePath := filepath.Join(dir, "notadir")
		os.WriteFile(filePath, []byte("i am a file"), 0644)

		_, err := NewWriter(filepath.Join(filePath, "subdir"))
		if err == nil {
			t.Error("expected error when cannot create directory")
		}
	})
}

func TestEmptyIndexReader(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "abis.dat"), []byte{}, 0644)
	os.WriteFile(filepath.Join(dir, "abis.index"), []byte{}, 0644)

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Get(chain.StringToName("any"), 100)
	if err == nil {
		t.Error("expected error when index is empty")
	}
}

func TestClearedABI(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("cleared")

	if err := w.Write(100, contract, []byte{}); err != nil {
		t.Fatalf("Write empty ABI failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Get(contract, 200)
	if err == nil {
		t.Error("expected error for cleared ABI (zero length)")
	}
}

func TestDecodeNotAMap(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("notmap")
	actionName := chain.StringToName("primitive")

	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"primitive","fields":[]}],"actions":[{"name":"primitive","type":"uint64"}]}`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	data := make([]byte, 8)
	_, err = r.Decode(contract, actionName, data, 150)
	if err == nil {
		t.Error("expected error when decoded result is not a map")
	}
}

func TestDecodeInvalidABIJSON(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("badjson")
	actionName := chain.StringToName("act")

	abi := []byte(`{"version":"eosio::abi/1.1", invalid json here`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Decode(contract, actionName, []byte{}, 150)
	if err == nil {
		t.Error("expected error for invalid ABI JSON")
	}
}

func TestDecodeActionFails(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("badaction")
	actionName := chain.StringToName("nonexistent")

	abi := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"real","fields":[{"name":"x","type":"uint64"}]}],"actions":[{"name":"real","type":"real"}]}`)

	if err := w.Write(100, contract, abi); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Decode(contract, actionName, []byte{0x01}, 150)
	if err == nil {
		t.Error("expected error for non-existent action")
	}
}

func TestParseSetabiWithValidBinaryABI(t *testing.T) {
	binaryABI := []byte{
		0x0e, 0x65, 0x6f, 0x73, 0x69, 0x6f, 0x3a, 0x3a, 0x61, 0x62, 0x69, 0x2f, 0x31, 0x2e, 0x31,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
	}

	contract := chain.StringToName("testabi")
	data := make([]byte, 8+1+len(binaryABI))
	data[0] = byte(contract)
	data[1] = byte(contract >> 8)
	data[2] = byte(contract >> 16)
	data[3] = byte(contract >> 24)
	data[4] = byte(contract >> 32)
	data[5] = byte(contract >> 40)
	data[6] = byte(contract >> 48)
	data[7] = byte(contract >> 56)
	data[8] = byte(len(binaryABI))
	copy(data[9:], binaryABI)

	gotContract, gotABI, err := ParseSetabi(data)
	if err != nil {
		t.Fatalf("ParseSetabi failed: %v", err)
	}
	if gotContract != contract {
		t.Errorf("contract = %d, want %d", gotContract, contract)
	}
	if gotABI == nil {
		t.Error("expected non-nil ABI JSON")
	}
	if len(gotABI) == 0 {
		t.Error("expected non-empty ABI JSON")
	}
}

func TestReaderReadHeaderError(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("truncated")
	if err := w.Write(100, contract, []byte(`{"test":true}`)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	dataPath := filepath.Join(dir, "abis.dat")
	if err := os.Truncate(dataPath, 10); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Get(contract, 200)
	if err == nil {
		t.Error("expected error when reading truncated header")
	}
}

func TestReaderReadDataError(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract := chain.StringToName("truncdata")
	longABI := []byte(`{"version":"eosio::abi/1.1","structs":[{"name":"test","fields":[{"name":"x","type":"uint64"}]}],"actions":[]}`)
	if err := w.Write(100, contract, longABI); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	dataPath := filepath.Join(dir, "abis.dat")
	if err := os.Truncate(dataPath, 20); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Get(contract, 200)
	if err == nil {
		t.Error("expected error when reading truncated data")
	}
}

func TestIndexGrowthDuringRead(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	contract1 := chain.StringToName("first")
	abi1 := []byte(`{"version":"eosio::abi/1.1","structs":[],"actions":[]}`)

	if err := w.Write(100, contract1, abi1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	got, err := r.Get(contract1, 200)
	if err != nil {
		t.Fatalf("Get contract1 failed: %v", err)
	}
	if string(got) != string(abi1) {
		t.Errorf("Get(contract1) mismatch")
	}

	contract2 := chain.StringToName("second")
	abi2 := []byte(`{"version":"eosio::abi/1.2","structs":[],"actions":[]}`)
	if err := w.Write(150, contract2, abi2); err != nil {
		t.Fatalf("Write contract2 failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	got, err = r.Get(contract2, 200)
	if err != nil {
		t.Fatalf("Get contract2 after index growth failed: %v", err)
	}
	if string(got) != string(abi2) {
		t.Errorf("Get(contract2) mismatch")
	}

	contract3 := chain.StringToName("third")
	abi3 := []byte(`{"version":"eosio::abi/1.3","structs":[],"actions":[]}`)
	if err := w.Write(175, contract3, abi3); err != nil {
		t.Fatalf("Write contract3 failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	got, err = r.Get(contract3, 200)
	if err != nil {
		t.Fatalf("Get contract3 failed: %v", err)
	}
	if string(got) != string(abi3) {
		t.Errorf("Get(contract3) mismatch")
	}

	w.Close()
}
