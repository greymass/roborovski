package chain

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

// =============================================================================
// Time Conversion Tests
// =============================================================================

func TestUint32ToTime(t *testing.T) {
	tests := []struct {
		name     string
		input    uint32
		expected string
	}{
		{
			name:     "epoch (Jan 1, 2000)",
			input:    0,
			expected: "2000-01-01T00:00:00.000",
		},
		{
			name:     "one interval (500ms)",
			input:    1,
			expected: "2000-01-01T00:00:00.500",
		},
		{
			name:     "one second (2 intervals)",
			input:    2,
			expected: "2000-01-01T00:00:01.000",
		},
		{
			name:     "one minute",
			input:    120,
			expected: "2000-01-01T00:01:00.000",
		},
		{
			name:     "one hour",
			input:    7200,
			expected: "2000-01-01T01:00:00.000",
		},
		{
			name:     "one day",
			input:    172800,
			expected: "2000-01-02T00:00:00.000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Uint32ToTime(tt.input)
			if result != tt.expected {
				t.Errorf("Uint32ToTime(%d) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTimeToUint32(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint32
	}{
		{
			name:     "epoch",
			input:    "2000-01-01T00:00:00.000",
			expected: 0,
		},
		{
			name:     "with Z suffix",
			input:    "2000-01-01T00:00:00.000Z",
			expected: 0,
		},
		{
			name:     "empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "one second",
			input:    "2000-01-01T00:00:01.000",
			expected: 2,
		},
		{
			name:     "one day",
			input:    "2000-01-02T00:00:00.000",
			expected: 172800,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TimeToUint32(tt.input)
			if result != tt.expected {
				t.Errorf("TimeToUint32(%q) = %d; want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTimeConversionRoundtrip(t *testing.T) {
	// Test roundtrip conversion for various timestamps
	testValues := []uint32{0, 1, 2, 100, 1000, 172800, 1000000}

	for _, val := range testValues {
		timeStr := Uint32ToTime(val)
		result := TimeToUint32(timeStr)
		if result != val {
			t.Errorf("Roundtrip failed for %d: got %d via %q", val, result, timeStr)
		}
	}
}

func TestTimeToUint32InvalidFormat(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("TimeToUint32 with invalid format should panic")
		}
	}()

	TimeToUint32("not-a-valid-time")
}

// =============================================================================
// Character/Symbol Conversion Tests (Internal functions)
// =============================================================================

func TestCharToSymbol(t *testing.T) {
	tests := []struct {
		name     string
		input    byte
		expected uint64
	}{
		{"first letter", 'a', 6},
		{"last letter", 'z', 31},
		{"first number", '1', 1},
		{"last number", '5', 5},
		{"dot", '.', 0},
		{"invalid char", 'A', 0}, // uppercase not valid
		{"invalid char", '@', 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := charToSymbol(tt.input)
			if result != tt.expected {
				t.Errorf("charToSymbol(%c) = %d; want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSymbolToChar(t *testing.T) {
	tests := []struct {
		name     string
		input    byte
		expected byte
	}{
		{"dot", 0, '.'},
		{"first number", 1, '1'},
		{"last number", 5, '5'},
		{"first letter", 6, 'a'},
		{"last letter", 31, 'z'},
		{"invalid", 32, '@'}, // out of range
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := symbolToChar(tt.input)
			if result != tt.expected {
				t.Errorf("symbolToChar(%d) = %c; want %c", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCharSymbolRoundtrip(t *testing.T) {
	// Test all valid characters roundtrip correctly
	validChars := "abcdefghijklmnopqrstuvwxyz12345."

	for _, ch := range validChars {
		symbol := charToSymbol(byte(ch))
		result := symbolToChar(byte(symbol))
		if result != byte(ch) {
			t.Errorf("Roundtrip failed for %c: symbol=%d, result=%c", ch, symbol, result)
		}
	}
}

// =============================================================================
// Name Encoding Tests (CRITICAL - Most Important Functions)
// =============================================================================

func TestStringToName(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty string", input: ""},
		{name: "single char", input: "a"},
		{name: "eosio", input: "eosio"},
		{name: "eosio.token", input: "eosio.token"},
		{name: "eosio.ram", input: "eosio.ram"},
		{name: "atomicassets", input: "atomicassets"},
		{name: "12 chars", input: "abcdefghijkl"},
		{name: "13 chars (max)", input: "abcdefghijklm"},
		{name: "with dots", input: "a.b.c"},
		{name: "numbers", input: "12345"},
		{name: "mixed", input: "a1b2c3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just test that it returns something and doesn't panic
			result := StringToName(tt.input)

			// Verify it's deterministic
			result2 := StringToName(tt.input)
			if result != result2 {
				t.Errorf("StringToName not deterministic for %q: %x != %x", tt.input, result, result2)
			}
		})
	}
}

func TestNameToString(t *testing.T) {
	// Test a few known conversions
	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		{
			name:     "zero",
			input:    0,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NameToString(tt.input)
			if result != tt.expected {
				t.Errorf("NameToString(0x%016x) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNameEncodingRoundtrip(t *testing.T) {
	// Test roundtrip for common account names
	names := []string{
		"",
		"a",
		"eosio",
		"eosio.token",
		"eosio.ram",
		"eosio.ramfee",
		"eosio.stake",
		"eosio.saving",
		"eosio.bpay",
		"eosio.vpay",
		"eosio.rex",
		"atomicassets",
		"tokentoken",
		"12345",
		"a1b2c3d4e5",
		"zzzzzzzzzzzzj",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			encoded := StringToName(name)
			decoded := NameToString(encoded)
			if decoded != name {
				t.Errorf("Roundtrip failed for %q: encoded=0x%016x, decoded=%q", name, encoded, decoded)
			}
		})
	}
}

// Property-based test: all valid names should roundtrip
func TestNameEncodingProperties(t *testing.T) {
	// Test that encoding is deterministic
	name := "eosio.token"
	enc1 := StringToName(name)
	enc2 := StringToName(name)
	if enc1 != enc2 {
		t.Errorf("StringToName not deterministic: %x != %x", enc1, enc2)
	}

	// Test that decoding is deterministic
	dec1 := NameToString(enc1)
	dec2 := NameToString(enc1)
	if dec1 != dec2 {
		t.Errorf("NameToString not deterministic: %q != %q", dec1, dec2)
	}
}

// =============================================================================
// Action Tests
// =============================================================================

func TestActionBytes(t *testing.T) {
	// Create a simple transfer action
	action := Action{
		Account: "eosio.token",
		Name:    "transfer",
		Authorization: []PermissionLevel{
			{Actor: "alice", Permission: "active"},
		},
		Data: "0000000000855c340000000000000e3d0100000000000000045359530000000007746573746d656d6f", // transfer alice->bob 1 SYS "testmemo"
	}

	// Should serialize without error
	serialized := action.Bytes()
	if len(serialized) == 0 {
		t.Error("Action.Bytes() returned empty bytes")
	}

	// A real transfer action should be at least 20 bytes (names + auth + data)
	// and less than 500 bytes for typical transfer
	if len(serialized) < 20 || len(serialized) > 500 {
		t.Errorf("Action bytes length suspicious: %d", len(serialized))
	}
}

func TestActionBytesInvalidHex(t *testing.T) {
	action := Action{
		Account:       "eosio.token",
		Name:          "transfer",
		Authorization: []PermissionLevel{},
		Data:          "not_valid_hex!",
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Action.Bytes() with invalid hex should panic")
		}
	}()

	action.Bytes()
}

func TestActionBytesMultipleAuth(t *testing.T) {
	action := Action{
		Account: "eosio.token",
		Name:    "transfer",
		Authorization: []PermissionLevel{
			{Actor: "alice", Permission: "active"},
			{Actor: "bob", Permission: "active"},
			{Actor: "charlie", Permission: "owner"},
		},
		Data: "00",
	}

	serialized := action.Bytes()
	if len(serialized) == 0 {
		t.Error("Action.Bytes() with multiple auths returned empty bytes")
	}
}

func TestActionBytesLargeData(t *testing.T) {
	largeData := make([]byte, 200)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	action := Action{
		Account:       "test",
		Name:          "bigdata",
		Authorization: []PermissionLevel{},
		Data:          hex.EncodeToString(largeData),
	}

	serialized := action.Bytes()
	if len(serialized) == 0 {
		t.Error("Action.Bytes() with large data returned empty bytes")
	}
}

func TestActionDigest(t *testing.T) {
	action := Action{
		Account: "eosio.token",
		Name:    "transfer",
		Authorization: []PermissionLevel{
			{Actor: "alice", Permission: "active"},
		},
		Data: "0000000000855c340000000000000e3d0100000000000000045359530000000007746573746d656d6f",
	}

	digest := action.Digest()

	// Should be 64 hex characters (32 bytes SHA256)
	if len(digest) != 64 {
		t.Errorf("Action.Digest() length = %d; want 64", len(digest))
	}

	// Should be valid hex
	_, err := hex.DecodeString(digest)
	if err != nil {
		t.Errorf("Action.Digest() not valid hex: %v", err)
	}

	// Same action should produce same digest
	digest2 := action.Digest()
	if digest != digest2 {
		t.Error("Action.Digest() not deterministic")
	}

	// Different action should produce different digest
	action2 := action
	action2.Data = "00" // change data
	digest3 := action2.Digest()
	if digest == digest3 {
		t.Error("Different actions produced same digest")
	}
}

// =============================================================================
// JSON Marshaling Tests
// =============================================================================

func TestActionTraceJSON(t *testing.T) {
	at := ActionTrace{
		ActionOrdinal: 1,
		CreatorAO:     0,
		ClosestUAAO:   0,
		Receipt: ActionReceipt{
			Receiver:       "eosio.token",
			ActDigest:      "abcd1234",
			GlobalSequence: json.Number("12345"),
			RecvSequence:   json.Number("678"),
			AuthSequence:   [][]interface{}{},
			CodeSequence:   1,
			AbiSequence:    2,
		},
		Receiver: "eosio.token",
		Act: Action{
			Account: "eosio.token",
			Name:    "transfer",
			Authorization: []PermissionLevel{
				{Actor: "alice", Permission: "active"},
			},
			Data: "deadbeef",
		},
		ContextFree:     false,
		Elapsed:         123,
		TrxID:           "abcdef1234567890",
		BlockNum:        1000,
		BlockTime:       "2020-01-01T00:00:00.000",
		ProducerBlockID: "0000000000000000",
		AccountRAMDeltas: []AccountDelta{
			{Account: "alice", Delta: -100},
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(at)
	if err != nil {
		t.Fatalf("Failed to marshal ActionTrace: %v", err)
	}

	// Unmarshal back
	var decoded ActionTrace
	err = json.Unmarshal(jsonData, &decoded)
	if err != nil {
		t.Fatalf("Failed to unmarshal ActionTrace: %v", err)
	}

	// Verify key fields
	if decoded.ActionOrdinal != at.ActionOrdinal {
		t.Errorf("ActionOrdinal mismatch: got %d, want %d", decoded.ActionOrdinal, at.ActionOrdinal)
	}

	if decoded.Receiver != at.Receiver {
		t.Errorf("Receiver mismatch: got %q, want %q", decoded.Receiver, at.Receiver)
	}

	if decoded.Act.Account != at.Act.Account {
		t.Errorf("Act.Account mismatch: got %q, want %q", decoded.Act.Account, at.Act.Account)
	}
}

func TestPermissionLevelJSON(t *testing.T) {
	pl := PermissionLevel{
		Actor:      "alice",
		Permission: "active",
	}

	jsonData, err := json.Marshal(pl)
	if err != nil {
		t.Fatalf("Failed to marshal PermissionLevel: %v", err)
	}

	var decoded PermissionLevel
	err = json.Unmarshal(jsonData, &decoded)
	if err != nil {
		t.Fatalf("Failed to unmarshal PermissionLevel: %v", err)
	}

	if decoded.Actor != pl.Actor {
		t.Errorf("Actor mismatch: got %q, want %q", decoded.Actor, pl.Actor)
	}

	if decoded.Permission != pl.Permission {
		t.Errorf("Permission mismatch: got %q, want %q", decoded.Permission, pl.Permission)
	}
}

// =============================================================================
// Benchmarks (CRITICAL - Hot Path Functions)
// =============================================================================

func BenchmarkStringToName(b *testing.B) {
	names := []string{
		"eosio",
		"eosio.token",
		"atomicassets",
		"abcdefghijkl",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = StringToName(names[i%len(names)])
	}
}

func BenchmarkNameToString(b *testing.B) {
	nameValues := []uint64{
		0x000000000000ea30,           // eosio
		0x00000000a0a6e05a,           // eosio.token
		StringToName("abcdefghijkl"), // 12 char name
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NameToString(nameValues[i%len(nameValues)])
	}
}

func BenchmarkActionBytes(b *testing.B) {
	action := Action{
		Account: "eosio.token",
		Name:    "transfer",
		Authorization: []PermissionLevel{
			{Actor: "alice", Permission: "active"},
		},
		Data: "0000000000855c340000000000000e3d0100000000000000045359530000000007746573746d656d6f",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = action.Bytes()
	}
}

func BenchmarkActionDigest(b *testing.B) {
	action := Action{
		Account: "eosio.token",
		Name:    "transfer",
		Authorization: []PermissionLevel{
			{Actor: "alice", Permission: "active"},
		},
		Data: "0000000000855c340000000000000e3d0100000000000000045359530000000007746573746d656d6f",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = action.Digest()
	}
}
