package encoding

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
)

// =============================================================================
// Varint Tests
// =============================================================================

func TestVarintRoundTrip(t *testing.T) {
	testCases := []int64{
		0,
		1,
		-1,
		127,
		-127,
		128,
		-128,
		math.MaxInt32,
		math.MinInt32,
		math.MaxInt64,
		math.MinInt64,
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		PutAsVarint(&buf, tc)

		reader := bytes.NewReader(buf.Bytes())
		result := GetAsVarint(reader)

		if result != tc {
			t.Errorf("Varint roundtrip failed: got %d, want %d", result, tc)
		}
	}
}

func TestUVarintRoundTrip(t *testing.T) {
	testCases := []uint64{
		0,
		1,
		127,
		128,
		255,
		256,
		math.MaxUint32,
		math.MaxUint64,
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		PutAsUVarint(&buf, tc)

		reader := bytes.NewReader(buf.Bytes())
		result := GetAsUVarint(reader)

		if result != tc {
			t.Errorf("UVarint roundtrip failed: got %d, want %d", result, tc)
		}
	}
}

// =============================================================================
// JSON Tests
// =============================================================================

func TestMaybeGetInt64FromJSONNumber(t *testing.T) {
	num := json.Number("12345")
	result, ok := MaybeGetInt64(num)
	if !ok || result != 12345 {
		t.Errorf("Expected (12345, true), got (%d, %v)", result, ok)
	}
}

func TestMaybeGetInt64FromString(t *testing.T) {
	result, ok := MaybeGetInt64("67890")
	if !ok || result != 67890 {
		t.Errorf("Expected (67890, true), got (%d, %v)", result, ok)
	}
}

func TestMaybeGetInt64FromInt64(t *testing.T) {
	result, ok := MaybeGetInt64(int64(999))
	if !ok || result != 999 {
		t.Errorf("Expected (999, true), got (%d, %v)", result, ok)
	}
}

func TestMaybeGetInt64InvalidType(t *testing.T) {
	_, ok := MaybeGetInt64(3.14)
	if ok {
		t.Error("Expected false for invalid type")
	}
}

func TestJSONiterConfig(t *testing.T) {
	if JSONiter == nil {
		t.Fatal("JSONiter is nil")
	}

	data := map[string]interface{}{
		"foo": "bar",
		"num": 42,
	}

	jsonBytes, err := JSONiter.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var result map[string]interface{}
	err = JSONiter.Unmarshal(jsonBytes, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if result["foo"] != "bar" {
		t.Errorf("Expected foo=bar, got %v", result["foo"])
	}
}

// =============================================================================
// MaybeGetInt64 Edge Cases
// =============================================================================

func TestMaybeGetInt64_JSONNumber_Invalid(t *testing.T) {
	invalidNum := json.Number("not_a_number")
	result, ok := MaybeGetInt64(invalidNum)

	if ok {
		t.Errorf("Expected ok=false for invalid json.Number, got ok=true, result=%d", result)
	}

	if result != 0 {
		t.Errorf("Expected result=0 for invalid json.Number, got %d", result)
	}
}

func TestMaybeGetInt64_JSONNumber_Overflow(t *testing.T) {
	overflowNum := json.Number("99999999999999999999999999")
	result, ok := MaybeGetInt64(overflowNum)

	if ok {
		t.Errorf("Expected ok=false for overflow json.Number, got ok=true, result=%d", result)
	}
}

func TestMaybeGetInt64_JSONNumber_Float(t *testing.T) {
	floatNum := json.Number("123.45")
	result, ok := MaybeGetInt64(floatNum)

	if ok {
		t.Errorf("Expected ok=false for float json.Number, got ok=true, result=%d", result)
	}
}

func TestMaybeGetInt64_String_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"non-numeric", "not_a_number"},
		{"empty", ""},
		{"float", "3.14"},
		{"with spaces", " 123 "},
		{"hex format", "0x123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := MaybeGetInt64(tt.input)

			if ok {
				t.Errorf("Expected ok=false for %q, got ok=true, result=%d", tt.input, result)
			}
		})
	}
}

func TestMaybeGetInt64_String_Overflow(t *testing.T) {
	overflowInputs := []string{
		"99999999999999999999999999",
		"-99999999999999999999999999",
	}

	for _, input := range overflowInputs {
		t.Run(input, func(t *testing.T) {
			_, ok := MaybeGetInt64(input)
			if ok {
				t.Errorf("Expected ok=false for overflow input %q", input)
			}
		})
	}
}

func TestMaybeGetInt64_InvalidTypes(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"float64", float64(3.14)},
		{"float32", float32(2.71)},
		{"int", int(42)},
		{"int32", int32(100)},
		{"uint64", uint64(200)},
		{"bool", true},
		{"nil", nil},
		{"slice", []int{1, 2, 3}},
		{"map", map[string]int{"a": 1}},
		{"struct", struct{ X int }{X: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := MaybeGetInt64(tt.input)

			if ok {
				t.Errorf("Expected ok=false for type %T, got ok=true, result=%d", tt.input, result)
			}

			if result != 0 {
				t.Errorf("Expected result=0 for type %T, got %d", tt.input, result)
			}
		})
	}
}

func TestMaybeGetInt64_AllSuccessCases(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  int64
	}{
		{"json.Number positive", json.Number("12345"), 12345},
		{"json.Number negative", json.Number("-9876"), -9876},
		{"json.Number zero", json.Number("0"), 0},
		{"string positive", "54321", 54321},
		{"string negative", "-11111", -11111},
		{"string zero", "0", 0},
		{"int64 positive", int64(999), 999},
		{"int64 negative", int64(-888), -888},
		{"int64 zero", int64(0), 0},
		{"int64 max", int64(9223372036854775807), 9223372036854775807},
		{"int64 min", int64(-9223372036854775808), -9223372036854775808},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := MaybeGetInt64(tt.input)

			if !ok {
				t.Errorf("Expected ok=true for %v, got ok=false", tt.input)
				return
			}

			if result != tt.want {
				t.Errorf("MaybeGetInt64(%v) = %d, want %d", tt.input, result, tt.want)
			}
		})
	}
}

// =============================================================================
// JSONiter Integration Tests
// =============================================================================

func TestJSONiter_UseNumber(t *testing.T) {
	jsonData := `{"value": 12345678901234567890}`

	var result map[string]interface{}
	err := JSONiter.Unmarshal([]byte(jsonData), &result)
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	num, ok := result["value"].(json.Number)
	if !ok {
		t.Errorf("UseNumber not working: got type %T, want json.Number", result["value"])
	} else {
		expected := "12345678901234567890"
		if string(num) != expected {
			t.Errorf("Number value incorrect: got %s, want %s", num, expected)
		}
	}
}
