package chain

import (
	"testing"
)

func TestSymbolCodeToString(t *testing.T) {
	tests := []struct {
		code uint64
		want string
	}{
		{0, ""},
		{0x4D45544F54, "TOTEM"}, // "TOTEM" = T(0x54), O(0x4F), T(0x54), E(0x45), M(0x4D)
		{0x4F4F46, "FOO"},       // "FOO" = F(0x46), O(0x4F), O(0x4F)
		{0x534F45, "EOS"},       // "EOS" = E(0x45), O(0x4F), S(0x53)
	}

	for _, tc := range tests {
		got := SymbolCodeToString(tc.code)
		if got != tc.want {
			t.Errorf("SymbolCodeToString(%x) = %q, want %q", tc.code, got, tc.want)
		}
	}
}

func TestStringToSymbolCode(t *testing.T) {
	tests := []struct {
		name string
		want uint64
	}{
		{"", 0},
		{"TOTEM", 0x4D45544F54},
		{"FOO", 0x4F4F46},
		{"EOS", 0x534F45},
	}

	for _, tc := range tests {
		got := StringToSymbolCode(tc.name)
		if got != tc.want {
			t.Errorf("StringToSymbolCode(%q) = %x, want %x", tc.name, got, tc.want)
		}
	}
}

func TestSymbolRoundTrip(t *testing.T) {
	names := []string{"TOTEM", "EOS", "WAX", "A", "ABCDEFG"}
	for _, name := range names {
		code := StringToSymbolCode(name)
		got := SymbolCodeToString(code)
		if got != name {
			t.Errorf("Round trip failed for %q: got %q", name, got)
		}
	}
}

func TestSymbolPrecision(t *testing.T) {
	// Symbol with precision 4 and code "FOO"
	symbol := NewSymbolFromString(4, "FOO")
	if got := SymbolPrecision(symbol); got != 4 {
		t.Errorf("SymbolPrecision() = %d, want 4", got)
	}

	// Precision 8
	symbol8 := NewSymbolFromString(8, "WAX")
	if got := SymbolPrecision(symbol8); got != 8 {
		t.Errorf("SymbolPrecision() = %d, want 8", got)
	}

	// Precision 0
	symbol0 := NewSymbolFromString(0, "NFT")
	if got := SymbolPrecision(symbol0); got != 0 {
		t.Errorf("SymbolPrecision() = %d, want 0", got)
	}
}

func TestAssetString(t *testing.T) {
	tests := []struct {
		amount    int64
		precision uint8
		symbol    string
		want      string
	}{
		{1000000, 4, "TOTEM", "100.0000 TOTEM"},
		{12345, 4, "FOO", "1.2345 FOO"},
		{100, 0, "NFT", "100 NFT"},
		{1, 4, "EOS", "0.0001 EOS"},
		{0, 4, "EOS", "0.0000 EOS"},
		{-12345, 4, "NEGS", "-1.2345 NEGS"},
		{-1, 4, "NEGS", "-0.0001 NEGS"},
		{99999999999, 0, "BIG", "99999999999 BIG"},
		{123456789012, 8, "WAX", "1234.56789012 WAX"},
	}

	for _, tc := range tests {
		asset := Asset{
			Amount: tc.amount,
			Symbol: NewSymbolFromString(tc.precision, tc.symbol),
		}
		got := asset.String()
		if got != tc.want {
			t.Errorf("Asset{%d, %d, %s}.String() = %q, want %q",
				tc.amount, tc.precision, tc.symbol, got, tc.want)
		}
	}
}

func TestParseAsset(t *testing.T) {
	tests := []struct {
		input    string
		wantAmt  int64
		wantPrec uint8
		wantCode string
		wantErr  bool
	}{
		{"100.0000 TOTEM", 1000000, 4, "TOTEM", false},
		{"1.2345 FOO", 12345, 4, "FOO", false},
		{"100 NFT", 100, 0, "NFT", false},
		{"0.0001 EOS", 1, 4, "EOS", false},
		{"-1.2345 NEGS", -12345, 4, "NEGS", false},
		{"0.0000 EOS", 0, 4, "EOS", false},
		{"invalid", 0, 0, "", true},
		{"no_space_symbol", 0, 0, "", true},
	}

	for _, tc := range tests {
		got, err := ParseAsset(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseAsset(%q) expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAsset(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if got.Amount != tc.wantAmt {
			t.Errorf("ParseAsset(%q).Amount = %d, want %d", tc.input, got.Amount, tc.wantAmt)
		}
		if SymbolPrecision(got.Symbol) != tc.wantPrec {
			t.Errorf("ParseAsset(%q) precision = %d, want %d", tc.input, SymbolPrecision(got.Symbol), tc.wantPrec)
		}
		if SymbolCodeToString(SymbolCode(got.Symbol)) != tc.wantCode {
			t.Errorf("ParseAsset(%q) code = %q, want %q", tc.input, SymbolCodeToString(SymbolCode(got.Symbol)), tc.wantCode)
		}
	}
}

func TestAssetRoundTrip(t *testing.T) {
	inputs := []string{
		"100.0000 TOTEM",
		"1.2345 FOO",
		"0.0001 EOS",
		"-99.9999 NEGS",
		"1000000 NFT",
	}

	for _, input := range inputs {
		asset, err := ParseAsset(input)
		if err != nil {
			t.Errorf("ParseAsset(%q) error: %v", input, err)
			continue
		}
		got := asset.String()
		if got != input {
			t.Errorf("Round trip failed: %q -> %q", input, got)
		}
	}
}

func TestNewAsset(t *testing.T) {
	symbol := NewSymbolFromString(4, "EOS")
	asset := NewAsset(10000, symbol)

	if asset.Amount != 10000 {
		t.Errorf("NewAsset amount = %d; want 10000", asset.Amount)
	}
	if asset.Symbol != symbol {
		t.Errorf("NewAsset symbol = %d; want %d", asset.Symbol, symbol)
	}

	got := asset.String()
	if got != "1.0000 EOS" {
		t.Errorf("NewAsset(10000, EOS).String() = %q; want %q", got, "1.0000 EOS")
	}
}

func TestParseAssetInvalidAmount(t *testing.T) {
	_, err := ParseAsset("not_a_number SYS")
	if err == nil {
		t.Error("ParseAsset with invalid amount should return error")
	}
}
