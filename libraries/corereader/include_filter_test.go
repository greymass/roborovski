package corereader

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

func TestNewIncludeFilter_Empty(t *testing.T) {
	f := NewIncludeFilter(nil)
	if f == nil {
		t.Fatal("NewIncludeFilter(nil) returned nil")
	}
	if !f.IsEmpty() {
		t.Error("filter should be empty with nil input")
	}

	f = NewIncludeFilter([]string{})
	if !f.IsEmpty() {
		t.Error("filter should be empty with empty slice")
	}

	f = NewIncludeFilter([]string{"", "  ", "\t"})
	if !f.IsEmpty() {
		t.Error("filter should be empty with whitespace-only entries")
	}
}

func TestNewIncludeFilter_WithContracts(t *testing.T) {
	f := NewIncludeFilter([]string{"eosio.token", "eosio"})
	if f.IsEmpty() {
		t.Error("filter should not be empty")
	}

	contracts := f.Contracts()
	if len(contracts) != 2 {
		t.Errorf("Contracts() returned %d items, want 2", len(contracts))
	}
}

func TestNewIncludeFilter_WhitespaceHandling(t *testing.T) {
	f := NewIncludeFilter([]string{"  eosio.token  ", "\teosio\t"})
	if f.IsEmpty() {
		t.Error("filter should not be empty")
	}

	token := chain.StringToName("eosio.token")
	if !f.ShouldInclude(token) {
		t.Error("ShouldInclude(eosio.token) = false, want true")
	}
}

func TestIncludeFilter_IsEmpty(t *testing.T) {
	f := NewIncludeFilter(nil)
	if !f.IsEmpty() {
		t.Error("empty filter should be empty")
	}

	f = NewIncludeFilter([]string{"eosio.token"})
	if f.IsEmpty() {
		t.Error("non-empty filter should not be empty")
	}
}

func TestIncludeFilter_ShouldInclude_EmptyFilter(t *testing.T) {
	f := NewIncludeFilter(nil)

	token := chain.StringToName("eosio.token")
	if !f.ShouldInclude(token) {
		t.Error("empty filter should include everything")
	}

	random := chain.StringToName("randomcontract")
	if !f.ShouldInclude(random) {
		t.Error("empty filter should include everything")
	}
}

func TestIncludeFilter_ShouldInclude_WithContracts(t *testing.T) {
	f := NewIncludeFilter([]string{"eosio.token", "eosio"})

	token := chain.StringToName("eosio.token")
	eosio := chain.StringToName("eosio")
	other := chain.StringToName("atomicassets")

	if !f.ShouldInclude(token) {
		t.Error("ShouldInclude(eosio.token) = false, want true")
	}
	if !f.ShouldInclude(eosio) {
		t.Error("ShouldInclude(eosio) = false, want true")
	}
	if f.ShouldInclude(other) {
		t.Error("ShouldInclude(atomicassets) = true, want false")
	}
}

func TestIncludeFilter_AsFunc_Empty(t *testing.T) {
	f := NewIncludeFilter(nil)
	fn := f.AsFunc()
	if fn != nil {
		t.Error("AsFunc() should return nil for empty filter")
	}
}

func TestIncludeFilter_AsFunc_WithContracts(t *testing.T) {
	f := NewIncludeFilter([]string{"eosio.token"})
	fn := f.AsFunc()
	if fn == nil {
		t.Fatal("AsFunc() returned nil for non-empty filter")
	}

	token := chain.StringToName("eosio.token")
	other := chain.StringToName("atomicassets")
	action := chain.StringToName("transfer")

	if fn(token, action) {
		t.Error("AsFunc()(eosio.token, transfer) = true (skip), want false (include)")
	}
	if !fn(other, action) {
		t.Error("AsFunc()(atomicassets, transfer) = false (include), want true (skip)")
	}
}

func TestIncludeFilter_CanFulfill_Empty(t *testing.T) {
	f := NewIncludeFilter(nil)

	err := f.CanFulfill(nil)
	if err != nil {
		t.Errorf("CanFulfill(nil) = %v, want nil", err)
	}

	token := chain.StringToName("eosio.token")
	err = f.CanFulfill([]uint64{token})
	if err != nil {
		t.Errorf("CanFulfill with empty filter should return nil, got %v", err)
	}
}

func TestIncludeFilter_CanFulfill_WithContracts(t *testing.T) {
	f := NewIncludeFilter([]string{"eosio.token", "eosio"})

	token := chain.StringToName("eosio.token")
	eosio := chain.StringToName("eosio")
	other := chain.StringToName("atomicassets")

	err := f.CanFulfill([]uint64{token})
	if err != nil {
		t.Errorf("CanFulfill([eosio.token]) = %v, want nil", err)
	}

	err = f.CanFulfill([]uint64{token, eosio})
	if err != nil {
		t.Errorf("CanFulfill([eosio.token, eosio]) = %v, want nil", err)
	}

	err = f.CanFulfill([]uint64{other})
	if err == nil {
		t.Error("CanFulfill([atomicassets]) = nil, want error")
	}

	err = f.CanFulfill([]uint64{token, other})
	if err == nil {
		t.Error("CanFulfill([eosio.token, atomicassets]) = nil, want error")
	}
}

func TestIncludeFilter_Contracts(t *testing.T) {
	f := NewIncludeFilter(nil)
	contracts := f.Contracts()
	if len(contracts) != 0 {
		t.Errorf("empty filter Contracts() = %d items, want 0", len(contracts))
	}

	f = NewIncludeFilter([]string{"eosio.token", "eosio", "atomicassets"})
	contracts = f.Contracts()
	if len(contracts) != 3 {
		t.Errorf("Contracts() = %d items, want 3", len(contracts))
	}

	token := chain.StringToName("eosio.token")
	eosio := chain.StringToName("eosio")
	atomic := chain.StringToName("atomicassets")

	contractMap := make(map[uint64]bool)
	for _, c := range contracts {
		contractMap[c] = true
	}

	if !contractMap[token] {
		t.Error("Contracts() missing eosio.token")
	}
	if !contractMap[eosio] {
		t.Error("Contracts() missing eosio")
	}
	if !contractMap[atomic] {
		t.Error("Contracts() missing atomicassets")
	}
}

func TestIncludeFilter_Summary_Empty(t *testing.T) {
	f := NewIncludeFilter(nil)
	summary := f.Summary()
	if summary != "DISABLED (indexing all contracts)" {
		t.Errorf("Summary() = %q, want 'DISABLED (indexing all contracts)'", summary)
	}
}

func TestIncludeFilter_Summary_WithContracts(t *testing.T) {
	f := NewIncludeFilter([]string{"eosio.token", "eosio"})
	summary := f.Summary()

	if len(summary) == 0 {
		t.Error("Summary() returned empty string")
	}

	if !containsString(summary, "ENABLED") {
		t.Errorf("Summary() = %q, expected to contain 'ENABLED'", summary)
	}
	if !containsString(summary, "2 contracts") {
		t.Errorf("Summary() = %q, expected to contain '2 contracts'", summary)
	}
}

func BenchmarkIncludeFilter_ShouldInclude(b *testing.B) {
	f := NewIncludeFilter([]string{
		"eosio.token",
		"eosio",
		"atomicassets",
		"atomicmarket",
		"simpleassets",
	})

	contract := chain.StringToName("eosio.token")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ShouldInclude(contract)
	}
}

func BenchmarkIncludeFilter_ShouldInclude_Miss(b *testing.B) {
	f := NewIncludeFilter([]string{
		"eosio.token",
		"eosio",
		"atomicassets",
	})

	contract := chain.StringToName("randomcontract")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ShouldInclude(contract)
	}
}

func BenchmarkIncludeFilter_AsFunc(b *testing.B) {
	f := NewIncludeFilter([]string{
		"eosio.token",
		"eosio",
		"atomicassets",
	})

	fn := f.AsFunc()
	contract := chain.StringToName("eosio.token")
	action := chain.StringToName("transfer")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(contract, action)
	}
}
