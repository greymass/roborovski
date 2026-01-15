package corereader

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

func TestFilterSummary_String(t *testing.T) {
	tests := []struct {
		name     string
		summary  FilterSummary
		contains string
	}{
		{
			name:     "empty filter",
			summary:  FilterSummary{TotalFilters: 0},
			contains: "DISABLED",
		},
		{
			name: "with filters",
			summary: FilterSummary{
				TotalFilters:    5,
				GlobalExact:     2,
				GlobalContract:  1,
				GlobalAction:    1,
				AccountExact:    1,
				AccountContract: 0,
				AccountAction:   0,
				AccountGlobal:   0,
			},
			contains: "5 filter(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.summary.String()
			if len(result) == 0 {
				t.Error("String() returned empty")
			}
			if tt.contains != "" && !containsString(result, tt.contains) {
				t.Errorf("String() = %q, expected to contain %q", result, tt.contains)
			}
		})
	}
}

func TestNewEmptyFilter(t *testing.T) {
	f := NewEmptyFilter()
	if f == nil {
		t.Fatal("NewEmptyFilter() returned nil")
	}
	if !f.IsEmpty() {
		t.Error("NewEmptyFilter().IsEmpty() = false, want true")
	}
	if f.HasGlobalFilters() {
		t.Error("NewEmptyFilter().HasGlobalFilters() = true, want false")
	}
	if f.HasAccountFilters() {
		t.Error("NewEmptyFilter().HasAccountFilters() = true, want false")
	}
}

func TestNewActionFilter_EmptyPatterns(t *testing.T) {
	f := NewActionFilter(nil)
	if f == nil {
		t.Fatal("NewActionFilter(nil) returned nil")
	}
	if !f.IsEmpty() {
		t.Error("filter should be empty with nil patterns")
	}

	f = NewActionFilter([]string{})
	if !f.IsEmpty() {
		t.Error("filter should be empty with empty slice")
	}

	f = NewActionFilter([]string{"", "  ", "\t"})
	if !f.IsEmpty() {
		t.Error("filter should be empty with whitespace-only patterns")
	}
}

func TestNewActionFilter_ContractWildcard(t *testing.T) {
	f := NewActionFilter([]string{"eosio.token"})
	if f.IsEmpty() {
		t.Error("filter should not be empty")
	}
	if !f.HasGlobalFilters() {
		t.Error("filter should have global filters")
	}

	contract := chain.StringToName("eosio.token")
	action := chain.StringToName("transfer")

	if !f.ShouldSkip(contract, action) {
		t.Error("ShouldSkip(eosio.token, transfer) = false, want true")
	}
	if !f.ShouldSkip(contract, chain.StringToName("issue")) {
		t.Error("ShouldSkip(eosio.token, issue) = false, want true")
	}

	otherContract := chain.StringToName("eosio")
	if f.ShouldSkip(otherContract, action) {
		t.Error("ShouldSkip(eosio, transfer) = true, want false")
	}
}

func TestNewActionFilter_ContractColonWildcard(t *testing.T) {
	f := NewActionFilter([]string{"eosio.token::*"})
	if !f.HasGlobalFilters() {
		t.Error("filter should have global filters")
	}

	contract := chain.StringToName("eosio.token")
	if !f.ShouldSkip(contract, chain.StringToName("transfer")) {
		t.Error("ShouldSkip(eosio.token, transfer) = false, want true")
	}
	if !f.ShouldSkip(contract, chain.StringToName("issue")) {
		t.Error("ShouldSkip(eosio.token, issue) = false, want true")
	}
}

func TestNewActionFilter_ActionWildcard(t *testing.T) {
	f := NewActionFilter([]string{"*::transfer"})
	if !f.HasGlobalFilters() {
		t.Error("filter should have global filters")
	}

	action := chain.StringToName("transfer")
	if !f.ShouldSkip(chain.StringToName("eosio.token"), action) {
		t.Error("ShouldSkip(eosio.token, transfer) = false, want true")
	}
	if !f.ShouldSkip(chain.StringToName("mycontract"), action) {
		t.Error("ShouldSkip(mycontract, transfer) = false, want true")
	}

	if f.ShouldSkip(chain.StringToName("eosio.token"), chain.StringToName("issue")) {
		t.Error("ShouldSkip(eosio.token, issue) = true, want false")
	}
}

func TestNewActionFilter_ExactMatch(t *testing.T) {
	f := NewActionFilter([]string{"eosio.token::transfer"})
	if !f.HasGlobalFilters() {
		t.Error("filter should have global filters")
	}

	contract := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")
	issue := chain.StringToName("issue")

	if !f.ShouldSkip(contract, transfer) {
		t.Error("ShouldSkip(eosio.token, transfer) = false, want true")
	}
	if f.ShouldSkip(contract, issue) {
		t.Error("ShouldSkip(eosio.token, issue) = true, want false")
	}
}

func TestNewActionFilter_DoubleWildcard(t *testing.T) {
	f := NewActionFilter([]string{"*::*"})
	if f.HasGlobalFilters() {
		t.Error("*::* should not create global filters")
	}
	if !f.IsEmpty() {
		t.Error("*::* should result in empty filter")
	}
}

func TestNewActionFilter_AccountSpecific_GlobalWildcard(t *testing.T) {
	f := NewActionFilter([]string{"alice@*::*"})
	if !f.HasAccountFilters() {
		t.Error("filter should have account filters")
	}
	if f.HasGlobalFilters() {
		t.Error("filter should not have global filters")
	}

	alice := chain.StringToName("alice")
	bob := chain.StringToName("bob")
	contract := chain.StringToName("eosio.token")
	action := chain.StringToName("transfer")

	if !f.ShouldSkipForAccount(alice, contract, action) {
		t.Error("ShouldSkipForAccount(alice, ...) = false, want true")
	}
	if f.ShouldSkipForAccount(bob, contract, action) {
		t.Error("ShouldSkipForAccount(bob, ...) = true, want false")
	}
}

func TestNewActionFilter_AccountSpecific_ContractWildcard(t *testing.T) {
	f := NewActionFilter([]string{"alice@eosio.token::*"})
	if !f.HasAccountFilters() {
		t.Error("filter should have account filters")
	}

	alice := chain.StringToName("alice")
	bob := chain.StringToName("bob")
	token := chain.StringToName("eosio.token")
	eosio := chain.StringToName("eosio")
	transfer := chain.StringToName("transfer")

	if !f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount(alice, eosio.token, transfer) = false, want true")
	}
	if f.ShouldSkipForAccount(alice, eosio, transfer) {
		t.Error("ShouldSkipForAccount(alice, eosio, transfer) = true, want false")
	}
	if f.ShouldSkipForAccount(bob, token, transfer) {
		t.Error("ShouldSkipForAccount(bob, eosio.token, transfer) = true, want false")
	}
}

func TestNewActionFilter_AccountSpecific_ContractNoColon(t *testing.T) {
	f := NewActionFilter([]string{"alice@eosio.token"})
	if !f.HasAccountFilters() {
		t.Error("filter should have account filters")
	}

	alice := chain.StringToName("alice")
	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")

	if !f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount(alice, eosio.token, transfer) = false, want true")
	}
}

func TestNewActionFilter_AccountSpecific_ActionWildcard(t *testing.T) {
	f := NewActionFilter([]string{"alice@*::transfer"})
	if !f.HasAccountFilters() {
		t.Error("filter should have account filters")
	}

	alice := chain.StringToName("alice")
	bob := chain.StringToName("bob")
	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")
	issue := chain.StringToName("issue")

	if !f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount(alice, eosio.token, transfer) = false, want true")
	}
	if f.ShouldSkipForAccount(alice, token, issue) {
		t.Error("ShouldSkipForAccount(alice, eosio.token, issue) = true, want false")
	}
	if f.ShouldSkipForAccount(bob, token, transfer) {
		t.Error("ShouldSkipForAccount(bob, eosio.token, transfer) = true, want false")
	}
}

func TestNewActionFilter_AccountSpecific_Exact(t *testing.T) {
	f := NewActionFilter([]string{"alice@eosio.token::transfer"})
	if !f.HasAccountFilters() {
		t.Error("filter should have account filters")
	}

	alice := chain.StringToName("alice")
	bob := chain.StringToName("bob")
	token := chain.StringToName("eosio.token")
	eosio := chain.StringToName("eosio")
	transfer := chain.StringToName("transfer")
	issue := chain.StringToName("issue")

	if !f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount(alice, eosio.token, transfer) = false, want true")
	}
	if f.ShouldSkipForAccount(alice, token, issue) {
		t.Error("ShouldSkipForAccount(alice, eosio.token, issue) = true, want false")
	}
	if f.ShouldSkipForAccount(alice, eosio, transfer) {
		t.Error("ShouldSkipForAccount(alice, eosio, transfer) = true, want false")
	}
	if f.ShouldSkipForAccount(bob, token, transfer) {
		t.Error("ShouldSkipForAccount(bob, eosio.token, transfer) = true, want false")
	}
}

func TestNewActionFilter_MultiplePatterns(t *testing.T) {
	f := NewActionFilter([]string{
		"eosio.token::transfer",
		"eosio::setcode",
		"alice@*::*",
	})
	if !f.HasGlobalFilters() {
		t.Error("filter should have global filters")
	}
	if !f.HasAccountFilters() {
		t.Error("filter should have account filters")
	}

	token := chain.StringToName("eosio.token")
	eosio := chain.StringToName("eosio")
	transfer := chain.StringToName("transfer")
	setcode := chain.StringToName("setcode")
	alice := chain.StringToName("alice")

	if !f.ShouldSkip(token, transfer) {
		t.Error("ShouldSkip(eosio.token, transfer) = false, want true")
	}
	if !f.ShouldSkip(eosio, setcode) {
		t.Error("ShouldSkip(eosio, setcode) = false, want true")
	}
	if !f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount(alice, ...) = false, want true")
	}
}

func TestNewActionFilter_WhitespaceHandling(t *testing.T) {
	f := NewActionFilter([]string{
		"  eosio.token::transfer  ",
		"\teosio::setcode\t",
		"  alice @ eosio.token :: transfer  ",
	})

	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")

	if !f.ShouldSkip(token, transfer) {
		t.Error("whitespace trimming failed for eosio.token::transfer")
	}
}

func TestActionFilter_ShouldSkip_NoGlobalFilters(t *testing.T) {
	f := NewActionFilter([]string{"alice@eosio.token::transfer"})
	if f.HasGlobalFilters() {
		t.Error("filter should not have global filters")
	}

	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")

	if f.ShouldSkip(token, transfer) {
		t.Error("ShouldSkip should return false when no global filters")
	}
}

func TestActionFilter_ShouldSkipForAccount_NoFilters(t *testing.T) {
	f := NewEmptyFilter()
	alice := chain.StringToName("alice")
	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")

	if f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount should return false when no filters")
	}
}

func TestActionFilter_ShouldSkipForAccount_FallsBackToGlobal(t *testing.T) {
	f := NewActionFilter([]string{"eosio.token::transfer"})
	alice := chain.StringToName("alice")
	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")

	if !f.ShouldSkipForAccount(alice, token, transfer) {
		t.Error("ShouldSkipForAccount should fall back to global filter")
	}
}

func TestActionFilter_AsFunc(t *testing.T) {
	f := NewEmptyFilter()
	fn := f.AsFunc()
	if fn != nil {
		t.Error("AsFunc() should return nil for empty filter")
	}

	f = NewActionFilter([]string{"eosio.token::transfer"})
	fn = f.AsFunc()
	if fn == nil {
		t.Fatal("AsFunc() returned nil for non-empty filter")
	}

	token := chain.StringToName("eosio.token")
	transfer := chain.StringToName("transfer")
	issue := chain.StringToName("issue")

	if !fn(token, transfer) {
		t.Error("AsFunc()(eosio.token, transfer) = false, want true")
	}
	if fn(token, issue) {
		t.Error("AsFunc()(eosio.token, issue) = true, want false")
	}
}

func TestActionFilter_IsEmpty(t *testing.T) {
	f := NewEmptyFilter()
	if !f.IsEmpty() {
		t.Error("empty filter should be empty")
	}

	f = NewActionFilter([]string{"eosio.token"})
	if f.IsEmpty() {
		t.Error("filter with patterns should not be empty")
	}

	f = NewActionFilter([]string{"alice@eosio.token"})
	if f.IsEmpty() {
		t.Error("filter with account patterns should not be empty")
	}
}

func TestActionFilter_Summary(t *testing.T) {
	f := NewEmptyFilter()
	s := f.Summary()
	if s.TotalFilters != 0 {
		t.Errorf("empty filter TotalFilters = %d, want 0", s.TotalFilters)
	}

	f = NewActionFilter([]string{
		"eosio.token::transfer",
		"eosio",
		"*::setcode",
		"alice@eosio.token::transfer",
		"alice@eosio",
		"alice@*::issue",
		"bob@*::*",
	})
	s = f.Summary()

	if s.GlobalExact != 1 {
		t.Errorf("GlobalExact = %d, want 1", s.GlobalExact)
	}
	if s.GlobalContract != 1 {
		t.Errorf("GlobalContract = %d, want 1", s.GlobalContract)
	}
	if s.GlobalAction != 1 {
		t.Errorf("GlobalAction = %d, want 1", s.GlobalAction)
	}
	if s.GlobalFilters != 3 {
		t.Errorf("GlobalFilters = %d, want 3", s.GlobalFilters)
	}
	if s.AccountExact != 1 {
		t.Errorf("AccountExact = %d, want 1", s.AccountExact)
	}
	if s.AccountContract != 1 {
		t.Errorf("AccountContract = %d, want 1", s.AccountContract)
	}
	if s.AccountAction != 1 {
		t.Errorf("AccountAction = %d, want 1", s.AccountAction)
	}
	if s.AccountGlobal != 1 {
		t.Errorf("AccountGlobal = %d, want 1", s.AccountGlobal)
	}
	if s.AccountFilters != 4 {
		t.Errorf("AccountFilters = %d, want 4", s.AccountFilters)
	}
	if s.TotalFilters != 7 {
		t.Errorf("TotalFilters = %d, want 7", s.TotalFilters)
	}
}

func TestActionFilter_HasGlobalFilters(t *testing.T) {
	f := NewEmptyFilter()
	if f.HasGlobalFilters() {
		t.Error("empty filter should not have global filters")
	}

	f = NewActionFilter([]string{"eosio.token"})
	if !f.HasGlobalFilters() {
		t.Error("filter with global pattern should have global filters")
	}

	f = NewActionFilter([]string{"alice@eosio.token"})
	if f.HasGlobalFilters() {
		t.Error("filter with only account patterns should not have global filters")
	}
}

func TestActionFilter_HasAccountFilters(t *testing.T) {
	f := NewEmptyFilter()
	if f.HasAccountFilters() {
		t.Error("empty filter should not have account filters")
	}

	f = NewActionFilter([]string{"eosio.token"})
	if f.HasAccountFilters() {
		t.Error("filter with only global patterns should not have account filters")
	}

	f = NewActionFilter([]string{"alice@eosio.token"})
	if !f.HasAccountFilters() {
		t.Error("filter with account pattern should have account filters")
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func BenchmarkActionFilter_ShouldSkip(b *testing.B) {
	f := NewActionFilter([]string{
		"eosio.token::transfer",
		"eosio::setcode",
		"eosio::setabi",
		"atomicassets",
	})

	contract := chain.StringToName("eosio.token")
	action := chain.StringToName("transfer")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ShouldSkip(contract, action)
	}
}

func BenchmarkActionFilter_ShouldSkipForAccount(b *testing.B) {
	f := NewActionFilter([]string{
		"eosio.token::transfer",
		"alice@eosio.token::transfer",
		"alice@*::*",
	})

	alice := chain.StringToName("alice")
	contract := chain.StringToName("eosio.token")
	action := chain.StringToName("transfer")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ShouldSkipForAccount(alice, contract, action)
	}
}
