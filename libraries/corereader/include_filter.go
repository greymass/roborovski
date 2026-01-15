package corereader

import (
	"fmt"
	"strings"

	"github.com/greymass/roborovski/libraries/chain"
)

type IncludeFilter struct {
	contracts map[uint64]struct{}
}

func NewIncludeFilter(contractNames []string) *IncludeFilter {
	f := &IncludeFilter{
		contracts: make(map[uint64]struct{}),
	}

	for _, name := range contractNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		contract := chain.StringToName(name)
		f.contracts[contract] = struct{}{}
	}

	return f
}

func (f *IncludeFilter) IsEmpty() bool {
	return len(f.contracts) == 0
}

func (f *IncludeFilter) ShouldInclude(contract uint64) bool {
	if f.IsEmpty() {
		return true
	}
	_, found := f.contracts[contract]
	return found
}

func (f *IncludeFilter) AsFunc() ActionFilterFunc {
	if f.IsEmpty() {
		return nil
	}
	return func(contract, action uint64) bool {
		_, found := f.contracts[contract]
		return !found
	}
}

func (f *IncludeFilter) CanFulfill(contracts []uint64) error {
	if f.IsEmpty() {
		return nil
	}
	for _, contract := range contracts {
		if _, found := f.contracts[contract]; !found {
			return fmt.Errorf("contract %d not in filter", contract)
		}
	}
	return nil
}

func (f *IncludeFilter) Contracts() []uint64 {
	result := make([]uint64, 0, len(f.contracts))
	for c := range f.contracts {
		result = append(result, c)
	}
	return result
}

func (f *IncludeFilter) Summary() string {
	if f.IsEmpty() {
		return "DISABLED (indexing all contracts)"
	}
	names := make([]string, 0, len(f.contracts))
	for c := range f.contracts {
		names = append(names, chain.NameToString(c))
	}
	return fmt.Sprintf("ENABLED (%d contracts: %s)", len(f.contracts), strings.Join(names, ", "))
}
