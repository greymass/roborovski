package corereader

import (
	"fmt"
	"strings"

	"github.com/greymass/roborovski/libraries/chain"
)

type filterKey struct {
	contract uint64
	action   uint64
}

type accountFilterKey struct {
	account  uint64
	contract uint64
	action   uint64
}

type accountContractKey struct {
	account  uint64
	contract uint64
}

type accountActionKey struct {
	account uint64
	action  uint64
}

type ActionFilter struct {
	ignoredExact             map[filterKey]struct{}
	ignoredContractWildcards map[uint64]struct{}
	ignoredActionWildcards   map[uint64]struct{}
	accountExact             map[accountFilterKey]struct{}
	accountContractWildcards map[accountContractKey]struct{}
	accountActionWildcards   map[accountActionKey]struct{}
	accountGlobalWildcards   map[uint64]struct{}
	hasGlobalFilters         bool
	hasAccountFilters        bool
}

type FilterSummary struct {
	GlobalFilters   int
	AccountFilters  int
	TotalFilters    int
	GlobalExact     int
	GlobalContract  int
	GlobalAction    int
	AccountExact    int
	AccountContract int
	AccountAction   int
	AccountGlobal   int
}

func (s FilterSummary) String() string {
	if s.TotalFilters == 0 {
		return "DISABLED (indexing all actions)"
	}
	return fmt.Sprintf("%d filter(s) (global: %d exact, %d contract::*, %d *::action | account: %d exact, %d @contract::*, %d @*::action, %d @*::*)",
		s.TotalFilters,
		s.GlobalExact, s.GlobalContract, s.GlobalAction,
		s.AccountExact, s.AccountContract, s.AccountAction, s.AccountGlobal)
}

func NewEmptyFilter() *ActionFilter {
	return &ActionFilter{
		ignoredExact:             make(map[filterKey]struct{}),
		ignoredContractWildcards: make(map[uint64]struct{}),
		ignoredActionWildcards:   make(map[uint64]struct{}),
		accountExact:             make(map[accountFilterKey]struct{}),
		accountContractWildcards: make(map[accountContractKey]struct{}),
		accountActionWildcards:   make(map[accountActionKey]struct{}),
		accountGlobalWildcards:   make(map[uint64]struct{}),
	}
}

func NewActionFilter(patterns []string) *ActionFilter {
	f := NewEmptyFilter()

	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}

		var accountName string
		var accountSpecific bool
		if strings.Contains(pattern, "@") {
			atParts := strings.SplitN(pattern, "@", 2)
			if len(atParts) == 2 {
				accountName = strings.TrimSpace(atParts[0])
				pattern = atParts[1]
				accountSpecific = true
			}
		}

		if !strings.Contains(pattern, "::") {
			contract := chain.StringToName(pattern)
			if accountSpecific {
				account := chain.StringToName(accountName)
				f.accountContractWildcards[accountContractKey{account, contract}] = struct{}{}
			} else {
				f.ignoredContractWildcards[contract] = struct{}{}
			}
			continue
		}

		parts := strings.SplitN(pattern, "::", 2)
		contractName := strings.TrimSpace(parts[0])
		actionName := strings.TrimSpace(parts[1])

		isContractWildcard := contractName == "*"
		isActionWildcard := actionName == "*"

		if accountSpecific {
			account := chain.StringToName(accountName)
			if isContractWildcard && isActionWildcard {
				f.accountGlobalWildcards[account] = struct{}{}
			} else if isContractWildcard {
				action := chain.StringToName(actionName)
				f.accountActionWildcards[accountActionKey{account, action}] = struct{}{}
			} else if isActionWildcard {
				contract := chain.StringToName(contractName)
				f.accountContractWildcards[accountContractKey{account, contract}] = struct{}{}
			} else {
				contract := chain.StringToName(contractName)
				action := chain.StringToName(actionName)
				f.accountExact[accountFilterKey{account, contract, action}] = struct{}{}
			}
		} else {
			if isContractWildcard && isActionWildcard {
				continue
			} else if isContractWildcard {
				action := chain.StringToName(actionName)
				f.ignoredActionWildcards[action] = struct{}{}
			} else if isActionWildcard {
				contract := chain.StringToName(contractName)
				f.ignoredContractWildcards[contract] = struct{}{}
			} else {
				contract := chain.StringToName(contractName)
				action := chain.StringToName(actionName)
				f.ignoredExact[filterKey{contract, action}] = struct{}{}
			}
		}
	}

	f.hasGlobalFilters = len(f.ignoredExact) > 0 || len(f.ignoredContractWildcards) > 0 || len(f.ignoredActionWildcards) > 0
	f.hasAccountFilters = len(f.accountExact) > 0 || len(f.accountContractWildcards) > 0 ||
		len(f.accountActionWildcards) > 0 || len(f.accountGlobalWildcards) > 0

	return f
}

func (f *ActionFilter) ShouldSkip(contract, action uint64) bool {
	if !f.hasGlobalFilters {
		return false
	}

	if _, found := f.ignoredContractWildcards[contract]; found {
		return true
	}
	if _, found := f.ignoredActionWildcards[action]; found {
		return true
	}
	if _, found := f.ignoredExact[filterKey{contract, action}]; found {
		return true
	}
	return false
}

func (f *ActionFilter) ShouldSkipForAccount(account, contract, action uint64) bool {
	if !f.hasGlobalFilters && !f.hasAccountFilters {
		return false
	}

	if f.hasAccountFilters {
		if _, found := f.accountGlobalWildcards[account]; found {
			return true
		}
		if _, found := f.accountExact[accountFilterKey{account, contract, action}]; found {
			return true
		}
		if _, found := f.accountContractWildcards[accountContractKey{account, contract}]; found {
			return true
		}
		if _, found := f.accountActionWildcards[accountActionKey{account, action}]; found {
			return true
		}
	}

	return f.ShouldSkip(contract, action)
}

func (f *ActionFilter) AsFunc() ActionFilterFunc {
	if !f.hasGlobalFilters {
		return nil
	}
	return func(contract, action uint64) bool {
		return f.ShouldSkip(contract, action)
	}
}

func (f *ActionFilter) IsEmpty() bool {
	return !f.hasGlobalFilters && !f.hasAccountFilters
}

func (f *ActionFilter) Summary() FilterSummary {
	globalExact := len(f.ignoredExact)
	globalContract := len(f.ignoredContractWildcards)
	globalAction := len(f.ignoredActionWildcards)
	accountExact := len(f.accountExact)
	accountContract := len(f.accountContractWildcards)
	accountAction := len(f.accountActionWildcards)
	accountGlobal := len(f.accountGlobalWildcards)

	globalFilters := globalExact + globalContract + globalAction
	accountFilters := accountExact + accountContract + accountAction + accountGlobal

	return FilterSummary{
		GlobalFilters:   globalFilters,
		AccountFilters:  accountFilters,
		TotalFilters:    globalFilters + accountFilters,
		GlobalExact:     globalExact,
		GlobalContract:  globalContract,
		GlobalAction:    globalAction,
		AccountExact:    accountExact,
		AccountContract: accountContract,
		AccountAction:   accountAction,
		AccountGlobal:   accountGlobal,
	}
}

func (f *ActionFilter) HasGlobalFilters() bool {
	return f.hasGlobalFilters
}

func (f *ActionFilter) HasAccountFilters() bool {
	return f.hasAccountFilters
}
