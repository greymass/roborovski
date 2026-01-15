module github.com/greymass/roborovski/libraries/abicache

go 1.25.3

require (
	github.com/greymass/go-eosio v0.2.3
	github.com/greymass/roborovski/libraries/chain v0.0.0
)

replace github.com/greymass/roborovski/libraries/chain => ../chain
