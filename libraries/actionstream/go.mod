module github.com/greymass/roborovski/libraries/actionstream

go 1.25.3

require (
	github.com/greymass/roborovski/libraries/chain v0.0.0
	github.com/greymass/roborovski/libraries/logger v0.0.0
)

replace (
	github.com/greymass/roborovski/libraries/chain => ../chain
	github.com/greymass/roborovski/libraries/logger => ../logger
)
