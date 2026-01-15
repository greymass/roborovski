module github.com/greymass/roborovski/services/coreverify

go 1.25.3

require github.com/greymass/roborovski/services/coreindex v0.0.0

require (
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/greymass/roborovski/libraries/logger v0.0.0 // indirect
)

replace (
	github.com/greymass/roborovski/libraries/logger => ../../libraries/logger
	github.com/greymass/roborovski/services/coreindex => ../coreindex
)
