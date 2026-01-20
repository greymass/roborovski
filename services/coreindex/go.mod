module github.com/greymass/roborovski/services/coreindex

go 1.25.3

require (
	github.com/DataDog/zstd v1.5.7
	github.com/greymass/roborovski/libraries/chain v0.0.0
	github.com/greymass/roborovski/libraries/corestream v0.0.0
	github.com/greymass/roborovski/libraries/encoding v0.0.0
	github.com/greymass/roborovski/libraries/enforce v0.0.0
	github.com/greymass/roborovski/libraries/fcraw v0.0.0
	github.com/greymass/roborovski/libraries/logger v0.0.0
	github.com/greymass/roborovski/libraries/openapi v0.0.0
	github.com/greymass/roborovski/libraries/profiler v0.0.0
	github.com/greymass/roborovski/libraries/server v0.0.0
	github.com/greymass/roborovski/libraries/tracereader v0.0.0
)

require (
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/google/pprof v0.0.0-20251213031049-b05bdaca462f // indirect
	github.com/greymass/go-eosio v0.2.6 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pb33f/jsonpath v0.7.0 // indirect
	github.com/pb33f/libopenapi v0.31.2 // indirect
	github.com/pb33f/ordered-map/v2 v2.3.0 // indirect
	go.yaml.in/yaml/v4 v4.0.0-rc.3 // indirect
	golang.org/x/sync v0.19.0 // indirect
)

replace (
	github.com/greymass/roborovski/libraries/chain => ../../libraries/chain
	github.com/greymass/roborovski/libraries/corestream => ../../libraries/corestream
	github.com/greymass/roborovski/libraries/encoding => ../../libraries/encoding
	github.com/greymass/roborovski/libraries/enforce => ../../libraries/enforce
	github.com/greymass/roborovski/libraries/fcraw => ../../libraries/fcraw
	github.com/greymass/roborovski/libraries/logger => ../../libraries/logger
	github.com/greymass/roborovski/libraries/openapi => ../../libraries/openapi
	github.com/greymass/roborovski/libraries/profiler => ../../libraries/profiler
	github.com/greymass/roborovski/libraries/server => ../../libraries/server
	github.com/greymass/roborovski/libraries/tracereader => ../../libraries/tracereader
)
