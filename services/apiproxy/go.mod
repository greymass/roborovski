module github.com/greymass/roborovski/services/apiproxy

go 1.25.3

require (
	github.com/greymass/roborovski/libraries/config v0.0.0
	github.com/greymass/roborovski/libraries/logger v0.0.0
	github.com/greymass/roborovski/libraries/server v0.0.0-00010101000000-000000000000
	github.com/prometheus/client_golang v1.22.0
	github.com/sony/gobreaker v1.0.0
	golang.org/x/sync v0.19.0
	golang.org/x/time v0.8.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/greymass/roborovski/libraries/encoding v0.0.0-00010101000000-000000000000 // indirect
	github.com/greymass/roborovski/libraries/enforce v0.0.0-00010101000000-000000000000 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	golang.org/x/sys v0.32.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace (
	github.com/greymass/roborovski/libraries/config => ../../libraries/config
	github.com/greymass/roborovski/libraries/encoding => ../../libraries/encoding
	github.com/greymass/roborovski/libraries/enforce => ../../libraries/enforce
	github.com/greymass/roborovski/libraries/logger => ../../libraries/logger
	github.com/greymass/roborovski/libraries/profiler => ../../libraries/profiler
	github.com/greymass/roborovski/libraries/server => ../../libraries/server
)
