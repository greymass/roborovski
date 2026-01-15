module github.com/greymass/roborovski/services/streamproxy

go 1.25.3

require (
	github.com/greymass/roborovski/libraries/config v0.0.0
	github.com/greymass/roborovski/libraries/logger v0.0.0
	github.com/greymass/roborovski/libraries/profiler v0.0.0-00010101000000-000000000000
	github.com/prometheus/client_golang v1.22.0
	nhooyr.io/websocket v1.8.17
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/pprof v0.0.0-20251213031049-b05bdaca462f // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/sys v0.32.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace (
	github.com/greymass/roborovski/libraries/config => ../../libraries/config
	github.com/greymass/roborovski/libraries/logger => ../../libraries/logger
	github.com/greymass/roborovski/libraries/profiler => ../../libraries/profiler
)
