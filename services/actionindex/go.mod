module github.com/greymass/roborovski/services/actionindex

go 1.25.3

require (
	github.com/RoaringBitmap/roaring v1.9.4
	github.com/cockroachdb/pebble/v2 v2.1.3
	github.com/greymass/roborovski/libraries/abicache v0.0.0
	github.com/greymass/roborovski/libraries/chain v0.0.0
	github.com/greymass/roborovski/libraries/corereader v0.0.0
	github.com/greymass/roborovski/libraries/encoding v0.0.0
	github.com/greymass/roborovski/libraries/openapi v0.0.0
	github.com/greymass/roborovski/libraries/server v0.0.0
	nhooyr.io/websocket v1.8.17
)

require (
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.5-0.20231225225746-43d5d4cd4e0e // indirect
	github.com/greymass/go-eosio v0.2.6 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pb33f/jsonpath v0.7.0 // indirect
	github.com/pb33f/libopenapi v0.31.2 // indirect
	github.com/pb33f/ordered-map/v2 v2.3.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.22.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	go.yaml.in/yaml/v4 v4.0.0-rc.3 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace (
	github.com/greymass/roborovski/libraries/abicache => ../../libraries/abicache
	github.com/greymass/roborovski/libraries/chain => ../../libraries/chain
	github.com/greymass/roborovski/libraries/corereader => ../../libraries/corereader
	github.com/greymass/roborovski/libraries/encoding => ../../libraries/encoding
	github.com/greymass/roborovski/libraries/openapi => ../../libraries/openapi
	github.com/greymass/roborovski/libraries/server => ../../libraries/server
)
