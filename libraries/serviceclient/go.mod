module github.com/greymass/roborovski/libraries/serviceclient

go 1.25.3

require github.com/greymass/roborovski/libraries/encoding v0.0.0

require (
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/greymass/roborovski/libraries/encoding => ../encoding
