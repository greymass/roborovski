package internal

import (
	_ "embed"
	"fmt"
	"net/http"
	"strings"

	"github.com/greymass/roborovski/libraries/logger"
	"github.com/greymass/roborovski/libraries/openapi"
)

//go:embed openapi.yaml
var openapiYAML []byte

var openapiSpec *openapi.Spec

func InitOpenAPI(version string) error {
	spec, err := openapi.LoadWithVersion(openapiYAML, version)
	if err != nil {
		return fmt.Errorf("failed to load OpenAPI spec: %w", err)
	}
	openapiSpec = spec
	return nil
}

var registeredRoutes = map[string][]string{
	"/v1/history/get_actions":     {"GET", "POST"},
	"/account/{account}/activity": {"GET"},
	"/account/{account}/log":      {"GET"},
	"/account/{account}/stats":    {"GET"},
}

var debugRoutes = map[string][]string{
	"/debug/account":                 {"GET"},
	"/debug/chunk":                   {"GET"},
	"/debug/read-chunk":              {"GET"},
	"/debug/compare":                 {"GET"},
	"/debug/scan-contract-action":    {"GET"},
	"/debug/timemap":                 {"GET"},
	"/debug/seq":                     {"GET"},
	"/debug/seq-to-date":             {"GET"},
	"/debug/wal":                     {"GET"},
	"/debug/slice":                   {"GET"},
	"/debug/block":                   {"GET"},
	"/debug/action":                  {"GET"},
	"/debug/search-account-in-block": {"GET"},
	"/debug/filter-block":            {"GET"},
}

func ValidateOpenAPIRoutes(includeDebug bool) error {
	if openapiSpec == nil {
		return fmt.Errorf("OpenAPI spec not loaded")
	}

	routes := make(map[string][]string)
	for k, v := range registeredRoutes {
		routes[k] = v
	}
	if includeDebug {
		for k, v := range debugRoutes {
			routes[k] = v
		}
	}

	result := openapiSpec.ValidateRoutes(func(path string, method string) bool {
		if !includeDebug && strings.HasPrefix(path, "/debug/") {
			return true
		}
		methods, exists := routes[path]
		if !exists {
			return false
		}
		for _, m := range methods {
			if m == method {
				return true
			}
		}
		return false
	})

	if !result.Valid {
		var errMsgs []string
		for _, missing := range result.MissingHandlers {
			errMsgs = append(errMsgs, fmt.Sprintf("OpenAPI declares %s but no handler registered", missing))
		}
		return fmt.Errorf("OpenAPI validation failed:\n  %s", strings.Join(errMsgs, "\n  "))
	}

	routeCount := 0
	for _, methods := range routes {
		routeCount += len(methods)
	}
	logger.Printf("startup", "OpenAPI spec validated: %d routes match handlers", routeCount)
	return nil
}

func HandleOpenAPI(includeDebug bool, w http.ResponseWriter, r *http.Request) {
	if openapiSpec == nil {
		http.Error(w, "OpenAPI spec not loaded", http.StatusInternalServerError)
		return
	}
	if includeDebug {
		openapiSpec.Handler().ServeHTTP(w, r)
	} else {
		openapiSpec.FilteredHandler(func(path string) bool {
			return !strings.HasPrefix(path, "/debug/")
		}).ServeHTTP(w, r)
	}
}
