package main

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

func initOpenAPI() error {
	spec, err := openapi.LoadWithVersion(openapiYAML, Version)
	if err != nil {
		return fmt.Errorf("failed to load OpenAPI spec: %w", err)
	}
	openapiSpec = spec
	return nil
}

var registeredRoutes = map[string][]string{
	"/v1/history/get_info":                    {"GET", "POST"},
	"/v1/chain/get_info":                      {"GET", "POST"},
	"/v1/history/get_notified_in_block":       {"POST"},
	"/v1/history/get_raw_actions_in_block":    {"POST"},
	"/v1/history/get_block_binary":            {"POST"},
	"/v1/history/get_actions_by_globs_binary": {"POST"},
	"/v1/history/get_state_binary":            {"POST"},
	"/v1/history/get_blocks_batch_binary":     {"POST"},
}

func validateOpenAPIRoutes() error {
	if openapiSpec == nil {
		return fmt.Errorf("OpenAPI spec not loaded")
	}

	result := openapiSpec.ValidateRoutes(func(path string, method string) bool {
		methods, exists := registeredRoutes[path]
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
	for _, methods := range registeredRoutes {
		routeCount += len(methods)
	}
	logger.Printf("startup", "OpenAPI spec validated: %d routes match handlers", routeCount)
	return nil
}

func handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if openapiSpec == nil {
		http.Error(w, "OpenAPI spec not loaded", http.StatusInternalServerError)
		return
	}
	openapiSpec.Handler().ServeHTTP(w, r)
}
