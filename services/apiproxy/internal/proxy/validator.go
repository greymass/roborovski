package proxy

import (
	"bytes"
	"net/url"
	"sync"
)

type RouteValidator struct {
	allowedQueryParams map[string]struct{}
	allowedBodyKeys    map[string]struct{}
	enabled            bool
}

type ValidatorConfig struct {
	AllowedQueryParams []string
	AllowedBodyKeys    []string
}

func NewRouteValidator(cfg *ValidatorConfig) *RouteValidator {
	if cfg == nil {
		return &RouteValidator{enabled: false}
	}

	v := &RouteValidator{
		allowedQueryParams: make(map[string]struct{}, len(cfg.AllowedQueryParams)),
		allowedBodyKeys:    make(map[string]struct{}, len(cfg.AllowedBodyKeys)),
		enabled:            len(cfg.AllowedQueryParams) > 0 || len(cfg.AllowedBodyKeys) > 0,
	}

	for _, p := range cfg.AllowedQueryParams {
		v.allowedQueryParams[p] = struct{}{}
	}
	for _, k := range cfg.AllowedBodyKeys {
		v.allowedBodyKeys[k] = struct{}{}
	}

	return v
}

func (v *RouteValidator) Enabled() bool {
	return v.enabled
}

func (v *RouteValidator) ValidateQuery(query url.Values) bool {
	if len(v.allowedQueryParams) == 0 {
		return true
	}
	for key := range query {
		if _, ok := v.allowedQueryParams[key]; !ok {
			return false
		}
	}
	return true
}

func (v *RouteValidator) ValidateBody(body []byte) bool {
	if len(v.allowedBodyKeys) == 0 {
		return true
	}
	if len(body) == 0 {
		return true
	}

	keys := extractJSONKeys(body)
	for _, key := range keys {
		if _, ok := v.allowedBodyKeys[key]; !ok {
			return false
		}
	}
	return true
}

var keyBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 64)
		return &buf
	},
}

func extractJSONKeys(body []byte) []string {
	var keys []string
	depth := 0
	inString := false
	inKey := false
	escaped := false

	bufPtr := keyBufPool.Get().(*[]byte)
	keyBuf := (*bufPtr)[:0]
	defer func() {
		*bufPtr = keyBuf[:0]
		keyBufPool.Put(bufPtr)
	}()

	for i := 0; i < len(body); i++ {
		c := body[i]

		if escaped {
			if inKey {
				keyBuf = append(keyBuf, c)
			}
			escaped = false
			continue
		}

		if c == '\\' {
			escaped = true
			if inKey {
				keyBuf = append(keyBuf, c)
			}
			continue
		}

		if c == '"' {
			if !inString {
				inString = true
				if depth == 1 && i > 0 {
					prev := findPrevNonWhitespace(body, i-1)
					if prev == '{' || prev == ',' {
						inKey = true
						keyBuf = keyBuf[:0]
					}
				}
			} else {
				inString = false
				if inKey {
					keys = append(keys, string(keyBuf))
					inKey = false
				}
			}
			continue
		}

		if inString {
			if inKey {
				keyBuf = append(keyBuf, c)
			}
			continue
		}

		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
		} else if c == '[' {
			depth++
		} else if c == ']' {
			depth--
		}
	}

	return keys
}

func findPrevNonWhitespace(body []byte, start int) byte {
	for i := start; i >= 0; i-- {
		c := body[i]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return c
		}
	}
	return 0
}

var defaultValidators = map[string]*ValidatorConfig{
	"/v1/history/get_actions": {
		AllowedBodyKeys: []string{"account_name", "pos", "offset", "filter", "sort", "after", "before", "parent"},
	},
	"/v1/history/get_transaction": {
		AllowedBodyKeys: []string{"id"},
	},
}

var defaultActivityValidator = &ValidatorConfig{
	AllowedQueryParams: []string{"limit", "cursor", "order", "contract", "action", "decode", "date", "start_date", "end_date", "trace"},
	AllowedBodyKeys:    []string{"limit", "cursor", "order", "contract", "action", "decode", "date", "start_date", "end_date", "trace"},
}

func GetDefaultValidator(path string) *ValidatorConfig {
	if cfg, ok := defaultValidators[path]; ok {
		return cfg
	}
	if bytes.HasPrefix([]byte(path), []byte("/account/")) {
		return defaultActivityValidator
	}
	if bytes.HasPrefix([]byte(path), []byte("/debug/")) {
		return nil
	}
	return nil
}
