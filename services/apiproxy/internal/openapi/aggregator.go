package openapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type RouteConfig struct {
	Path    string
	Backend string
}

type Aggregator struct {
	routes      []RouteConfig
	title       string
	description string
	version     string

	mu         sync.RWMutex
	cachedSpec []byte
	cachedAt   time.Time
	cacheTTL   time.Duration
}

func NewAggregator(title, description, version string, cacheTTL time.Duration) *Aggregator {
	return &Aggregator{
		title:       title,
		description: description,
		version:     version,
		cacheTTL:    cacheTTL,
	}
}

func (a *Aggregator) AddRoute(path, backend string) {
	a.routes = append(a.routes, RouteConfig{
		Path:    path,
		Backend: backend,
	})
}

func (a *Aggregator) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spec, err := a.GetSpec(r.Context())
		if err != nil {
			log.Printf("Failed to generate OpenAPI spec: %v", err)
			http.Error(w, "Failed to generate OpenAPI spec", http.StatusInternalServerError)
			return
		}

		format := r.URL.Query().Get("format")
		accept := r.Header.Get("Accept")

		if format == "yaml" || (format == "" && strings.Contains(accept, "yaml")) {
			w.Header().Set("Content-Type", "application/x-yaml")
		} else {
			w.Header().Set("Content-Type", "application/json")
		}

		w.Write(spec)
	})
}

func (a *Aggregator) GetSpec(ctx context.Context) ([]byte, error) {
	a.mu.RLock()
	if a.cachedSpec != nil && time.Since(a.cachedAt) < a.cacheTTL {
		spec := a.cachedSpec
		a.mu.RUnlock()
		return spec, nil
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cachedSpec != nil && time.Since(a.cachedAt) < a.cacheTTL {
		return a.cachedSpec, nil
	}

	spec, err := a.buildSpec(ctx)
	if err != nil {
		return nil, err
	}

	a.cachedSpec = spec
	a.cachedAt = time.Now()
	return spec, nil
}

func (a *Aggregator) buildSpec(ctx context.Context) ([]byte, error) {
	backendSpecs := a.fetchBackendSpecs(ctx)

	merged := map[string]interface{}{
		"openapi": "3.1.0",
		"info": map[string]interface{}{
			"title":       a.title,
			"description": a.description,
			"version":     a.version,
		},
		"paths":      map[string]interface{}{},
		"components": map[string]interface{}{"schemas": map[string]interface{}{}},
	}

	paths := merged["paths"].(map[string]interface{})
	schemas := merged["components"].(map[string]interface{})["schemas"].(map[string]interface{})
	tagsMap := make(map[string]map[string]interface{})

	for _, route := range a.routes {
		spec, ok := backendSpecs[route.Backend]
		if !ok {
			continue
		}

		backendPaths, ok := spec["paths"].(map[string]interface{})
		if !ok {
			continue
		}

		for specPath, pathItem := range backendPaths {
			if a.pathMatchesRoute(specPath, route.Path) {
				paths[specPath] = pathItem
			}
		}

		if components, ok := spec["components"].(map[string]interface{}); ok {
			if backendSchemas, ok := components["schemas"].(map[string]interface{}); ok {
				for name, schema := range backendSchemas {
					schemas[name] = schema
				}
			}
		}

		if backendTags, ok := spec["tags"].([]interface{}); ok {
			for _, t := range backendTags {
				if tagObj, ok := t.(map[string]interface{}); ok {
					if name, ok := tagObj["name"].(string); ok {
						tagsMap[name] = tagObj
					}
				}
			}
		}
	}

	if len(tagsMap) > 0 {
		restTags := []string{}
		legacyTags := []string{}
		debugTags := []string{}

		for name := range tagsMap {
			if strings.HasPrefix(name, "Legacy") {
				legacyTags = append(legacyTags, name)
			} else if strings.HasPrefix(name, "Debug") {
				debugTags = append(debugTags, name)
			} else {
				restTags = append(restTags, name)
			}
		}

		sort.Strings(restTags)
		sort.Strings(legacyTags)
		sort.Strings(debugTags)

		tags := make([]interface{}, 0, len(tagsMap))
		for _, name := range restTags {
			tags = append(tags, tagsMap[name])
		}
		for _, name := range legacyTags {
			tags = append(tags, tagsMap[name])
		}
		for _, name := range debugTags {
			tags = append(tags, tagsMap[name])
		}
		merged["tags"] = tags

		tagGroups := []map[string]interface{}{}
		if len(restTags) > 0 {
			tagGroups = append(tagGroups, map[string]interface{}{
				"name": "REST API",
				"tags": restTags,
			})
		}
		if len(legacyTags) > 0 {
			tagGroups = append(tagGroups, map[string]interface{}{
				"name": "Legacy API",
				"tags": legacyTags,
			})
		}
		if len(debugTags) > 0 {
			tagGroups = append(tagGroups, map[string]interface{}{
				"name": "Debug",
				"tags": debugTags,
			})
		}
		if len(tagGroups) > 0 {
			merged["x-tagGroups"] = tagGroups
		}
	}

	return json.MarshalIndent(merged, "", "  ")
}

func (a *Aggregator) pathMatchesRoute(specPath, routePath string) bool {
	normalizedSpec := normalizePath(specPath)
	normalizedRoute := normalizePath(routePath)

	if normalizedSpec == normalizedRoute {
		return true
	}

	if strings.HasPrefix(normalizedSpec, normalizedRoute) {
		remainder := normalizedSpec[len(normalizedRoute):]
		return remainder == "" || remainder[0] == '/'
	}

	return false
}

func normalizePath(p string) string {
	p = strings.TrimSuffix(p, "/")
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

func (a *Aggregator) fetchBackendSpecs(ctx context.Context) map[string]map[string]interface{} {
	uniqueBackends := make(map[string]struct{})
	for _, route := range a.routes {
		uniqueBackends[route.Backend] = struct{}{}
	}

	results := make(map[string]map[string]interface{})
	var mu sync.Mutex
	var wg sync.WaitGroup

	for backend := range uniqueBackends {
		wg.Add(1)
		go func(backend string) {
			defer wg.Done()

			spec, err := a.fetchSpec(ctx, backend)
			if err != nil {
				log.Printf("Failed to fetch OpenAPI spec from %s: %v", backend, err)
				return
			}

			mu.Lock()
			results[backend] = spec
			mu.Unlock()
		}(backend)
	}

	wg.Wait()
	return results
}

func (a *Aggregator) fetchSpec(ctx context.Context, backend string) (map[string]interface{}, error) {
	parsedURL, err := url.Parse(backend)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL: %w", err)
	}

	var client *http.Client
	var reqURL string

	if parsedURL.Scheme == "unix" {
		client = &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", parsedURL.Path)
				},
			},
			Timeout: 5 * time.Second,
		}
		reqURL = "http://unix/openapi.json"
	} else {
		client = &http.Client{Timeout: 5 * time.Second}
		reqURL = strings.TrimSuffix(backend, "/") + "/openapi.json"
	}

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var spec map[string]interface{}
	if err := json.Unmarshal(body, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return spec, nil
}

func (a *Aggregator) GetRoutes() []string {
	routes := make([]string, len(a.routes))
	for i, r := range a.routes {
		routes[i] = r.Path
	}
	sort.Strings(routes)
	return routes
}
