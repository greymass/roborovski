package proxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/greymass/roborovski/services/apiproxy/internal/metrics"
	"github.com/sony/gobreaker"
	"golang.org/x/sync/singleflight"
)

type Backend struct {
	URL            *url.URL
	ReverseProxy   *httputil.ReverseProxy
	CircuitBreaker *gobreaker.CircuitBreaker
	Transport      *http.Transport
	CacheDuration  time.Duration
	Validator      *RouteValidator
}

type Proxy struct {
	backends     map[string]*Backend
	singleflight *singleflight.Group
	cache        *Cache
}

func NewProxy(cacheEnabled bool) *Proxy {
	return &Proxy{
		backends:     make(map[string]*Backend),
		singleflight: &singleflight.Group{},
		cache:        NewCache(cacheEnabled),
	}
}

func (p *Proxy) AddBackend(path, backend string, timeout int, cacheDuration int, validatorCfg *ValidatorConfig, cbSettings gobreaker.Settings) error {
	parsedURL, err := url.Parse(backend)
	if err != nil {
		return fmt.Errorf("invalid backend URL %s: %w", backend, err)
	}

	var transport *http.Transport
	if parsedURL.Scheme == "unix" {
		transport = &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", parsedURL.Path)
			},
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: time.Duration(timeout) * time.Second,
		}
	} else {
		transport = &http.Transport{
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: time.Duration(timeout) * time.Second,
		}
	}

	reverseProxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			if parsedURL.Scheme == "unix" {
				req.URL.Scheme = "http"
				req.URL.Host = "unix"
			} else {
				req.URL.Scheme = parsedURL.Scheme
				req.URL.Host = parsedURL.Host
			}
			req.Host = req.URL.Host
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("Proxy error for %s: %v", path, err)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		},
	}

	cb := gobreaker.NewCircuitBreaker(cbSettings)

	p.backends[path] = &Backend{
		URL:            parsedURL,
		ReverseProxy:   reverseProxy,
		CircuitBreaker: cb,
		Transport:      transport,
		CacheDuration:  time.Duration(cacheDuration) * time.Second,
		Validator:      NewRouteValidator(validatorCfg),
	}

	return nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	backend, ok := p.backends[r.URL.Path]
	if !ok {
		for path, b := range p.backends {
			if matchPath(r.URL.Path, path) {
				backend = b
				break
			}
		}
	}

	if backend == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	var body []byte
	var err error

	if backend.Validator.Enabled() || (p.cache.Enabled() && backend.CacheDuration > 0) {
		body, err = DrainRequestBody(r)
		if err != nil {
			log.Printf("Failed to read request body: %v", err)
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
	}

	if backend.Validator.Enabled() {
		if !backend.Validator.ValidateQuery(r.URL.Query()) {
			metrics.ValidationRejected.WithLabelValues(r.URL.Path, "query").Inc()
			log.Printf("REJECTED invalid query: %s %s", r.Method, r.URL.Path)
			http.Error(w, "Bad Request: invalid query parameters", http.StatusBadRequest)
			return
		}

		if !backend.Validator.ValidateBody(body) {
			metrics.ValidationRejected.WithLabelValues(r.URL.Path, "body").Inc()
			log.Printf("REJECTED invalid body: %s %s", r.Method, r.URL.Path)
			http.Error(w, "Bad Request: invalid request body", http.StatusBadRequest)
			return
		}
	}

	cacheEnabled := p.cache.Enabled() && backend.CacheDuration > 0
	var cacheKey string

	if cacheEnabled {
		cacheKey = p.cache.GenerateKey(r, body)

		if cached, ok := p.cache.Get(cacheKey); ok {
			metrics.CacheHits.Inc()
			log.Printf("CACHE HIT: %s %s", r.Method, r.URL.Path)

			for k, v := range cached.Headers {
				for _, val := range v {
					w.Header().Add(k, val)
				}
			}
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(cached.StatusCode)
			w.Write(cached.Body)
			return
		}

		metrics.CacheMisses.Inc()
		log.Printf("CACHE MISS: %s %s", r.Method, r.URL.Path)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	r = r.WithContext(ctx)

	var crw *CachingResponseWriter
	var responseWriter http.ResponseWriter

	if cacheEnabled {
		crw = NewCachingResponseWriter(w)
		crw.Header().Set("X-Cache", "MISS")
		responseWriter = crw
	} else {
		responseWriter = w
	}

	_, err = backend.CircuitBreaker.Execute(func() (interface{}, error) {
		backend.ReverseProxy.ServeHTTP(responseWriter, r)
		return nil, nil
	})

	if err != nil {
		log.Printf("Circuit breaker error: %v", err)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	if cacheEnabled && crw != nil && crw.statusCode >= 200 && crw.statusCode < 300 {
		cached := crw.ToCachedResponse(backend.CacheDuration)
		p.cache.Set(cacheKey, cached)
	}
}

func matchPath(requestPath, routePath string) bool {
	return len(requestPath) >= len(routePath) && requestPath[:len(routePath)] == routePath
}
