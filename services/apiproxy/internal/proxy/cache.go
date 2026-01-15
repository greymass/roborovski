package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
	"time"
)

type CachedResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	ExpiresAt  time.Time
}

func (c *CachedResponse) IsExpired() bool {
	return time.Now().After(c.ExpiresAt)
}

type Cache struct {
	mu      sync.RWMutex
	entries map[string]*CachedResponse
	enabled bool
}

func NewCache(enabled bool) *Cache {
	c := &Cache{
		entries: make(map[string]*CachedResponse),
		enabled: enabled,
	}
	if enabled {
		go c.cleanupLoop()
	}
	return c
}

func (c *Cache) Enabled() bool {
	return c.enabled
}

func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

func (c *Cache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.entries {
		if now.After(entry.ExpiresAt) {
			delete(c.entries, key)
		}
	}
}

func (c *Cache) GenerateKey(r *http.Request, body []byte) string {
	h := sha256.New()
	h.Write([]byte(r.Method))
	h.Write([]byte(r.URL.Path))
	h.Write([]byte(r.URL.RawQuery))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func (c *Cache) Get(key string) (*CachedResponse, bool) {
	if !c.enabled {
		return nil, false
	}

	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if entry.IsExpired() {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}

	return entry, true
}

func (c *Cache) Set(key string, resp *CachedResponse) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	c.entries[key] = resp
	c.mu.Unlock()
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

type CachingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
	headers    http.Header
}

func NewCachingResponseWriter(w http.ResponseWriter) *CachingResponseWriter {
	return &CachingResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
		headers:        make(http.Header),
	}
}

func (w *CachingResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	for k, v := range w.Header() {
		w.headers[k] = v
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *CachingResponseWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

func (w *CachingResponseWriter) ToCachedResponse(duration time.Duration) *CachedResponse {
	for k, v := range w.Header() {
		if _, ok := w.headers[k]; !ok {
			w.headers[k] = v
		}
	}

	return &CachedResponse{
		StatusCode: w.statusCode,
		Headers:    w.headers.Clone(),
		Body:       w.body.Bytes(),
		ExpiresAt:  time.Now().Add(duration),
	}
}

func DrainRequestBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}
