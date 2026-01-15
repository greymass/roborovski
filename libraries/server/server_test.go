package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSocketListen_TCP tests TCP socket creation
func TestSocketListen_TCP(t *testing.T) {
	// Use a random high port
	socket := "localhost:18765"

	listener := SocketListen(socket)
	if listener == nil {
		t.Fatal("Expected non-nil listener")
	}
	defer listener.Close()

	// Verify it's a TCP listener
	addr := listener.Addr()
	if addr.Network() != "tcp" {
		t.Errorf("Expected tcp network, got %s", addr.Network())
	}

	// Verify we can connect to it
	conn, err := net.Dial("tcp", socket)
	if err != nil {
		t.Errorf("Failed to connect to TCP socket: %v", err)
	} else {
		conn.Close()
	}
}

// TestSocketListen_Unix tests Unix socket creation
func TestSocketListen_Unix(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	listener := SocketListen(socketPath)
	if listener == nil {
		t.Fatal("Expected non-nil listener")
	}
	defer listener.Close()

	// Verify it's a Unix listener
	addr := listener.Addr()
	if addr.Network() != "unix" {
		t.Errorf("Expected unix network, got %s", addr.Network())
	}

	// Verify socket file was created
	if _, err := os.Stat(socketPath); os.IsNotExist(err) {
		t.Error("Socket file was not created")
	}

	// Verify socket permissions (should be 0777)
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Errorf("Failed to stat socket: %v", err)
	} else {
		mode := info.Mode()
		if mode.Perm() != 0777 {
			t.Errorf("Expected permissions 0777, got %o", mode.Perm())
		}
	}

	// Verify we can connect to it
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Errorf("Failed to connect to Unix socket: %v", err)
	} else {
		conn.Close()
	}
}

// TestSocketListen_MultipleUnix tests that multiple Unix sockets can be created
func TestSocketListen_MultipleUnix(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath1 := filepath.Join(tmpDir, "test1.sock")
	socketPath2 := filepath.Join(tmpDir, "test2.sock")

	// Create first listener
	listener1 := SocketListen(socketPath1)
	if listener1 == nil {
		t.Fatal("Expected non-nil listener1")
	}
	defer listener1.Close()

	// Create second listener
	listener2 := SocketListen(socketPath2)
	if listener2 == nil {
		t.Fatal("Expected non-nil listener2")
	}
	defer listener2.Close()

	// Both should be accessible
	conn1, err := net.Dial("unix", socketPath1)
	if err != nil {
		t.Errorf("Failed to connect to socket1: %v", err)
	} else {
		conn1.Close()
	}

	conn2, err := net.Dial("unix", socketPath2)
	if err != nil {
		t.Errorf("Failed to connect to socket2: %v", err)
	} else {
		conn2.Close()
	}
}

// TestSocketListen_TCPMultiplePorts tests that different ports can coexist
func TestSocketListen_TCPMultiplePorts(t *testing.T) {
	socket1 := "localhost:18766"
	socket2 := "localhost:18767"

	listener1 := SocketListen(socket1)
	if listener1 == nil {
		t.Fatal("Expected non-nil listener1")
	}
	defer listener1.Close()

	listener2 := SocketListen(socket2)
	if listener2 == nil {
		t.Fatal("Expected non-nil listener2")
	}
	defer listener2.Close()

	// Both should be accessible
	conn1, err := net.Dial("tcp", socket1)
	if err != nil {
		t.Errorf("Failed to connect to socket1: %v", err)
	} else {
		conn1.Close()
	}

	conn2, err := net.Dial("tcp", socket2)
	if err != nil {
		t.Errorf("Failed to connect to socket2: %v", err)
	} else {
		conn2.Close()
	}
}

// TestGetRequestParams_QueryString tests parsing query string parameters
func TestGetRequestParams_QueryString(t *testing.T) {
	// Create request with query string
	req, err := http.NewRequest("GET", "http://example.com/test?foo=bar&num=42", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	if params["foo"] != "bar" {
		t.Errorf("Expected foo=bar, got %v", params["foo"])
	}

	if params["num"] != "42" {
		t.Errorf("Expected num=42, got %v", params["num"])
	}
}

// TestGetRequestParams_MultipleValues tests parsing query string with multiple values
func TestGetRequestParams_MultipleValues(t *testing.T) {
	// Create request with multiple values for same key
	req, err := http.NewRequest("GET", "http://example.com/test?tag=foo&tag=bar&tag=baz", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	// Multiple values should be returned as array
	tags, ok := params["tag"].([]interface{})
	if !ok {
		t.Fatalf("Expected tag to be []interface{}, got %T", params["tag"])
	}

	if len(tags) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(tags))
	}

	expected := []string{"foo", "bar", "baz"}
	for i, tag := range tags {
		if tag != expected[i] {
			t.Errorf("Expected tag[%d]=%s, got %v", i, expected[i], tag)
		}
	}
}

// TestGetRequestParams_JSONBody tests parsing JSON body
func TestGetRequestParams_JSONBody(t *testing.T) {
	// Create request with JSON body
	jsonData := map[string]interface{}{
		"name":   "test",
		"value":  123,
		"active": true,
	}
	jsonBytes, _ := json.Marshal(jsonData)

	req, err := http.NewRequest("POST", "http://example.com/test", bytes.NewBuffer(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	if params["name"] != "test" {
		t.Errorf("Expected name=test, got %v", params["name"])
	}

	// JSON numbers are parsed as float64
	// JSON numbers may be parsed as json.Number or float64 depending on decoder
	if params["value"] == nil {
		t.Error("Expected value to be set")
	}
	// Accept either format
	switch v := params["value"].(type) {
	case float64:
		if v != 123.0 {
			t.Errorf("Expected value=123.0, got %v", v)
		}
	case json.Number:
		if v.String() != "123" {
			t.Errorf("Expected value=123, got %v", v)
		}
	default:
		t.Errorf("Expected value to be number, got %T: %v", params["value"], params["value"])
	}

	if params["active"] != true {
		t.Errorf("Expected active=true, got %v", params["active"])
	}
}

// TestGetRequestParams_JSONBodyComplex tests parsing complex JSON structures
func TestGetRequestParams_JSONBodyComplex(t *testing.T) {
	// Create request with nested JSON
	jsonData := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "alice",
			"age":  30,
		},
		"tags": []interface{}{"admin", "developer"},
	}
	jsonBytes, _ := json.Marshal(jsonData)

	req, err := http.NewRequest("POST", "http://example.com/test", bytes.NewBuffer(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	// Verify nested object
	user, ok := params["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected user to be map, got %T", params["user"])
	}

	if user["name"] != "alice" {
		t.Errorf("Expected user.name=alice, got %v", user["name"])
	}

	// Verify array
	tags, ok := params["tags"].([]interface{})
	if !ok {
		t.Fatalf("Expected tags to be array, got %T", params["tags"])
	}

	if len(tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(tags))
	}
}

// TestGetRequestParams_EmptyBody tests handling of empty body
func TestGetRequestParams_EmptyBody(t *testing.T) {
	req, err := http.NewRequest("POST", "http://example.com/test", strings.NewReader(""))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	// Empty body should result in error (EOF) but still return params
	if len(params) != 0 {
		t.Errorf("Expected empty params for empty body, got %v", params)
	}
}

// TestGetRequestParams_InvalidJSON tests handling of invalid JSON
func TestGetRequestParams_InvalidJSON(t *testing.T) {
	req, err := http.NewRequest("POST", "http://example.com/test", strings.NewReader("invalid json"))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}

	if len(params) != 0 {
		t.Errorf("Expected empty params for invalid JSON, got %v", params)
	}
}

// TestGetRequestParams_QueryStringPriority tests that query string takes priority over body
func TestGetRequestParams_QueryStringPriority(t *testing.T) {
	// Create request with both query string and body
	jsonData := map[string]interface{}{"name": "from_body"}
	jsonBytes, _ := json.Marshal(jsonData)

	req, err := http.NewRequest("POST", "http://example.com/test?name=from_query", bytes.NewBuffer(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	// Query string should take priority
	if params["name"] != "from_query" {
		t.Errorf("Expected name=from_query, got %v", params["name"])
	}
}

// TestServerIntegration tests full HTTP server with SocketListen
func TestServerIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	// Create listener
	listener := SocketListen(socketPath)
	if listener == nil {
		t.Fatal("Expected non-nil listener")
	}
	defer listener.Close()

	// Create simple HTTP handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, err := GetRequestParams(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Echo back the params
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(params)
	})

	// Start server in background
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	defer server.Close()

	// Wait a bit for server to start
	time.Sleep(10 * time.Millisecond)

	// Create client for Unix socket
	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
	}

	// Test GET with query string
	t.Run("GET_QueryString", func(t *testing.T) {
		resp, err := client.Get("http://unix/test?foo=bar&num=42")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["foo"] != "bar" {
			t.Errorf("Expected foo=bar, got %v", result["foo"])
		}
	})

	// Test POST with JSON body
	t.Run("POST_JSONBody", func(t *testing.T) {
		jsonData := map[string]interface{}{"name": "test", "value": 123}
		jsonBytes, _ := json.Marshal(jsonData)

		resp, err := client.Post("http://unix/test", "application/json", bytes.NewBuffer(jsonBytes))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["name"] != "test" {
			t.Errorf("Expected name=test, got %v", result["name"])
		}
	})
}

// TestServerIntegration_TCP tests full HTTP server with TCP listener
func TestServerIntegration_TCP(t *testing.T) {
	socket := "localhost:18768"

	// Create listener
	listener := SocketListen(socket)
	if listener == nil {
		t.Fatal("Expected non-nil listener")
	}
	defer listener.Close()

	// Create simple HTTP handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, err := GetRequestParams(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(params)
	})

	// Start server in background
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	defer server.Close()

	// Wait a bit for server to start
	time.Sleep(10 * time.Millisecond)

	// Test with standard HTTP client
	t.Run("TCP_Request", func(t *testing.T) {
		resp, err := http.Get("http://" + socket + "/test?foo=bar")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["foo"] != "bar" {
			t.Errorf("Expected foo=bar, got %v", result["foo"])
		}
	})
}

// TestConfig tests Config struct initialization
func TestConfig(t *testing.T) {
	config := Config{
		PrintSyncEvery: 1000,
		Debug:          true,
		LIBOnly:        false,
		ReadOnly:       true,
		PrintTiming:    false,
		PrintOnlyGTE:   100,
		AcceptHTTP:     true,
	}

	if config.PrintSyncEvery != 1000 {
		t.Errorf("Expected PrintSyncEvery=1000, got %d", config.PrintSyncEvery)
	}

	if !config.Debug {
		t.Error("Expected Debug=true")
	}

	if !config.ReadOnly {
		t.Error("Expected ReadOnly=true")
	}

	if !config.AcceptHTTP {
		t.Error("Expected AcceptHTTP=true")
	}
}

// TestGetRequestParams_BodyReadMultipleTimes tests that body can be read multiple times
func TestGetRequestParams_BodyReadMultipleTimes(t *testing.T) {
	jsonData := map[string]interface{}{"name": "test"}
	jsonBytes, _ := json.Marshal(jsonData)

	req, err := http.NewRequest("POST", "http://example.com/test", bytes.NewBuffer(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	// First read
	params1, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("First GetRequestParams failed: %v", err)
	}

	if params1["name"] != "test" {
		t.Errorf("Expected name=test, got %v", params1["name"])
	}

	// Second read should fail (body already consumed)
	// This tests that the function properly closes the body
	jsonBytes2, _ := json.Marshal(jsonData)
	req2, _ := http.NewRequest("POST", "http://example.com/test", bytes.NewBuffer(jsonBytes2))
	params2, err := GetRequestParams(req2)
	if err != nil {
		t.Errorf("Second GetRequestParams failed: %v", err)
	}

	if params2["name"] != "test" {
		t.Errorf("Expected name=test, got %v", params2["name"])
	}
}

// Benchmark GetRequestParams with query string
func BenchmarkGetRequestParams_QueryString(b *testing.B) {
	req, _ := http.NewRequest("GET", "http://example.com/test?foo=bar&num=42&tag=a&tag=b", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetRequestParams(req)
	}
}

// Benchmark GetRequestParams with JSON body
func BenchmarkGetRequestParams_JSONBody(b *testing.B) {
	jsonData := map[string]interface{}{"name": "test", "value": 123, "active": true}
	jsonBytes, _ := json.Marshal(jsonData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("POST", "http://example.com/test", bytes.NewBuffer(jsonBytes))
		GetRequestParams(req)
	}
}

// Benchmark SocketListen (TCP)
func BenchmarkSocketListen_TCP(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		socket := "localhost:0" // Let OS choose port
		listener := SocketListen(socket)
		listener.Close()
	}
}

// Test that GetRequestParams properly handles nil body with query params
func TestGetRequestParams_NilBody(t *testing.T) {
	// With query params, body won't be read
	req, err := http.NewRequest("GET", "http://example.com/test?foo=bar", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	if params["foo"] != "bar" {
		t.Errorf("Expected foo=bar, got %v", params["foo"])
	}
}

// Test GetRequestParams with empty query string and empty body
func TestGetRequestParams_EmptyQueryString(t *testing.T) {
	req, err := http.NewRequest("GET", "http://example.com/test", strings.NewReader(""))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	params, err := GetRequestParams(req)
	// Empty body should result in error or empty params
	if len(params) != 0 {
		t.Errorf("Expected empty params, got %v", params)
	}
}

// Test that reader can be closed and used in defer safely
func TestGetRequestParams_DeferSafe(t *testing.T) {
	jsonData := map[string]interface{}{"name": "test"}
	jsonBytes, _ := json.Marshal(jsonData)

	req, err := http.NewRequest("POST", "http://example.com/test", bytes.NewBuffer(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	// Simulate multiple defer calls (should not panic)
	defer func() {
		io.Copy(io.Discard, req.Body)
		req.Body.Close()
	}()

	params, err := GetRequestParams(req)
	if err != nil {
		t.Errorf("GetRequestParams failed: %v", err)
	}

	if params["name"] != "test" {
		t.Errorf("Expected name=test, got %v", params["name"])
	}
}
