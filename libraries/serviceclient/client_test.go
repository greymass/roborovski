package serviceclient

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	if se, ok := err.(*ServiceError); ok {
		switch se.StatusCode {
		case 429, 500, 502, 503, 504:
			return true
		}
		return false
	}

	if _, ok := err.(net.Error); ok {
		return true
	}

	return false
}

func TestNewClient_HTTP(t *testing.T) {
	client := New("https://example.com", 5*time.Second)
	if client.baseURL != "https://example.com" {
		t.Errorf("expected baseURL https://example.com, got %s", client.baseURL)
	}
}

func TestNewClient_Unix(t *testing.T) {
	client := New("unix:///tmp/test.sock", 5*time.Second)
	if client.baseURL != "http://localhost" {
		t.Errorf("expected baseURL http://localhost for unix socket, got %s", client.baseURL)
	}
}

func TestPost_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected application/json content type")
		}

		var req map[string]string
		json.NewDecoder(r.Body).Decode(&req)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"echo": req["message"]})
	}))
	defer server.Close()

	client := New(server.URL, 5*time.Second)

	var resp map[string]string
	err := client.Post(context.Background(), "/test", map[string]string{"message": "hello"}, &resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp["echo"] != "hello" {
		t.Errorf("expected echo=hello, got %s", resp["echo"])
	}
}

func TestPost_ServiceError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client := New(server.URL, 5*time.Second)

	err := client.Post(context.Background(), "/test", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	se, ok := err.(*ServiceError)
	if !ok {
		t.Fatalf("expected ServiceError, got %T", err)
	}

	if se.StatusCode != 404 {
		t.Errorf("expected status 404, got %d", se.StatusCode)
	}
}

func TestUnixSocket(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to create unix socket: %v", err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		}),
	}
	go server.Serve(listener)
	defer server.Close()

	client := New("unix://"+socketPath, 5*time.Second)

	var resp map[string]string
	err = client.Post(context.Background(), "/health", nil, &resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %s", resp["status"])
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"500 error", &ServiceError{StatusCode: 500}, true},
		{"503 error", &ServiceError{StatusCode: 503}, true},
		{"404 error", &ServiceError{StatusCode: 404}, false},
		{"400 error", &ServiceError{StatusCode: 400}, false},
		{"429 error", &ServiceError{StatusCode: 429}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryable(tt.err); got != tt.expected {
				t.Errorf("isRetryable() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestServiceError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *ServiceError
		expected string
	}{
		{
			name:     "with body",
			err:      &ServiceError{StatusCode: 404, Message: "Not Found", Body: []byte("resource not found")},
			expected: "service error 404: resource not found",
		},
		{
			name:     "without body",
			err:      &ServiceError{StatusCode: 500, Message: "Internal Server Error", Body: nil},
			expected: "service error 500: Internal Server Error",
		},
		{
			name:     "empty body",
			err:      &ServiceError{StatusCode: 503, Message: "Service Unavailable", Body: []byte{}},
			expected: "service error 503: Service Unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestPost_NilResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ignored": "response"}`))
	}))
	defer server.Close()

	client := New(server.URL, 5*time.Second)

	err := client.Post(context.Background(), "/test", map[string]string{"key": "value"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPost_DecodeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	client := New(server.URL, 5*time.Second)

	var resp map[string]string
	err := client.Post(context.Background(), "/test", map[string]string{}, &resp)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !contains(err.Error(), "decode response") {
		t.Errorf("expected decode error message, got: %v", err)
	}
}

func TestPost_ConnectionError(t *testing.T) {
	client := New("http://127.0.0.1:1", 100*time.Millisecond)

	err := client.Post(context.Background(), "/test", nil, nil)
	if err == nil {
		t.Fatal("expected connection error")
	}
	if !contains(err.Error(), "do request") {
		t.Errorf("expected do request error, got: %v", err)
	}
}

func TestNewClient_InvalidURL(t *testing.T) {
	client := New("://invalid", 5*time.Second)
	if client.baseURL != "://invalid" {
		t.Errorf("expected baseURL to be preserved for invalid URL, got %s", client.baseURL)
	}
}

func TestPost_ServiceErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "bad request details"}`))
	}))
	defer server.Close()

	client := New(server.URL, 5*time.Second)

	err := client.Post(context.Background(), "/test", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	se, ok := err.(*ServiceError)
	if !ok {
		t.Fatalf("expected ServiceError, got %T", err)
	}

	errMsg := se.Error()
	if !contains(errMsg, "bad request details") {
		t.Errorf("expected error to contain body, got: %s", errMsg)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
