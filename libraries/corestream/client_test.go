package corestream

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type mockBlockProvider struct {
	mu      sync.RWMutex
	blocks  map[uint32][]byte
	lib     uint32
	head    uint32
	actions map[uint64]ActionData
}

func newMockBlockProvider(startBlock, endBlock uint32) *mockBlockProvider {
	p := &mockBlockProvider{
		blocks:  make(map[uint32][]byte),
		actions: make(map[uint64]ActionData),
		lib:     endBlock,
		head:    endBlock + 10,
	}
	for i := startBlock; i <= endBlock; i++ {
		p.blocks[i] = []byte(fmt.Sprintf("block-%d-data", i))
	}
	return p
}

func (p *mockBlockProvider) GetBlock(blockNum uint32) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if data, ok := p.blocks[blockNum]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (p *mockBlockProvider) GetBlockByGlob(glob uint64) (uint32, error) {
	return uint32(glob / 1000), nil
}

func (p *mockBlockProvider) GetActionsByGlobs(globalSeqs []uint64) ([]ActionData, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var result []ActionData
	for _, seq := range globalSeqs {
		if action, ok := p.actions[seq]; ok {
			result = append(result, action)
		}
	}
	return result, nil
}

func (p *mockBlockProvider) GetLIB() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lib
}

func (p *mockBlockProvider) GetHead() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.head
}

func (p *mockBlockProvider) SetLIB(lib uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lib = lib
}

func (p *mockBlockProvider) SetHead(head uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = head
}

func (p *mockBlockProvider) AddBlock(blockNum uint32, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocks[blockNum] = data
}

func (p *mockBlockProvider) AddAction(globalSeq uint64, action ActionData) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.actions[globalSeq] = action
}

func TestDefaultClientConfig(t *testing.T) {
	cfg := DefaultClientConfig()

	if cfg.ReconnectDelay != 1*time.Second {
		t.Errorf("ReconnectDelay = %v, want 1s", cfg.ReconnectDelay)
	}
	if cfg.ReconnectMaxDelay != 30*time.Second {
		t.Errorf("ReconnectMaxDelay = %v, want 30s", cfg.ReconnectMaxDelay)
	}
	if cfg.AckInterval != 1000 {
		t.Errorf("AckInterval = %d, want 1000", cfg.AckInterval)
	}
	if cfg.Debug {
		t.Error("Debug should be false by default")
	}
}

func TestNewClient(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:9999", cfg)

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.address != "localhost:9999" {
		t.Errorf("address = %q, want %q", client.address, "localhost:9999")
	}
	if client.recvChan == nil {
		t.Error("recvChan should not be nil")
	}
	if client.closeChan == nil {
		t.Error("closeChan should not be nil")
	}
}

func TestClientConnect_NoServer(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	err := client.Connect(1)
	if err == nil {
		t.Error("Connect should fail when no server is running")
	}
}

func TestClientClose_NotConnected(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	err := client.Close()
	if err != nil {
		t.Errorf("Close should succeed even if never connected: %v", err)
	}
}

func TestClientClose_AlreadyClosed(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	client.Close()
	err := client.Close()
	if !errors.Is(err, ErrAlreadyClosed) {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestClientConnect_AlreadyClosed(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	client.Close()
	err := client.Connect(1)
	if !errors.Is(err, ErrAlreadyClosed) {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestClientGetters_NotConnected(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	if client.IsConnected() {
		t.Error("IsConnected should return false when not connected")
	}
	if client.GetLIB() != 0 {
		t.Errorf("GetLIB should return 0 when not connected, got %d", client.GetLIB())
	}
	if client.GetHEAD() != 0 {
		t.Errorf("GetHEAD should return 0 when not connected, got %d", client.GetHEAD())
	}
}

func TestClientQueryState_NotConnected(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	_, _, err := client.QueryState(time.Second)
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}
}

func TestClientQueryBlock_NotConnected(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	_, err := client.QueryBlock(100, time.Second)
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}
}

func TestClientQueryBlockBatch_NotConnected(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	_, err := client.QueryBlockBatch(100, 200, time.Second)
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}
}

func TestClientQueryGlobs_NotConnected(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	_, err := client.QueryGlobs([]uint64{100, 200}, time.Second)
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}
}

func TestClientQueryGlobs_TooMany(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	seqs := make([]uint64, 1001)
	_, err := client.QueryGlobs(seqs, time.Second)
	if err == nil {
		t.Error("expected error for too many seqs")
	}
	if err.Error() != "max 1000 global seqs per query" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewServer(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: time.Second,
	}
	server := NewServer(provider, cfg)

	if server == nil {
		t.Fatal("NewServer returned nil")
	}
	if server.provider != provider {
		t.Error("provider not set correctly")
	}
	if server.config.MaxClients != 10 {
		t.Errorf("MaxClients = %d, want 10", server.config.MaxClients)
	}
}

func TestServerClose_NotStarted(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)

	err := server.Close()
	if err != nil {
		t.Errorf("Close should succeed: %v", err)
	}
}

func TestServerClose_AlreadyClosed(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)

	server.Close()
	err := server.Close()
	if err == nil {
		t.Error("expected error on double close")
	}
}

func TestServerLiveMode(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)

	if server.IsLiveMode() {
		t.Error("server should not be in live mode by default")
	}

	server.SetLiveMode(true)
	if !server.IsLiveMode() {
		t.Error("server should be in live mode after SetLiveMode(true)")
	}

	server.SetLiveMode(false)
	if server.IsLiveMode() {
		t.Error("server should not be in live mode after SetLiveMode(false)")
	}

	server.Close()
}

func TestServerBroadcast_NotLive(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)
	defer server.Close()

	server.Broadcast(50, []byte("test"))
}

func TestServerBroadcast_Closed(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)
	server.Close()

	server.Broadcast(50, []byte("test"))
}

func TestServerGetStats(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)
	defer server.Close()

	stats := server.GetStats()
	if stats.ClientCount != 0 {
		t.Errorf("ClientCount = %d, want 0", stats.ClientCount)
	}
	if stats.TotalBroadcasts != 0 {
		t.Errorf("TotalBroadcasts = %d, want 0", stats.TotalBroadcasts)
	}
}

func TestServerLogStats_NoClients(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)
	defer server.Close()

	server.LogStats()
}

func getTestSocketPath(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "corestream-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func TestServerListen_UnixSocket(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	err := server.Listen(sockPath)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	if _, err := os.Stat(sockPath); os.IsNotExist(err) {
		t.Error("socket file should exist")
	}

	server.Close()
}

func TestServerListen_TCP(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{MaxClients: 10, HeartbeatInterval: time.Second}
	server := NewServer(provider, cfg)

	err := server.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	server.Close()
}

func TestClientServer_Connection(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 100 * time.Millisecond,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       10,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(1); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	if !client.IsConnected() {
		t.Error("client should be connected")
	}

	if client.GetLIB() != 100 {
		t.Errorf("GetLIB = %d, want 100", client.GetLIB())
	}
	if client.GetHEAD() != 110 {
		t.Errorf("GetHEAD = %d, want 110", client.GetHEAD())
	}
}

func TestClientServer_NextBlock(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       10,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(1); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	for i := uint32(1); i <= 10; i++ {
		blockNum, data, err := client.NextBlockWithTimeout(2 * time.Second)
		if err != nil {
			t.Fatalf("NextBlockWithTimeout failed at block %d: %v", i, err)
		}
		if blockNum != i {
			t.Errorf("blockNum = %d, want %d", blockNum, i)
		}
		expected := []byte(fmt.Sprintf("block-%d-data", i))
		if !bytes.Equal(data, expected) {
			t.Errorf("data = %q, want %q", data, expected)
		}
	}
}

func TestClientServer_NextBlockWithTimeout_Timeout(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(100); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	for i := 0; i < 100; i++ {
		client.NextBlockWithTimeout(10 * time.Millisecond)
	}

	blockNum, data, err := client.NextBlockWithTimeout(100 * time.Millisecond)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if blockNum != 0 || data != nil {
		t.Error("expected timeout to return zero values")
	}
}

func TestClientServer_QueryState(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	head, lib, err := client.QueryState(2 * time.Second)
	if err != nil {
		t.Fatalf("QueryState failed: %v", err)
	}
	if head != 110 {
		t.Errorf("head = %d, want 110", head)
	}
	if lib != 100 {
		t.Errorf("lib = %d, want 100", lib)
	}
}

func TestClientServer_QueryBlock(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	resp, err := client.QueryBlock(75, 2*time.Second)
	if err != nil {
		t.Fatalf("QueryBlock failed: %v", err)
	}
	if resp.BlockNum != 75 {
		t.Errorf("BlockNum = %d, want 75", resp.BlockNum)
	}
	expected := []byte("block-75-data")
	if !bytes.Equal(resp.Data, expected) {
		t.Errorf("Data = %q, want %q", resp.Data, expected)
	}
}

func TestClientServer_QueryBlockBatch(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	blocks, err := client.QueryBlockBatch(10, 15, 2*time.Second)
	if err != nil {
		t.Fatalf("QueryBlockBatch failed: %v", err)
	}
	if len(blocks) != 6 {
		t.Errorf("got %d blocks, want 6", len(blocks))
	}

	for i, block := range blocks {
		expected := []byte(fmt.Sprintf("block-%d-data", 10+i))
		if !bytes.Equal(block, expected) {
			t.Errorf("block[%d] = %q, want %q", i, block, expected)
		}
	}
}

func TestClientServer_QueryGlobs(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	provider.AddAction(1001, ActionData{
		GlobalSeq: 1001,
		BlockNum:  50,
		BlockTime: 1700000000,
		Data:      []byte("action-1001"),
	})
	provider.AddAction(1002, ActionData{
		GlobalSeq: 1002,
		BlockNum:  51,
		BlockTime: 1700000001,
		Data:      []byte("action-1002"),
	})

	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	resp, err := client.QueryGlobs([]uint64{1001, 1002}, 2*time.Second)
	if err != nil {
		t.Fatalf("QueryGlobs failed: %v", err)
	}
	if len(resp.Actions) != 2 {
		t.Errorf("got %d actions, want 2", len(resp.Actions))
	}
	if resp.LIB != 100 {
		t.Errorf("LIB = %d, want 100", resp.LIB)
	}
	if resp.HEAD != 110 {
		t.Errorf("HEAD = %d, want 110", resp.HEAD)
	}
}

func TestClientServer_QueryGlobs_Empty(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	resp, err := client.QueryGlobs([]uint64{}, 2*time.Second)
	if err != nil {
		t.Fatalf("QueryGlobs failed: %v", err)
	}
	if len(resp.Actions) != 0 {
		t.Errorf("expected 0 actions, got %d", len(resp.Actions))
	}
}

func TestServer_RejectWhenSyncing(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer server.Close()

	clientCfg := DefaultClientConfig()
	client := NewClient(sockPath, clientCfg)

	err := client.Connect(1)
	if err == nil {
		client.Close()
		t.Error("expected connection to be rejected when server is syncing")
	}
}

func TestServer_RejectStartBlockAheadOfLIB(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := DefaultClientConfig()
	client := NewClient(sockPath, clientCfg)

	err := client.Connect(500)
	if err == nil {
		client.Close()
		t.Error("expected connection to be rejected when start block ahead of LIB")
	}
}

func TestServer_RejectBlockNotFound(t *testing.T) {
	provider := newMockBlockProvider(50, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := DefaultClientConfig()
	client := NewClient(sockPath, clientCfg)

	err := client.Connect(10)
	if err == nil {
		client.Close()
		t.Error("expected connection to be rejected when start block not found")
	}
}

func TestServer_MaxClients(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        2,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}

	client1 := NewClient(sockPath, clientCfg)
	if err := client1.Connect(50); err != nil {
		t.Fatalf("Client 1 connect failed: %v", err)
	}
	defer client1.Close()

	client2 := NewClient(sockPath, clientCfg)
	if err := client2.Connect(50); err != nil {
		t.Fatalf("Client 2 connect failed: %v", err)
	}
	defer client2.Close()

	time.Sleep(50 * time.Millisecond)

	client3 := NewClient(sockPath, clientCfg)
	err := client3.Connect(50)
	if err == nil {
		client3.Close()
		t.Error("expected client 3 to be rejected due to max clients")
	}
}

func TestServer_InvalidInitialMessage(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	WriteMessage(conn, MsgTypeHeartbeat, []byte{})

	conn.SetReadDeadline(time.Now().Add(time.Second))
	msgType, _, err := ReadMessage(conn)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if msgType != MsgTypeError {
		t.Errorf("expected error message, got type %d", msgType)
	}
}

func TestServer_Broadcast(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(100); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	for i := 0; i < 100; i++ {
		client.NextBlockWithTimeout(10 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)

	provider.AddBlock(101, []byte("new-block-101"))
	provider.SetLIB(101)
	server.Broadcast(101, []byte("new-block-101"))

	blockNum, data, err := client.NextBlockWithTimeout(2 * time.Second)
	if err != nil {
		t.Fatalf("NextBlockWithTimeout failed: %v", err)
	}
	if blockNum != 101 {
		t.Errorf("blockNum = %d, want 101", blockNum)
	}
	if !bytes.Equal(data, []byte("new-block-101")) {
		t.Errorf("data = %q, want %q", data, "new-block-101")
	}
}

func TestNextBlock_AfterClose(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	client.Close()

	_, _, err := client.NextBlock()
	if !errors.Is(err, ErrAlreadyClosed) {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestNextBlockWithTimeout_AfterClose(t *testing.T) {
	cfg := DefaultClientConfig()
	client := NewClient("localhost:59999", cfg)

	client.Close()

	_, _, err := client.NextBlockWithTimeout(time.Second)
	if !errors.Is(err, ErrAlreadyClosed) {
		t.Errorf("expected ErrAlreadyClosed, got %v", err)
	}
}

func TestQueryState_AfterClose(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	client.Close()

	_, _, err := client.QueryState(time.Second)
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestQueryBlock_AfterClose(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	client.Close()

	_, err := client.QueryBlock(75, time.Second)
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestQueryBlockBatch_AfterClose(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	client.Close()

	_, err := client.QueryBlockBatch(10, 15, time.Second)
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestQueryGlobs_AfterClose(t *testing.T) {
	provider := newMockBlockProvider(1, 100)
	cfg := ServerConfig{
		MaxClients:        10,
		HeartbeatInterval: 5 * time.Second,
	}
	server := NewServer(provider, cfg)

	sockPath := getTestSocketPath(t)
	if err := server.Listen(sockPath); err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	server.SetLiveMode(true)
	defer server.Close()

	clientCfg := ClientConfig{
		ReconnectDelay:    100 * time.Millisecond,
		ReconnectMaxDelay: 1 * time.Second,
		AckInterval:       1000,
	}
	client := NewClient(sockPath, clientCfg)

	if err := client.Connect(50); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	client.Close()

	_, err := client.QueryGlobs([]uint64{100, 200}, time.Second)
	if err == nil {
		t.Error("expected error after close")
	}
}
