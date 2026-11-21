package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/server"
	"github.com/greymass/roborovski/services/coreindex/internal/follower"
)

func TestFollowerRefusesWriterFlags(t *testing.T) {
	cfg := &Config{RepairSlice: -1}
	if got := refusedFollowerFlags(cfg); len(got) != 0 {
		t.Fatalf("defaults refused: %v", got)
	}
	cfg = &Config{Replay: true, RepairSlice: 3, RebuildABIs: true, StartBlock: 5, TraceCleanupMode: "delete", SliceDownloadURL: "http://x"}
	got := strings.Join(refusedFollowerFlags(cfg), " ")
	for _, flag := range []string{"--replay", "--repair-slice", "--rebuild-abis", "--start-block", "--trace-cleanup-mode", "--slice-download-url"} {
		if !strings.Contains(got, flag) {
			t.Errorf("%s not refused (refused: %s)", flag, got)
		}
	}
}

func TestFollowerGetInfoBeforeFirstManifest(t *testing.T) {
	store := follower.NewStore(t.TempDir(), corereader.QueryReaderOptions(), follower.Hooks{})
	if err := store.Poll(); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	cfg := &server.Config{AcceptHTTP: true}
	doHandleRPC(cfg, store, nil, rec, httptest.NewRequest("GET", "/v1/history/get_info", nil))
	if rec.Code != 503 {
		t.Fatalf("get_info status %d, want 503", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["state"] != "not_ready" || body["error"] != "no published view" {
		t.Fatalf("get_info body %v", body)
	}

	rec = httptest.NewRecorder()
	doHandleRPC(cfg, store, nil, rec, httptest.NewRequest("GET", "/abi/eosio", nil))
	if rec.Code != 503 {
		t.Fatalf("/abi status %d, want 503", rec.Code)
	}
}
