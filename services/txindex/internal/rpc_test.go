package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/abicache"
	"github.com/greymass/roborovski/libraries/chain"
)

func noopTrace() chain.ActionTrace {
	return chain.ActionTrace{
		Act: chain.Action{
			Account:       "greymassnoop",
			Name:          "noop",
			Authorization: []chain.PermissionLevel{{Actor: "greymassfuel", Permission: "cosign"}},
			Data:          "",
		},
	}
}

func TestActionToMapAlwaysSetsHexDataWithoutABIReader(t *testing.T) {
	s := &RPCServer{}
	actMap := s.actionToMap(noopTrace(), 100)["act"].(map[string]any)
	hexData, ok := actMap["hex_data"]
	if !ok {
		t.Fatal("hex_data missing")
	}
	if hexData != "" {
		t.Fatalf("hex_data = %#v, want empty string", hexData)
	}
	if actMap["data"] != "" {
		t.Fatalf("data = %#v, want raw hex when no ABI reader", actMap["data"])
	}
}

func TestActionToMapAlwaysSetsHexDataWhenDecodeFails(t *testing.T) {
	dir := t.TempDir()
	w, err := abicache.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := abicache.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	s := &RPCServer{abiReader: r}
	trace := noopTrace()
	trace.Act.Data = "deadbeef"
	actMap := s.actionToMap(trace, 100)["act"].(map[string]any)
	if actMap["hex_data"] != "deadbeef" {
		t.Fatalf("hex_data = %#v, want raw hex even when ABI decode fails", actMap["hex_data"])
	}
	if actMap["data"] != "deadbeef" {
		t.Fatalf("data = %#v, want raw hex fallback", actMap["data"])
	}
}
