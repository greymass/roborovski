// Package fault stops the writer at a named persistence boundary when ROBOROVSKI_WRITER_FAULT names one.
package fault

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/greymass/roborovski/libraries/logger"
)

const EnvVar = "ROBOROVSKI_WRITER_FAULT"

var Points = []string{
	"abi_synced", "core_synced", "manifest_dir_synced", "slice_rotated",
	"reset_manifest_written", "reset_slices_removed", "reset_abi_index_replaced",
}

type Spec struct {
	Point    string
	Pause    bool
	MinBlock uint32
}

func Parse(text string) (*Spec, error) {
	if text == "" {
		return nil, nil
	}
	parts := strings.Split(text, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return nil, fmt.Errorf("%s must be <point>:<kill|pause>[:<block>]", EnvVar)
	}
	s := &Spec{Point: parts[0]}
	known := false
	for _, p := range Points {
		known = known || p == s.Point
	}
	if !known {
		return nil, fmt.Errorf("%s names an unknown point: %s", EnvVar, s.Point)
	}
	switch parts[1] {
	case "pause":
		s.Pause = true
	case "kill":
	default:
		return nil, fmt.Errorf("%s action must be kill or pause, not %s", EnvVar, parts[1])
	}
	if len(parts) == 3 {
		n, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s block: %w", EnvVar, err)
		}
		s.MinBlock = uint32(n)
	}
	return s, nil
}

var (
	mu       sync.Mutex
	active   *Spec
	observer func(point string, block uint32)
)

// Configure reads the environment once at startup and warns when a fault is armed.
func Configure() error {
	s, err := Parse(os.Getenv(EnvVar))
	if err != nil {
		return err
	}
	mu.Lock()
	active = s
	mu.Unlock()
	if s != nil {
		action := "be killed"
		if s.Pause {
			action = "pause"
		}
		logger.Printf("warning", "%s is set: the writer will %s at %s from block %d", EnvVar, action, s.Point, s.MinBlock)
	}
	return nil
}

func SetObserver(o func(point string, block uint32)) {
	mu.Lock()
	observer = o
	mu.Unlock()
}

func Point(point string, block uint32) {
	mu.Lock()
	s, o := active, observer
	mu.Unlock()
	if o != nil {
		o(point, block)
	}
	if s == nil || s.Point != point || block < s.MinBlock {
		return
	}
	logger.Printf("warning", "%s reached %s at block %d", EnvVar, point, block)
	if s.Pause {
		select {}
	}
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}
