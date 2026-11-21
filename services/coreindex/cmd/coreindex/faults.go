package main

import (
	"github.com/greymass/roborovski/services/coreindex/internal/appendlog"
	"github.com/greymass/roborovski/services/coreindex/internal/fault"
	"github.com/greymass/roborovski/services/coreindex/internal/publish"
)

func armWriterFaults(store *appendlog.SliceStore, publisher *publish.Publisher) {
	store.SetRotateHook(func(firstBlock uint32) { fault.Point("slice_rotated", firstBlock) })
	publisher.FaultHook = func(step publish.Step) error {
		switch step {
		case publish.StepABISynced:
			fault.Point("abi_synced", store.WrittenHead())
		case publish.StepCoreSynced:
			fault.Point("core_synced", store.WrittenHead())
		case publish.StepManifestWritten:
			fault.Point("manifest_dir_synced", publisher.Manifest().Head)
		}
		return nil
	}
}

func resetFaults(boundary uint32) func(publish.ResetStep) error {
	return func(step publish.ResetStep) error {
		switch step {
		case publish.ResetManifestWritten:
			fault.Point("reset_manifest_written", boundary)
		case publish.ResetSlicesRemoved:
			fault.Point("reset_slices_removed", boundary)
		case publish.ResetABIIndexReplaced:
			fault.Point("reset_abi_index_replaced", boundary)
		}
		return nil
	}
}
