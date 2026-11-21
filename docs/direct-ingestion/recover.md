# Recover

When a writer crashed and nodeos cannot start, you run `coreindex --recover` to bring core storage and the ABI cache back to their last commit. Every writer runs the same recovery when it starts, so in most cases starting the writer again is all a crash needs. Use this command when the writer cannot start, or when coreindex in follower mode reports `head_uncertain` and you want readers back at full service before the writer returns.

```bash
coreindex --recover \
  --index-storage /data/roborovski/core \
  --abi-source /data/roborovski/core/abis
```

The command takes both writer locks, so stop every writer on the store first. It refuses to start while nodeos with the plugin, or coreindex in trace ingestion, holds the store. coreindex in follower mode holds no lock and can keep running.

Recovery removes everything written after the last commit: blocks past the manifest's head in the active slice, slices past it, and ABI entries past the committed extent. It changes nothing at or below the commit. It also finishes a [reset](reset-after-block.md) that was interrupted, and writes `control/published` so readers see the committed view. A store written before the release, with no manifest, is adopted at its last complete block instead.

On success it prints one of:

```
Core storage and the ABI cache match the manifest: block 24851230, epoch 1, 4211 ABI entries
Adopted the store at block 24851230 under epoch 1
```

The blocks it removed were never published, so no reader saw them. The writer writes them again from its own source when it starts: nodeos from its buffer or by replay, coreindex from the trace files.

`--recover` runs on its own. It refuses to combine with `--read-only`, `--replay`, `--preflight`, `--reset-after-block` or the verify, repair and ABI rebuild modes.
