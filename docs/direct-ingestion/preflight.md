# Preflight

Before a [cutover](cutover.md) you run `coreindex --preflight` against the store and the inputs the replacement writer will use. It runs while the current writer keeps writing, reads the store without changing anything, and prints a JSON report that says whether the handoff can proceed.

## Usage

Before handing the store to nodeos with the plugin, pass the snapshot nodeos will start from, described by `spring-util snapshot info`:

```bash
spring-util snapshot info /data/snapshots/snapshot-24849000.bin > /tmp/snapshot-info.json
coreindex --preflight --preflight-target direct \
  --index-storage /data/roborovski/core \
  --abi-source /data/roborovski/core/abis \
  --chain-id <chain id> \
  --snapshot-info /tmp/snapshot-info.json
```

Before handing the store back to trace ingestion, pass the trace directory coreindex will read and its stride:

```bash
coreindex --preflight --preflight-target trace \
  --index-storage /data/roborovski/core \
  --abi-source /data/roborovski/core/abis \
  --chain-id <chain id> \
  --trace-dir /data/nodeos/traces --stride 10000
```

`--stride` must equal the trace nodeos's `trace-slice-stride`. With a different stride coreindex looks for trace files under the wrong names.

## Exit status

| Exit | `result` | Next step |
|------|----------|-----------|
| 0 | `ready` | Continue with the handoff. Read `unestablished` for the facts the preflight could not check. |
| 2 | `blocked` | Resolve each entry in `blockers`, then run the preflight again. |
| 3 | `inconclusive` | The published view changed during five attempts in a row. Run it again. |

## Report

The report names the `boundary` it inspected (the published `head`, `head_id`, `epoch`, the `store` UUID and `chain_id`), whether a writer holds the store (`writer_active`), each check with `pass`, `fail` or `unknown`, and the `blockers` and `unestablished` lists drawn from them.

| Check | What it establishes |
|-------|---------------------|
| `paths` | Core storage and the ABI cache exist and are separate directories. |
| `geometry` | Slices are 10,000 blocks, as direct ingestion requires. |
| `binding` | Core storage and the ABI cache belong to the same store. |
| `manifest` | The manifest is readable and its checksum holds. |
| `head` | The head record matches the manifest's block number and block ID. |
| `abi cache` | The ABI cache holds at least what the manifest commits. |
| `writer` | Whether a writer is running. A running writer passes, because the handoff stops it. |
| `chain` | The store's chain ID matches `--chain-id` and, for direct ingestion, the snapshot's. |
| `snapshot` | Direct ingestion: the snapshot is at or below the head, and its block ID matches the block core storage holds at that height. nodeos then verifies the blocks between the snapshot and the head, skips them, and appends from the next block. |
| `block source` | Direct ingestion: reported as unestablished, because nodeos needs the blocks between the snapshot and the head from its blocks log or its peers and the preflight cannot see either. |
| `trace input` | Trace ingestion: the trace directory holds the block after the head. |

A store written before the release has no manifest yet. Its report sets `boundary.adoption` and inspects the head that a release writer would adopt at its first start.

## Limits

The preflight is a check of compatibility and of resume inputs at one moment. It is not an audit of the history already stored, and the store keeps advancing after it runs, so the boundary it reports is not reserved. The handoff repeats the same checks after the current writer stops, and a writer that finds a mismatch at startup refuses to write and leaves history unchanged.
