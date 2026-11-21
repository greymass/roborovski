# coreindex follower mode

In direct ingestion you run coreindex with `--read-only` beside the nodeos that writes the store. coreindex then serves the store over its local files, HTTP, the ABI endpoints and corestream, exactly as it does in trace ingestion, and writes nothing in core storage or the ABI cache.

## Configuration

```
read-only = true
index-storage = /data/roborovski/core
abi-source = /data/roborovski/core/abis
chain-id = <64 hex characters>
http-listen = 127.0.0.1:9400
http-socket = /run/roborovski/coreindex.sock
stream-listen = 127.0.0.1:9401
stream-socket = /run/roborovski/corestream.sock
metrics-listen = 127.0.0.1:9402
```

`index-storage` is the plugin's `roborovski-writer-dir` and `abi-source` is its `roborovski-writer-abi-dir`. The full file is [`examples/coreindex-follower.conf`](examples/coreindex-follower.conf).

Follower mode refuses to start with any flag that writes or rebuilds storage: `--replay`, `--verify`, `--verify-only`, `--repair`, `--repair-slice`, `--repair-all`, `--rebuild-abis`, `--slice-download-url`, `--trace-cleanup-mode` and `--start-block`. It exits with one line naming each one it found:

```
--read-only follows storage another process writes; it refuses --replay, --trace-cleanup-mode
```

Remove those flags from the follower's configuration. The trace-ingestion settings (`trace-dir`, `stride`, `workers`, `zstd`) are ignored in follower mode, so one file can serve both modes during a cutover once `read-only` is the only line that changes.

## Start order

1. Start nodeos with the plugin. The core storage directory must exist before coreindex starts; nodeos creates it on its first start.
2. Start coreindex with `--read-only`.
3. Start actionindex, txindex and other consumers once coreindex reports `ready`.

coreindex can start before nodeos has committed anything. It opens its listeners, reports `not_ready`, refuses stream clients, and checks `control/published` every 100 ms until a published view appears.

## Readiness

`/v1/chain/get_info` and `/v1/history/get_info` report the follower's state:

| `state` | HTTP status | Meaning |
|---------|-------------|---------|
| `ready` | 200 | coreindex serves history up to `head_block_num`, and corestream sends live blocks. |
| `not_ready` | 503 | No published view exists yet. The body carries `"error": "no published view"`. |
| `head_uncertain` | 503 | Storage extends past the published head and no writer holds the lock, which follows a writer crash. coreindex serves history through `history_through_block_num` and holds back head status and live blocks. |

A ready response also carries `committed_head_block_num`, the manifest's `epoch` and `manifest_age_seconds`, the time since the writer last published. When nodeos stops, coreindex keeps serving the last published view and `manifest_age_seconds` grows; alert on it the way you would on a stalled head.

`head_uncertain` clears once a writer has recovered the store. Start nodeos again, and the plugin recovers it at startup. When nodeos cannot start, run [`coreindex --recover`](recover.md) on the store.

## Metrics

With `metrics-listen` set, coreindex exposes `/metrics`:

| Metric | Meaning |
|--------|---------|
| `coreindex_committed_head_block` | Head of the published view coreindex last read. |
| `coreindex_served_head_block` | Head coreindex serves over HTTP and corestream. |
| `coreindex_abi_head_block` | Block through which the served ABI cache is committed. |
| `coreindex_head_uncertain` | 1 in the `head_uncertain` state. |
| `coreindex_commit_to_serve_seconds` | Histogram of the time from a commit to coreindex serving it. |

actionindex and txindex expose `actionindex_indexed_head_block` and `txindex_indexed_head_block`. The same coreindex gauges exist in trace ingestion, where coreindex commits and serves the same head.

## Consumers

actionindex and txindex read core storage and the ABI cache directly, or follow corestream (actionindex through `--sync-source`, txindex through a `stream://` history source). Both work unchanged in follower mode once they run the release's binaries, which bound every read by `control/published`. Consumers that use only coreindex's HTTP API or corestream need no upgrade.

During a [repair](repair.md), coreindex's head drops to the retained boundary, blocks above it return not found, and coreindex disconnects stream clients. A client that reconnects with a cursor above the new head is refused until the rebuild passes the cursor, then continues without receiving a block twice. Retained history stays readable throughout. Consumers keep what they indexed above the boundary; repair does not rewind them.

## Upgrades

Upgrade readers before writers, one at a time: actionindex, txindex and coreverify first, then coreindex, then nodeos. [Production rollout](rollout.md) gives the full order for an existing deployment.
