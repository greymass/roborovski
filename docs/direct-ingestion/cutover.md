# Cutover

You can move an existing store from trace ingestion to direct ingestion, and back, without re-encoding any history. The new writer verifies the overlap with what the store holds and appends from the block after the store's head. New history pauses from the moment the old writer stops until the new one catches up, and readers keep serving everything already published throughout.

Both directions follow the same five steps:

1. Run release binaries for every reader and both writers.
2. Run the [preflight](preflight.md) for the replacement writer and resolve its blockers.
3. Stop the current writer.
4. Start the replacement writer, which validates the store again under its own lock and recovers any interrupted commit before it writes.
5. Run coreindex in the role the new method needs, and check that the published head and the consumers advance.

## Before the first cutover

- Every reader and coreindex run the release, and coreindex in trace ingestion has started once on the store, which writes `control/manifest` and `control/published`. [Production rollout](rollout.md) gives the order.
- nodeos with `eosio::roborovski_writer_plugin` is built, and the trace-patched nodeos the store was written with stays available for the way back.
- The history node can take snapshots: load `eosio::producer_api_plugin` on an address only operators reach, or use the node's existing snapshot schedule.

The examples use core storage at `/data/roborovski/core` and the ABI cache at `/data/roborovski/core/abis`.

## Trace ingestion to direct ingestion

1. **Take a snapshot on the trace node** at a block S that the store already holds or will hold within minutes, and describe it:

   ```bash
   curl -s -X POST http://127.0.0.1:8888/v1/producer/create_snapshot
   spring-util snapshot info /data/nodeos/snapshots/snapshot-<id>.bin > /data/snapshots/S.info.json
   ```

   A snapshot close to the store's head keeps the pause short, because nodeos replays every block from S to the head before it appends.

2. **Run the preflight** once the store's published head has passed S:

   ```bash
   coreindex --preflight --preflight-target direct \
     --index-storage /data/roborovski/core --abi-source /data/roborovski/core/abis \
     --chain-id <chain id> --snapshot-info /data/snapshots/S.info.json
   ```

   Continue on exit 0. The report's `block source` entry is unestablished: nodeos needs blocks S+1 onward from its peers.

3. **Stop coreindex and the trace nodeos** with SIGTERM. The store's published head H is now fixed; `get_info` reported it last, and `control/published` records it. Readers that read files keep serving through H.

4. **Start nodeos with the plugin from the snapshot.** Move the trace node's data directory aside and start the release nodeos on an empty data directory with the plugin's configuration (`plugins/roborovski_writer_plugin/examples/config.ini` in `greymass/spring`):

   ```bash
   nodeos --data-dir /data/nodeos --config-dir /etc/nodeos-direct --snapshot /data/snapshots/snapshot-<id>.bin
   ```

   nodeos fetches blocks from S+1 from its peers. The plugin checks each block up to H against the block ID core storage holds, skips it, and appends from H+1. Its log reports `Roborovski writer started live at committed block H`, and `roborovski_writer_committed_block` passes H.

5. **Start coreindex in follower mode** with [`examples/coreindex-follower.conf`](examples/coreindex-follower.conf). `get_info` reports `state: ready` and a head past H. actionindex and txindex continue from their indexed heads without a reindex.

## Direct ingestion to trace ingestion

1. **Take a snapshot on the plugin node** at a block S, as above, and let the store's published head pass it.

2. **Stop nodeos** with SIGTERM. The plugin commits what is irreversible and exits; its log reports `Roborovski writer stopped at committed block H`. coreindex in follower mode keeps serving through H.

3. **Start the trace-patched nodeos from the snapshot** on an empty data directory, with trace_api writing to a new trace directory ([`examples/nodeos-trace.ini`](examples/nodeos-trace.ini)). Wait until its last irreversible block passes H, so the trace files hold H+1.

4. **Run the preflight:**

   ```bash
   coreindex --preflight --preflight-target trace \
     --index-storage /data/roborovski/core --abi-source /data/roborovski/core/abis \
     --chain-id <chain id> --trace-dir /data/nodeos/traces --stride 10000
   ```

   Continue on exit 0. A blocked `trace input` check means the trace files do not hold H+1 yet.

5. **Replace the follower with the trace writer.** Stop coreindex in follower mode, then start it with [`examples/coreindex-trace.conf`](examples/coreindex-trace.conf). It reads trace files from H+1 onward, and the published head passes H. actionindex and txindex continue without a reindex.

## When the new writer refuses

A replacement writer that finds a mismatch at startup exits before it writes, leaves the store unchanged, and names what is missing.

- nodeos started from a snapshot above H reports `nodeos is irreversible through block R but cannot name block H+1`. Start it again on an empty data directory from a snapshot at or below H.
- A snapshot whose block ID differs from the store at the same height fails the preflight's `snapshot` check, and the plugin stops with `block N is <id> in nodeos but <id> in committed history`. The snapshot belongs to another chain or fork; take one from this chain.
- coreindex in trace ingestion without trace files for H+1 exits with an error that names the trace files. Let the trace nodeos run past H, or point `trace-dir` at the right directory.
- A second writer on the same store, such as coreindex in trace ingestion while nodeos with the plugin runs, fails to take the writer lock and changes nothing.

A writer that stops part way through step 4 or 5 leaves the store at its last commit. Starting the same writer again resumes from there. Returning to the former method is another cutover in the other direction, with the same steps.
