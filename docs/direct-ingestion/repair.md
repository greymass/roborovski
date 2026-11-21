# Repair

When part of core storage is wrong or damaged, you remove whole slices from the end of the store and let the writer rebuild them by replay. The retained history before the removed slices stays in place and stays readable while the rebuild runs. Repair covers core storage, its indexes and metadata, and the ABI cache; it works the same under either ingestion method.

## Choose the boundary

The boundary B is the last block you keep. It must end a slice, so it is a multiple of 10,000, and it must lie below the first block you want rebuilt. You also need a snapshot of the chain at or below B.

Replay after a snapshot at height S starts at S+1. To rebuild from block 900,001, keep history through 900,000 and load a snapshot at 900,000. To rebuild block 900,000 itself, also remove the slice that starts at 890,001: set B to 890,000 and load a snapshot at or below 890,000.

B cannot go below the ABI cache's floor. A store started from a snapshot has its floor at that snapshot's block, and history below it has no source to rebuild from.

## Procedure

1. **Record the published head** H from coreindex's `get_info`. The removed range is B+1 to H.

2. **Stop the writer.** In direct ingestion stop nodeos; in trace ingestion stop coreindex. coreindex in follower mode keeps running.

3. **Reset the store to B:**

   ```bash
   coreindex --reset-after-block B \
     --index-storage /data/roborovski/core --abi-source /data/roborovski/core/abis
   ```

   Readers drop everything above B at once and keep serving history through B. If the command is interrupted, run it again or run `coreindex --recover`; see [Reset after a block](reset-after-block.md).

4. **Start the writer from a snapshot at or below B.**
   - Direct ingestion: start nodeos with the plugin on an empty data directory with `--snapshot`. nodeos replays from the snapshot, the plugin verifies and skips the blocks through B, and it appends from B+1 into live operation.
   - Trace ingestion: start the trace-patched nodeos from the snapshot on an empty data directory, writing trace files from the snapshot onward, then start coreindex in trace ingestion. It appends from B+1.

5. **Watch the rebuild.** The published head climbs from B back to H and on into live operation. Blocks above the published head return not found until the rebuild reaches them, and corestream clients whose cursor lies above it wait and then continue.

## Consumers after a repair

actionindex and txindex are not rewound. Each keeps what it indexed between B+1 and H, then continues from its own indexed head once the store passes it. When the rebuilt blocks differ from what a consumer indexed, rebuild that consumer's index from block B+1 under its own procedure.

## Interruptions during the rebuild

After the reset, an interruption is an ordinary writer restart: start the writer again and it resumes from its last commit, with no second reset.
