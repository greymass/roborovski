# Reset after a block

`coreindex --reset-after-block N` removes every slice above block N, under a new manifest epoch, so a writer can rebuild that history by replay. You run it as one step of a [repair](repair.md). N must be the last block of a slice, a multiple of 10,000.

```bash
coreindex --reset-after-block 24850000 \
  --index-storage /data/roborovski/core \
  --abi-source /data/roborovski/core/abis
```

Stop the writer first; the reset takes both writer locks. coreindex in follower mode keeps running and keeps serving retained history.

## Steps

1. Publish a manifest with the next epoch and head N. Readers drop their view above N at once: coreindex's head becomes N, blocks above N return not found, and stream clients are disconnected.
2. Remove the slice directories above N.
3. Cut the ABI cache back to the entries at or below N.

On success it prints the boundary, the new epoch and what it removed:

```
Reset to block 24850000 under epoch 2
Removed 3 slices and 17 ABI entries; replay from a snapshot at or below block 24850000 rebuilds the rest
```

Record the published head from `get_info` before the reset. The removed range runs from N+1 to that head, and consumers that indexed any of it keep their entries; judge whether their indexes need a rebuild from that range.

## Interruptions

The new epoch's manifest is the durable record of the reset. If the command stops at any step, run the same command again, or run [`coreindex --recover`](recover.md); either finishes the reset with the same result as an uninterrupted one. A repeated reset that completes an earlier attempt prints `Finished the reset to block N under epoch E`.

## Refusals

The reset changes nothing and exits with `Reset refused; storage is unchanged` when N is above the published head, N does not end a slice, the store has no manifest, or N is below the ABI cache's floor. A store started from a snapshot has its floor at the snapshot's block, and history below it has no source to rebuild from.

`--reset-after-block` runs on its own. It refuses to combine with `--read-only`, `--replay`, `--preflight`, `--recover` or the verify, repair and ABI rebuild modes.
