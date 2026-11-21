# Core storage publication format

This document defines the files that make core storage and the ABI cache safe to follow while one process writes them. Every writer of core storage implements it: coreindex in trace ingestion, and the nodeos `roborovski_writer_plugin` in direct ingestion. All integers are little-endian. Every checksum is CRC32C (Castagnoli) over the bytes that precede it.

## Files

| File | Location | Written |
|---|---|---|
| `store.binding` | core root and ABI root | once, when the store is created or adopted |
| `writer.lock` | core root and ABI root | never; it exists to be locked |
| `control/manifest` | core root | at every commit |
| `control/published` | core root | after every commit, and at writer start |

The ABI root defaults to `abis` under the core root and may be any other directory.

## Binding, 96 bytes

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `RBBIND\0\0` |
| 8 | 4 | version, 1 |
| 12 | 4 | role: 1 for the core root, 2 for the ABI root |
| 16 | 32 | chain ID, all zero when it was unknown at creation |
| 48 | 16 | core store UUID |
| 64 | 16 | ABI store UUID |
| 80 | 4 | slice span in blocks |
| 84 | 8 | reserved, zero |
| 92 | 4 | checksum of bytes 0 to 91 |

Both roots carry the same chain ID, UUID pair and slice span. A writer refuses to start when the two bindings disagree, when the span differs from its configuration, or when the binding's chain ID is set and differs from the chain it serves. A writer that knows the chain ID and finds it all zero records it once, in both bindings.

## Manifest, 128 bytes

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `RBMANIF\0` |
| 8 | 4 | version, 1 |
| 12 | 4 | reserved, zero |
| 16 | 16 | core store UUID |
| 32 | 16 | ABI store UUID |
| 48 | 8 | epoch |
| 56 | 4 | head: the last committed block, 0 for an empty store |
| 60 | 4 | committed entry count of `abis.index` |
| 64 | 32 | block ID of the head |
| 96 | 8 | committed length of `data.log` in the slice that holds the head |
| 104 | 4 | committed entry count of `blocks.index` in that slice |
| 108 | 4 | ABI snapshot floor: the first block the ABI cache covers, 0 for full history |
| 112 | 8 | committed length of `abis.dat` |
| 120 | 4 | reserved, zero |
| 124 | 4 | checksum of bytes 0 to 123 |

The manifest is the commit record and the only record of where a writer resumes. The store UUIDs must match the bindings. The head block ID lets a writer check blocks it applies again without reading the head record. Slices below the head slice are committed whole. The epoch starts at 1 and changes only when a repair reset removes a suffix of slices.

A writer replaces the manifest by writing a temporary file in `control`, syncing it, renaming it over `manifest`, and syncing `control`. Writers, `coreindex --recover`, `--reset-after-block` and `--preflight` read the manifest.

## Published view

`control/published` is a byte copy of the manifest, and readers follow it alone. A reader uses the head, the head slice's data length and index count, and the ABI cache's lengths from one read of it, and treats every byte or entry beyond them as absent.

A writer replaces the published view by writing a temporary file in `control` and renaming it over `published`, with no sync, only after the manifest's directory sync has returned. A rename is visible to every process as soon as it returns, and a power cut can take back an unsynced rename, so this order keeps the view readers see at or behind the durable manifest.

A writer copies the manifest to the published view at start, once recovery or adoption has finished, and after a reset's epoch-bumped manifest, before it appends or removes anything else.

A reader that finds the published view missing, or failing its size, magic, version or checksum, keeps serving its last good view. A reader with no good view waits when a manifest exists. A store with neither file was written before this format, and a reader serves it without a bound.

## Commit order

1. Append ABI records and core blocks beyond the manifest's bounds. Committed bytes are never rewritten, except the 4-byte count at the start of `blocks.index` and the finalized byte in a `data.log` header.
2. Sync `abis.dat` and `abis.index`.
3. Sync `data.log` and `blocks.index` of every slice written since the last commit, each new slice directory, and the core root when it gained a directory.
4. Replace the manifest and sync `control`.
5. Replace the published view.
6. Notify readers.

A crash before step 4 leaves the previous manifest in force; a crash after it leaves the new one. A crash between steps 4 and 5 leaves the published view one commit behind until the next writer starts.

## blocks.index

The file is a 4-byte entry count followed by 32-byte entries (block number u32, data offset u64, record size u32, first global sequence u64, last global sequence u64). A writer appends entries at `4 + count * 32` and then rewrites the count in place, so the count never names a missing entry.

## Recovery

On start the writer takes both locks and truncates everything beyond the manifest: slice directories above the head slice are removed, the head slice's `data.log` and `blocks.index` are cut to the committed length and count, a finalized flag on a head slice that ends before its last block is cleared, and `abis.dat` and `abis.index` are cut to their committed lengths. It then copies the manifest to the published view. When the store holds less than the manifest commits, the writer stops without changing anything. The redo source supplies every block above the head.

A store written before this format has no manifest. The first writer to open it takes the newest indexed block as the head after checking its record's CRC, cuts anything beyond it, writes both bindings, and then writes a manifest with epoch 1 and its published view. It stops without changes when the head record fails its check.

## Writer lock

The writer holds an exclusive lock on `writer.lock` in the core root and then the ABI root for the life of the process, so the kernel releases both when the process dies. On Linux the lock is an open file description lock (`F_OFD_SETLK`, write lock over the whole file). A follower tests whether a writer is running with `F_OFD_GETLK`, which never takes the lock and never delays a writer. Other platforms, used for development only, fall back to `flock`.
