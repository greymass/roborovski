# Direct ingestion

Operators can fill Roborovski core storage and the ABI cache in two ways, and can move an existing store from one to the other in either direction. Both write the same files, so actionindex, txindex and other consumers read the store the same way under either method.

- **Trace ingestion.** nodeos, built with the Roborovski trace patch, writes trace files through its trace_api plugin, and coreindex converts them into core storage and the ABI cache.
- **Direct ingestion.** nodeos, built with `eosio::roborovski_writer_plugin` from `greymass/spring`, writes core storage and the ABI cache itself. coreindex runs in follower mode and serves what nodeos commits.

Whichever process writes the store is the **writer**: coreindex in trace ingestion, nodeos in direct ingestion. Only one writer owns a store at a time; each writer locks the core storage and ABI directories, and a second writer refuses to start.

## Heads

Three block heights describe a running deployment:

- The **irreversible chain head** is the last block nodeos has made irreversible.
- The **published head** is the last block readers may see. The writer advances it only after the block is durable on disk, and records it in `control/published` inside core storage.
- The **indexed head** of each consumer is the last block actionindex or txindex has indexed.

The release's availability target is that 99% of newly irreversible blocks reach readers through coreindex within one second during steady live operation. coreindex serves up to the published head and never past it.

## Pages

- [coreindex follower mode](follower.md): run coreindex beside a nodeos that writes the store.
- [Preflight](preflight.md): check a store and the replacement writer's inputs before a cutover.
- [Cutover](cutover.md): move a store from trace ingestion to direct ingestion and back.
- [Repair](repair.md): remove a suffix of slices and rebuild it by replay.
- [Recover](recover.md): bring a crashed store back to its last commit without nodeos.
- [Reset after a block](reset-after-block.md): the command repair uses to remove slices.
- [Production rollout](rollout.md): upgrade order, the first production node, and rollback.
- [Release-candidate record](release-candidate-record.md): the evidence a release candidate keeps.

Building nodeos with the plugin, its options and its metrics are in `plugins/roborovski_writer_plugin/README.md` in `greymass/spring`. The on-disk publication protocol is specified in [`libraries/coremanifest/FORMAT.md`](../../libraries/coremanifest/FORMAT.md).

## Compatible versions

A release pairs one `greymass/spring` commit with one `greymass/roborovski` commit. Run nodeos, coreindex, actionindex, txindex and coreverify from the same release. Readers from before the release trust file growth instead of `control/published`, so they can serve blocks before they are durable; upgrade every reader before any writer runs the release (see [Production rollout](rollout.md)). Binaries from before the release are no supported rollback target for a store a release writer has written.

## Deployment boundary

nodeos, coreindex and the consumers that read files share one Linux server, with core storage, the ABI cache and the plugin's buffer on its local filesystems. The release is qualified on Ubuntu 22.04 x86_64. Core storage uses 10,000-block slices; direct ingestion refuses any other slice size.

## Example configurations

- [`examples/coreindex-follower.conf`](examples/coreindex-follower.conf): coreindex in follower mode for direct ingestion.
- [`examples/coreindex-trace.conf`](examples/coreindex-trace.conf): coreindex writing the store in trace ingestion.
- [`examples/nodeos-trace.ini`](examples/nodeos-trace.ini): the trace_api settings of a trace-ingestion nodeos.
- `plugins/roborovski_writer_plugin/examples/config.ini` in `greymass/spring`: nodeos with the plugin.

coreindex reads `--config <file>`, or `./config.ini` in its working directory when that file exists. Each line is `name = value` with the flag's name, and `#` starts a comment.
