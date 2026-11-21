# Production rollout

You bring an existing trace-ingestion deployment onto the release in four stages: readers, then coreindex, then one history node cut over to the plugin, then the rest. Each stage leaves the deployment fully working, so you can stop between stages for as long as you need.

## Order

1. **Upgrade the readers, one service at a time:** actionindex, txindex, then coreverify, each to the release's `greymass/roborovski` commit. Readers from before the release trust file growth, and a writer from the release can grow files before a block is durable, so every reader moves first. After each restart, check that the service's indexed head advances.

2. **Upgrade coreindex in trace ingestion** to the release. At its first start it adopts the existing store: it writes `control/manifest` and `control/published` at the last complete block and from then on publishes each block only after it is durable. Check that both files exist in core storage and that `get_info` advances.

3. **Build nodeos with the plugin** from the release's `greymass/spring` commit, following `plugins/roborovski_writer_plugin/README.md`, and keep its `spring-util chain-state build-info` output.

4. **Cut over one history node** to direct ingestion by the [cutover](cutover.md) procedure, then keep it under observation for 14 days before the remaining nodes follow, one at a time.

Consumers that read only coreindex's HTTP API or corestream need no upgrade at any stage.

## The first production node

During the 14 days, keep what a rollback to trace ingestion needs on the first node's server:

- the trace-patched nodeos build and its configuration;
- a snapshot from each day, so a rollback replays at most one day of blocks before trace ingestion resumes;
- free space for a trace directory covering that replay.

Watch, on that node, `roborovski_writer_committed_block` against nodeos's irreversible block, `roborovski_writer_failures_total`, `coreindex_head_uncertain`, `manifest_age_seconds`, and each consumer's indexed head. `coreverify live-probe` measures how long each block takes to reach readers:

```bash
coreverify live-probe --writer-url http://127.0.0.1:8888 \
  --data-path /data/roborovski/core --coreindex-url http://127.0.0.1:9400 \
  --stream-address 127.0.0.1:9401 --records /var/log/roborovski/probe.jsonl --duration 24h
```

## Rollback

Rolling back is the cutover from direct ingestion to trace ingestion, with the release's binaries throughout. Readers stay on the release, and coreindex runs the release in trace ingestion. A binary from before the release is no supported rollback target, because it ignores `control/published` and the writer locks.

When the rollback follows a suspected defect in the plugin's output, also [repair](repair.md) the store from the last block known to be right, so trace ingestion rewrites everything the plugin wrote after it. `coreverify --compare-path` compares two stores block by block when you have a trace-ingestion store of the same range to check against.

## Production sign-off

A release candidate cuts over its first production node only after the release owner signs off its [release-candidate record](release-candidate-record.md): every fault and recovery gate passing at the candidate commits, the Jungle4 72-hour and Vaulta 7-day live reports meeting the availability target, and a rehearsal on a copy of real history that cuts over in both directions with consumers continuing.
