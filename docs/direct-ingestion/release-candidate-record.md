# Release-candidate record

Each release candidate keeps one record, filled in from this template and attached to the release's final pull request. The release owner signs off the first production cutover on it. A code change to either writer after the record is filled re-runs the fault and recovery gates that change touches, and the record names the new commits.

```markdown
# Release candidate <name>

## Commits

- greymass/spring: <commit> (plugin directory: plugins/roborovski_writer_plugin)
- greymass/roborovski: <commit>

## Build

- `spring-util chain-state build-info`:
  <output>
- Compiler and CMake versions: <versions>
- libzstd-dev version: <version>
- Go version (`go version`): <version>

## Qualification hosts

For each host that ran a gate, live run or rehearsal:

- Host and role: <name, what it ran>
- Kernel (`uname -r`): <version>
- Distribution: <Ubuntu 22.04.x>
- Filesystems for core storage, the ABI cache and the buffer: <type and mount options from /proc/mounts>
- Disk model and write cache setting: <model; `hdparm -W` or `nvme get-feature -f 6` output>

## Configuration

- nodeos config.ini: <attached file>
- coreindex configuration, follower and trace: <attached files>
- Snapshots used, with SHA-256: <file, block, hash>

## Fault and recovery gates

- `roborovski_writer_gates.sh` run at the commits above: <log location>
- Result per gate: <unit, crash, two_writers, restart, fork, cutover, rotation, go_gates, disk: pass or fail>

## Parity

- Jungle4 comparison with trace-ingestion history: <report link, per-case counts, differences and their causes>
- Vaulta corpus and heavy-block replays: <report link, per-case counts>

## Live runs

- Jungle4, 72 hours after catch-up: <coreverify live-probe report, share of blocks within one second per path>
- Vaulta, 7 days after catch-up: <coreverify live-probe report, parity against the production trace-ingestion store over the same range>
- Replay throughput against trace ingestion: <blocks per second, recorded for comparison>

## Cutover rehearsal

- Store seeded from: <slice source, range>
- Trace to direct, direct to trace, trace to direct: <for each: pause length, preflight report, coreverify output>
- Consumers: <actionindex and txindex indexed heads across each handoff>

## Sign-off

- Signed off by: <name>, <date>
- First production node: <host>, cutover at block <N>, rollback window ends <date>
```
