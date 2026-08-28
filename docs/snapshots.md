# Snapshots and log compaction

ForgeKV bounds Raft log growth with versioned, checksummed state-machine
snapshots. The default trigger is 1,024 newly applied entries and can be changed
with `--snapshot-threshold ENTRIES`.

## Contents and recovery

`raft.snapshot` binds the image to the cluster ID, node ID, and fixed-membership
fingerprint. Its fixed header records the format version, `lastIncludedIndex`,
`lastIncludedTerm`, payload length, header checksum, and payload checksum. The
payload is a deterministic encoding of the key/value state sorted by key and
the request-deduplication table sorted by client ID. The payload format is
version 2; the decoder accepts version 1 key/value-only payloads for upgrades.

Recovery loads and verifies the snapshot before replaying `raft-log.wal`. The
journal header names the older boundary from which its retained records must be
reconstructed. Recovery replays from that declared boundary, validates the
final log at the published snapshot's index and term, discards the covered
prefix, and retains the suffix. Corrupt identity, length, format, checksum, or
boundary data fails startup closed.

Publication is ordered as follows:

1. Write `raft.snapshot.tmp` and `fdatasync` it.
2. Rename it to `raft.snapshot` and `fsync` the data directory.
3. Atomically replace the journal with a header plus the retained suffix and
   `fsync` the directory.

A crash can therefore expose the old snapshot and old journal, the new snapshot
with an uncompacted journal, the new snapshot with an older compacted journal,
or the new snapshot with its newly compacted journal. All combinations recover
when replay proves the new snapshot boundary's term. A new snapshot is durable
before its covered log prefix is removed.

## Request pause and background work

The Raft owner briefly copies the current key/value map and deduplication table
at an applied log
boundary. Encoding, checksum calculation, file writes, and syncs run on a
background worker. The owner publishes the completed snapshot into Raft only
after the file is durable. Snapshot publication is monotonic, so a delayed
background job cannot replace a newer snapshot installed from a leader.

The release benchmark on the development host measured:

| Entries (64-byte values) | owner copy pause, median | encode + durable publish, median |
|---:|---:|---:|
| 1,000 | 0.088 ms | 4.93 ms |
| 10,000 | 1.96 ms | 8.63 ms |
| 100,000 | 34.0 ms | 65.5 ms |

Run `forgekv_benchmarks --benchmark_filter='Snapshot(PauseCopy|EncodeAndPublish)'`
to reproduce the matrix. The 100,000-entry copy pause is explicitly not
considered free. A future optimization should use immutable generations or
incremental/copy-on-write snapshot views so owner-thread pause grows much more
slowly than the dataset.

## Follower installation

When a follower's `nextIndex` falls behind the leader's retained log base, the
leader sends `InstallSnapshot` in bounded 1 MiB chunks. Each chunk carries the
snapshot boundary, total size, offset, completion flag, and RPC ID. Followers
accept only an exact next offset, assemble within the 512 MiB hard limit,
durably publish the completed image, replace their state machine, and only then
acknowledge completion. The leader then resumes `AppendEntries` at
`lastIncludedIndex + 1`.

The end-to-end test keeps a follower offline while a leader commits large
writes and compacts its log, restarts the follower, verifies snapshot catch-up,
then partitions the old leader and confirms the caught-up nodes can elect and
continue consistently.

## Current limits

- Snapshots contain the full state machine and have a 512 MiB encoded cap.
- Fixed membership is assumed; membership changes need snapshot metadata for
  the last included configuration.
- Chunk transfer resumes within a running process. A follower restart begins
  transfer again at offset zero because partial images are intentionally not
  treated as durable snapshots.
