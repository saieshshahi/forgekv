# ADR 0003: Segmented WAL, In-Memory State, and Checksummed Snapshots

- Status: Accepted
- Date: 2026-08-24

## Context

Raft requires persistent term, vote, and log state with precise ordering before
protocol responses. ForgeKV values range up to 1 MiB, recovery must tolerate an
interrupted final write, and snapshots must bound replay time and disk usage.
The initial state machine needs deterministic behavior and does not require data
larger than local memory.

## Decision

Use four storage structures in one node-specific data directory:

1. a two-generation checksummed hard-state store for current term, voted-for
   node, and format/identity metadata;
2. immutable numbered WAL segments plus one active append segment;
3. immutable checksummed snapshots containing KV and deduplication state; and
4. a two-generation checksummed manifest naming the authoritative snapshot and
   retained segment set.

The active KV state and client deduplication table live in memory and are owned
by one apply thread. They are reconstructed from a snapshot plus known-committed
WAL entries. An embedded database is not used initially.

```text
data directory
  identity / lock
  hard-state.A
  hard-state.B
  manifest.A
  manifest.B
  wal-<first-index>-<generation>.log
  snapshot-<last-index>-<term>.snap
  tmp/...
```

Exact filenames and byte formats will be versioned by later storage and protocol
design work. Readers never infer ordering from unspecified directory enumeration
order.

## Hard state

Hard-state records contain a magic value, format version, cluster ID, node ID,
monotonic generation, term, optional voted-for ID, declared length, and checksum.
An update writes the inactive generation, flushes it, and only then permits the
dependent Raft response. Recovery selects the highest valid generation. No valid
generation is a fatal storage error.

Term and vote are never updated only in memory when a response could cause
another node to rely on them.

## WAL structure

Each segment starts with a checksummed header binding it to the cluster, node,
format version, generation, and first log index. Each record is independently
length-delimited and checksummed over its index, term, command type, flags, and
payload. Length and index continuity are checked before payload allocation.

The WAL worker is the sole owner of descriptors and performs:

1. encode to a bounded immutable buffer;
2. append complete records in index order;
3. group adjacent flush requests within a configured latency bound;
4. call `fdatasync`;
5. emit a monotonic durable-index completion to Raft; and
6. roll the segment at a size threshold.

When rolling, the new file is created and its header flushed before it is named
in a newly published manifest. Required directory entries are fsynced. A failed
append or flush never produces a success completion.

Conflicting uncommitted suffixes are removed logically before replacement.
Physical truncation or segment replacement is performed by the WAL owner and
made crash-safe through a new manifest generation; recovery must never combine
files from incompatible generations.

## Commit and applied watermarks

Log presence does not imply commitment. A checksummed monotonic commit/applied
checkpoint may be persisted as a recovery optimization only after the indicated
indexes are truly committed/applied and only when the referenced log or snapshot
is already durable. A stale lower checkpoint is safe and causes replay or Raft
reconciliation; a checkpoint beyond available valid storage is corruption.

Correctness does not require persisting every commit-index advance. A recovering
node that lacks a recent trustworthy watermark waits for the current leader to
communicate commitment before applying uncertain entries.

## Torn writes and corruption

Recovery scans segments in manifest order and accepts only a continuous prefix
of structurally valid records. If EOF occurs before the bytes declared by the
last record, the incomplete tail is truncated to the last complete boundary.

A checksum mismatch in a complete record, invalid index sequence, invalid
segment header, unexpected hole, or identity mismatch is not treated as a torn
tail. The node enters quarantine and requires peer repair or operator action.
This may sacrifice availability but prevents silent divergence.

## Snapshots

The apply thread chooses an exact `lastApplied` index and term and freezes an
immutable logical image of:

- every key and value;
- per-client highest request ID, fingerprint, and stored result; and
- state-machine/schema version.

The initial implementation may briefly pause application to clone the logical
map. It must check memory headroom and report pause/copy metrics. The snapshot
worker serializes the immutable image in deterministic key/client order to a
unique temporary file, computes a whole-file checksum, flushes, verifies, and
atomically renames it. The parent directory is flushed before a new manifest
generation makes it authoritative.

Only then can WAL segments fully covered by the snapshot be removed. Snapshot
installation from a leader follows the same verify-flush-rename-publish order.

## Recovery

Recovery performs, in order:

1. exclusive directory lock and identity/format validation;
2. newest-valid hard-state generation selection;
3. newest-valid manifest generation selection;
4. authoritative snapshot verification and restore;
5. ordered WAL scan and incomplete-tail truncation;
6. Raft log reconstruction without assuming commitment;
7. replay through a trustworthy commit watermark, if available; and
8. follower rejoin and reconciliation with the leader.

The same persistent bytes and cluster input produce the same recovery result.

## Alternatives considered

### One ever-growing WAL file

Rejected because compaction, replacement of conflicting suffixes, repair, and
bounded recovery become operationally awkward.

### One file per entry

Rejected because directory operations, inode usage, and sync overhead are too
high, especially for typical small values.

### Embedded LSM-tree or B-tree database

This could provide checksums, compaction, and snapshots, but introduces another
WAL and durability contract whose ordering must be reconciled with Raft. It also
obscures the learning and control goals of ForgeKV. Reconsider if datasets no
longer fit memory or snapshot pauses dominate.

### Memory-mapped WAL

Rejected initially because dirty-page lifetime, truncation, SIGBUS behavior, and
flush semantics are harder to audit than explicit append and `fdatasync`.

## Consequences

The design has a clear recovery chain and permits sequential I/O and group
commit. It also requires careful manifest publication and snapshot/WAL race
testing. The in-memory state machine limits dataset size and snapshot copying can
cause latency and memory spikes. Those are accepted initial constraints and are
top-level measured risks, not hidden implementation details.

## Required evidence

Tests must inject crashes before and after every write, flush, rename, manifest
publication, truncation, and deletion. Recovery corpus tests must cover empty
files, incomplete headers, incomplete payloads, invalid lengths, checksum
failures, index gaps, stale generations, mixed cluster IDs, disk-full errors, and
snapshot/WAL boundaries.
