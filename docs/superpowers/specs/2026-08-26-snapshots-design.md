# Phase 11: Snapshot and Log Compaction Design

## Durable image

`raft.snapshot` is a versioned, checksummed file bound to cluster ID, node ID,
and fixed-membership fingerprint. It contains `lastIncludedIndex`,
`lastIncludedTerm`, and an opaque deterministic state-machine payload. The
cluster payload encodes sorted key/value pairs with explicit lengths and bounds.

Creation writes `raft.snapshot.tmp`, syncs the file, renames it over the current
snapshot, and syncs the containing directory. Recovery ignores an unrenamed
temporary file, validates every header field and checksum in the published file,
and fails closed on corruption.

## Creation without a long owner pause

The Raft owner briefly copies the state machine at its applied index and submits
that immutable image to one bounded snapshot worker. Request processing resumes
while the worker encodes, checksums, writes, and syncs the image. When the worker
reports success, the owner verifies the exact index/term, adopts the durable
snapshot, and rewrites only the remaining log suffix. Only one creation may be
in flight; additional triggers are coalesced.

The copy pause and complete snapshot duration are measured separately by dataset
size. The initial copy is stop-the-world on the owner thread, but disk I/O is not.

## Crash-consistent compaction ordering

The snapshot is published before the log prefix is removed. Therefore recovery
may observe either:

1. the old snapshot and old log;
2. the new snapshot and old full log; or
3. the new snapshot and an older compacted suffix log; or
4. the new snapshot and its newly compacted suffix log.

All four are valid. Recovery replays from the boundary declared by the journal,
validates the reconstructed entry at the published snapshot's
`lastIncludedIndex` against `lastIncludedTerm`, and then discards the covered
prefix. A compacted journal begins at its declared boundary plus one.

## Raft indexing

The in-memory log keeps a sentinel at the snapshot boundary rather than assuming
index zero. Index-to-vector conversion subtracts `lastIncludedIndex`. Commit and
apply indexes recover at that boundary. Voting still compares the true last log
index and term.

## Follower installation

If a leader's `nextIndex` for a peer is at or below its snapshot boundary, it
sends `InstallSnapshot` chunks with exact RPC correlation, offset, boundary
metadata, and a final flag. Followers accept chunks only from the current leader
and in exact offset order. Intermediate chunks are restartable but not claimed
durable. The final chunk emits a durable snapshot action before the apply action
and response. A rejection reports the expected offset, allowing restart from
zero after follower crash.

After installation, a follower preserves a suffix only when its entry at the
snapshot boundary has the same term; otherwise it discards the suffix. It sets
commit/applied to at least the boundary, installs the state-machine payload, and
then accepts normal AppendEntries from `lastIncludedIndex + 1`.

## Trigger and limits

The first policy triggers after 1,024 newly applied entries since the last
snapshot and exposes a lower threshold for tests. Snapshot chunks are bounded by
the peer frame budget. Snapshot payload and key/value counts are explicitly
bounded before allocation.

## Verification

Tests cover deterministic encoding, checksum and identity corruption, every
atomic publication crash boundary, old-log/new-snapshot recovery,
older-compacted-log/new-snapshot recovery, core offset indexing, chunk retry,
follower offline through leader
compaction, install and catch-up, state consistency, and debug/release plus all
sanitizers. Benchmarks report owner copy pause and total snapshot duration across
dataset sizes.
