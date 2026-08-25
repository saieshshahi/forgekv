# Persisted Raft Design

## Scope

Phase 8 makes the Phase 6 Raft core crash-safe without adding sockets or
threads. A persistence driver executes the core's `PersistHardState` and
`PersistLog` actions synchronously and durably before releasing any dependent
outbound message. The core remains a pure transition system; Phase 9 will
connect the driver's output callback to peer TCP transport.

## Durable structures

Each node owns one data directory containing:

- one checksummed bootstrap-completion identity marker (`IDENTITY`);
- two checksummed hard-state generations (`hard-state.A` and `hard-state.B`);
- one checksummed append-only Raft log journal (`raft-log.wal`).

Hard-state records bind format version, cluster ID, node ID, generation,
current term, and optional vote. An update writes and flushes a unique
same-directory temporary file, atomically renames it over the inactive
generation, and flushes the directory. Recovery chooses the highest valid
generation but fails closed if any named generation is invalid or the pair is
inconsistent. A new directory atomically publishes two term-zero baselines, an
empty journal, then the identity marker. Once the marker exists, missing
artifacts fail closed; without it, startup may resume only a wholly pristine
bootstrap. Strictly recognized unpublished temporary files are removed after
authoritative recovery.

Each log-journal transaction records a suffix replacement: `fromIndex` followed
by zero or more complete entries. Entries contain index, term, kind, bounded
payload length, payload, and a transaction checksum. Recovery replays complete
transactions in order. An independent fixed-header checksum is validated before
the record length is trusted. An incomplete final transaction is truncated; a
complete checksum or structural failure is corruption. The journal header binds
cluster and node IDs and is atomically published. The initial journal is
deliberately single-file: Phase 11 snapshots and compaction introduce bounded
retention and segment replacement. This exception to ADR 0003 is temporary and
explicit.

## Persistence driver

`PersistedRaftNode` owns a `RaftNode` and `RaftStorage`. For each core result it:

1. collects non-persistence actions without publishing them;
2. invokes `before_persist`;
3. writes each persistent update;
4. invokes `after_write`;
5. calls `fdatasync` (and directory `fsync` where required);
6. invokes `after_sync`;
7. after every persistent action succeeds, publishes buffered actions in their
   original order;
8. invokes `before_response` immediately before each outbound message callback;
9. invokes `after_response` immediately after that callback returns.

Consequently a granted vote response cannot escape before the term/vote is
durable, a successful AppendEntries response cannot escape before its accepted
suffix is durable, and leader replication cannot advertise a locally appended
entry before it is durable. A storage error prevents all dependent output and
permanently faults that driver instance; restart reconstructs a fresh driver
from disk.

Role, commit, apply, and rejection notifications are also buffered behind any
persistence in the same transition. They do not require their own flush.

## Crash hooks

Test-only hooks exist at:

- `before_persist`;
- `after_write`;
- `after_file_sync`;
- `after_rename`;
- `after_sync`;
- `before_response`;
- `after_response`.

Failure tests kill a child process at each point, reopen its node directory,
and verify recovered term, vote, and log state. A hard-state write remains
unpublished until its rename and directory flush complete. An unsynced append
may recover only if its entire checksummed transaction is visible, but no
dependent response may have escaped. After sync and before/after response, the
required state must recover. A response marker written before `after_response` proves
that any externally visible grant or append acknowledgment has durable backing.
Recovery synchronizes every complete accepted journal and the data directory
before returning, promoting visible state from an interrupted sync sequence to
durable state before it can influence protocol output.

## Bounds and errors

All lengths are checked before allocation. Node IDs are nonzero, entry indexes
are gap-free, terms are nondecreasing and do not exceed hard state, and command
payloads use the existing 1 MiB bound. Arithmetic and platform file offsets are
checked. Unexpected interior truncation, invalid enum values, index gaps,
checksum mismatch, or incompatible cluster/node identity fail closed. A named
corrupt hard-state generation never falls back to an older vote.

## Deferred work

Phase 8 uses one synchronous flush per persistence action for an auditable
ordering baseline. Grouping adjacent log flushes, segmented journals,
asynchronous completion, and storage-owner queues are deferred until real
cluster integration and profiling. Relaxing flush semantics is not permitted;
the Phase 9 client contract remains strict quorum fsync.
