# Raft Persistence and Crash Ordering

Phase 8 connects the deterministic Raft core to crash-safe local storage. The
core still performs no I/O. `PersistedRaftNode` consumes its action stream,
executes persistence actions synchronously, and releases dependent actions only
after the required flush completes.

## Files and ownership

One node data directory contains:

```text
LOCK
IDENTITY
hard-state.A
hard-state.B
raft-log.wal
```

`LOCK` carries a nonblocking exclusive process lock. A second opener fails, so
two processes cannot mutate one node identity concurrently.

Hard state alternates between A and B. Recovery validates every named slot and
chooses the record with the greatest generation. Any existing invalid or
impossibly missing slot, invalid parity, or nonconsecutive pair is corruption and fails closed; silently
forgetting an acknowledged vote is never an acceptable fallback. A new store
atomically publishes pristine term-zero generations 1 and 2, its empty journal,
then an `IDENTITY` completion marker. Once that marker exists, no durable
artifact may be silently recreated. Strictly recognized unpublished temporary
files are removed only after authoritative recovery. Every durable file binds
both cluster and node ID.

`IDENTITY` is a 40-byte checksummed record containing magic `FRID`, format and
record size, cluster ID, node ID, a canonical fixed-voter fingerprint, reserved
bytes, and CRC32. It is published last using temporary-file, file-sync, rename,
and directory-sync ordering. A missing marker is resumable only while every
recovered structure is pristine. The same voter fingerprint appears in the log
header, so changing voting IDs on restart fails closed.
The fingerprint sorts voter IDs numerically, feeds each ID least-significant
byte first into 64-bit FNV-1a, then feeds the voter count; an all-zero result is
encoded as one. It binds voting identity, not mutable network endpoints.

Identity format version 2 is:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `FRID` |
| 4 | 2 | format version (`2`) |
| 6 | 2 | record size (`40`) |
| 8 | 8 | cluster ID |
| 16 | 8 | node ID |
| 24 | 8 | canonical fixed-voter fingerprint |
| 32 | 4 | reserved zero |
| 36 | 4 | CRC32 over bytes 0–35 |

## Hard-state format, version 2

All integers are unsigned big-endian. The record is exactly 60 bytes.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `FRHS` |
| 4 | 2 | format version (`2`) |
| 6 | 2 | flags: bit 0 means vote present |
| 8 | 4 | record size (`60`) |
| 12 | 4 | reserved zero |
| 16 | 8 | cluster ID |
| 24 | 8 | node ID |
| 32 | 8 | monotonic generation |
| 40 | 8 | current term |
| 48 | 8 | voted-for node ID, or zero |
| 56 | 4 | CRC32 over bytes 0–55 |

Term regression, a vote in term zero, node ID zero, changing an existing vote
within one term, and moving the hard term below a stored log term are rejected.

An update writes a unique same-directory temporary file, calls `fdatasync`,
renames it over the inactive slot, and calls `fsync` on the directory. Only then
does that generation become the driver's durable state. A crash before rename
leaves the previous named generations intact. Generation overflow fails closed.

## Log journal format, version 2

The 36-byte journal header is:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `FRLG` |
| 4 | 2 | format version (`2`) |
| 6 | 2 | header size (`36`) |
| 8 | 8 | cluster ID |
| 16 | 8 | node ID |
| 24 | 8 | canonical fixed-voter fingerprint |
| 32 | 4 | CRC32 over bytes 0–31 |

Each following transaction describes one suffix replacement.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `FRLR` |
| 4 | 2 | format version (`2`) |
| 6 | 2 | transaction header size (`32`) |
| 8 | 4 | total transaction size |
| 12 | 4 | entry count |
| 16 | 8 | replacement `fromIndex` |
| 24 | 4 | fixed-header CRC32 over bytes 0–23 |
| 28 | 4 | CRC32 over bytes 0–27 and 32–end |

Every entry then contains:

| Relative offset | Size | Field |
|---:|---:|---|
| 0 | 8 | log index |
| 8 | 8 | term |
| 16 | 1 | kind: command (`0`) or no-op (`1`) |
| 17 | 3 | reserved zero |
| 20 | 4 | payload length |
| 24 | variable | command bytes |

Transactions are capped at 64 MiB, 4,096 entries, and 1,049,664 bytes per command.
Indexes must be gap-free, terms nonzero and nondecreasing, and entry terms may
not exceed durable current term.

Recovery scans in file order and applies suffix replacements exactly. An
incomplete final transaction is truncated at its start and the truncation is
flushed. The fixed-header checksum is verified before trusting the record
length, so a corrupted length cannot be mistaken for an incomplete tail.
Invalid lengths, magic, version, reserved fields, checksum, indexes, terms,
kinds, or interior structure are corruption and quarantine the node. Log
presence never implies commitment. Initial journal publication uses the same
temporary-file, flush, rename, and directory-flush sequence as hard state.
Before recovery returns, it synchronizes the accepted journal and directory.
This completes durability if a prior process died after a complete append or
rename became visible but before its final sync.

Phase 9 increments the complete Raft storage format from version 1 to version 2
because the fixed-voter fingerprint changes the identity and journal headers.
There is no automatic in-place migration: a version 1 store fails with an
explicit offline-migration/data-wipe diagnostic. In this pre-release staged
project, an operator may archive and wipe a version 1 node directory and let
that node rejoin from a surviving quorum. A production upgrade tool must
preserve the old bytes, atomically publish every version 2 artifact, and be
separately crash-tested before in-place upgrades are supported.

The single journal is the Phase 8 correctness baseline. Phase 11 snapshots and
compaction will bound it and introduce segment/manifest publication as described
by ADR 0003.

## Required ordering

| Raft transition | Durable mutation and flush | Earliest dependent output |
|---|---|---|
| Start election | term and self-vote hard state | RequestVote RPCs |
| Observe higher term | new term and cleared vote | rejection/response in new term |
| Grant vote | term transition if needed, then candidate vote | granted RequestVoteResponse |
| Accept follower suffix | higher-term hard state if needed, then suffix journal transaction | successful AppendEntriesResponse |
| Reject AppendEntries | higher-term hard state if needed; no log write | rejection response |
| Become leader | current-term no-op journal transaction | AppendEntries carrying no-op |
| Accept leader proposal | command journal transaction | AppendEntries carrying command |
| Advance commit/apply | none; referenced log is already durable | commit/apply notification |

The driver buffers role, commit, apply, rejection, and message actions while it
executes every persistence action in the transition. It then publishes buffered
actions in original order. A persistence or hook failure faults that driver
instance and suppresses all remaining output; recovery requires a fresh open.

## Crash evidence

Process-level tests send `SIGKILL` at `before_persist`, `after_write`,
`after_file_sync`, `after_rename`, `after_sync`, `before_response`, and
`after_response`. Voting also crashes both
before and after each of its possible two hard-state generations. Log acceptance
uses the same matrix after a durable term bootstrap, plus a combined higher-term
and suffix transition that crashes around both ordered persistence actions.

Before sync, recovery may legitimately choose the previous record or a complete
new record, but no response marker may exist. After sync, the new state must
recover. If a response marker exists, its exact vote or log entry must be
durable. A restarted node that visibly granted a vote rejects a second candidate
in the same term. Unit recovery also tests every byte prefix of an incomplete
final journal transaction.

Phase 8 performs one flush per persistence action. Grouping and asynchronous
completion can improve throughput later, but may not weaken these ordering or
strict quorum-fsync contracts.
