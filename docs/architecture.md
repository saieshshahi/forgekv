# ForgeKV System Architecture

Status: Phase 0 design baseline  
Audience: implementers, reviewers, and operators

## 1. Purpose and scope

ForgeKV is a replicated key-value store for Linux. A cluster contains three to
seven fixed-membership nodes; three or five nodes are the recommended operating
sizes. One Raft leader orders all mutations. The initial public operations are:

- `PUT key value`: create or replace a value.
- `GET key`: return the current value or `NOT_FOUND`.
- `DELETE key`: remove a value and report whether it existed.

Keys are at most 1 KiB and values are at most 1 MiB. Typical values are 100 B to
4 KiB. Batch operations, dynamic membership, follower reads, and multi-key
transactions are outside the initial scope, but the command and log boundaries
must not prevent adding batches later.

This document is normative for component boundaries and end-to-end behavior.
The invariants in [invariants.md](invariants.md) take precedence if prose is
ambiguous. Detailed choices are recorded in [adr/](adr/).

## 2. Correctness contract

ForgeKV provides these initial guarantees:

1. Successful `PUT`, `DELETE`, and `GET` operations are linearizable.
2. Only the leader serves client operations. Followers return `REDIRECT` with a
   leader hint when known, otherwise a retryable no-leader response.
3. A mutation is acknowledged only after its log entry is durable on a quorum
   that includes the leader, committed by Raft, and applied to the leader state
   machine.
4. A follower reports an entry as replicated only after its WAL append has
   completed successfully and `fdatasync` (or an equivalent durable flush) has
   completed.
5. A leader performs a quorum-confirmed Raft ReadIndex round before a `GET`; it
   does not rely on a clock-based leader lease.
6. Uncommitted entries are never applied or acknowledged. After a leadership
   change they may be committed by a later leader or overwritten.
7. A client timeout is an unknown outcome, not a cancellation. Retrying the same
   logical mutation uses the same `(client_id, request_id)`.
8. Duplicate suppression is part of replicated state. Replaying the latest
   request with identical content returns its stored result without reapplying
   the mutation.

Here, **durable** means that an acknowledged mutation survives process restarts,
a full-cluster power interruption when disks remain intact, and permanent loss
of fewer than a quorum of disks. The guarantee assumes the operating system,
filesystem, and drive honor successful flush operations. Byzantine behavior,
filesystem bugs, controller lies, and loss or corruption of quorum storage are
outside the fault model.

## 3. Process architecture

Each node is one process. Components communicate with typed messages through
bounded queues; they do not share ownership of mutable protocol or storage
objects.

```text
                            CLIENTS
                               |
                               v
                    +--------------------+
                    | TCP / frame layer  |
                    | single epoll loop  |
                    +----------+---------+
                               |
                        bounded dispatch
                               |
                               v
                    +--------------------+
                    | request executors  |
                    +----+----------+----+
                         |          |
                     GET |          | PUT / DELETE
                         |          v
                         |   +--------------------+       peer RPC
                         +-->| Raft owner thread  |<---------------->
                             +----+-----------+---+
                                  |           |
                           WAL I/O |           | committed command
                                  v           v
                         +-------------+  +------------------+
                         | WAL worker  |  | apply thread     |
                         | + hard state|  | KV + dedup owner |
                         +-------------+  +--------+---------+
                                                   |
                                            immutable image
                                                   v
                                          +-----------------+
                                          | snapshot worker |
                                          +-----------------+
```

The intended source decomposition for later phases is:

```text
src/
  net/        socket lifecycle, epoll reactor, connection backpressure
  protocol/   client and Raft framing, validation, serialization
  raft/       elections, replication, ReadIndex, membership view
  storage/    hard state, segmented WAL, snapshots, recovery
  server/     lifecycle, routing, request orchestration, configuration
  metrics/    counters, gauges, histograms, exporter
  common/     narrowly scoped primitives shared by two or more components

tests/
  unit/
  integration/
  failure/
  model/

bench/
  client/
  micro/
```

`common/` is not a general utility dumping ground. An abstraction belongs there
only when it has a stable contract and multiple real consumers.

## 4. Components and ownership

| Component | Solely owns | May wait for | Must not do |
|---|---|---|---|
| Network reactor | sockets, connection state, input buffers, output queues | `epoll_wait` and nonblocking socket calls | wait for Raft, state-machine, or disk work |
| Request executors | validated request envelopes and transient orchestration state | bounded queue operations | mutate Raft or state-machine state |
| Raft thread | term, role, vote, log metadata, peer progress, commit index, read barriers | completion messages and timer deadlines | perform blocking disk or socket I/O |
| WAL worker | WAL descriptors, segment manifest, hard-state file, local durable index | disk I/O and flush completion | decide commitment or apply commands |
| Apply thread | in-memory KV map, deduplication table, last-applied index | committed-command and read queues | apply an uncommitted command |
| Snapshot worker | snapshot temporary file and serialization buffers | snapshot disk I/O | observe a mutating state-machine view |
| Metrics exporter | scrape connection and output formatting | metrics listener I/O | block producer threads or create unbounded labels |

Every cross-thread message carries the minimum immutable data needed by its
consumer. A completion is routed by an internal operation ID rather than by a
raw pointer to connection-owned state. Queues have configured item and byte
limits. Crossing a high-water mark propagates backpressure toward the reactor;
no producer can enqueue unlimited work.

## 5. Thread model

The initial node uses:

- one network reactor thread;
- a small, fixed request-executor pool for CPU-only validation and dispatch;
- one Raft owner thread;
- one WAL worker thread;
- one state-machine apply thread;
- at most one snapshot worker;
- one metrics/admin thread, or metrics handling integrated into a low-volume
  control loop if profiling supports it.

The single-owner model makes state transitions deterministic and minimizes lock
scope. Bounded multi-producer/single-consumer queues deliver work to owner
threads. Read-only configuration is immutable after startup. Metrics use
per-thread counters or atomics and are aggregated asynchronously.

The apply thread initially services both mutations and state-machine lookups.
This preserves an obvious ordering rule: after a ReadIndex barrier reaches
`lastApplied`, the following lookup observes at least that index. Parallel reads
or reactor-per-core networking require measurements and a new ADR.

## 6. Networking model

The node exposes separate configured listeners for client traffic and Raft peer
traffic, handled by one nonblocking, level-triggered `epoll` reactor. Separating
listeners allows independent authentication, quotas, and operational policy
later while retaining one readiness loop initially.

TCP is a byte stream. The protocol layer incrementally parses zero, one, or many
frames from arbitrary read chunks and retains partial frames per connection.
Frames have a fixed hard maximum; lengths are validated before allocation.
Client and Raft messages use distinct namespaces. Connection buffers, queued
responses, in-flight requests per connection, and global outstanding work all
have byte and item limits.

When a limit is reached, the reactor disables read interest for that connection
and re-enables it below a low-water mark. It sends `BUSY` only when a complete
request can be safely identified and output capacity remains; otherwise it
closes a pathological connection. Slow consumers cannot create unbounded output
queues. Peer connections use retry with capped exponential backoff and jitter.
Duplicate, delayed, and reordered connections do not alter Raft semantics;
terms, node identity, and RPC correlation identify stale traffic.

Networking reports message delivery, never durability or commitment. The peer
RPC path receives a successful AppendEntries response only after the follower's
WAL completion reaches the Raft thread.

## 7. Client request lifecycle

```text
socket readable
      |
      v
incremental frame parse --invalid/oversize--> ERROR or close
      |
      v
bounded request queue --full---------------> BUSY / pause reads
      |
      v
validate key, value, operation, client/request IDs
      |
      +-- follower -------------------------> REDIRECT / no leader
      |
      v
Raft write path or linearizable read path
      |
      v
completion tagged with connection generation + request correlation
      |
      v
bounded output queue -> partial nonblocking writes -> response complete
```

Closing a client connection drops only its response route. Once a valid write
has entered Raft, disconnect or timeout does not cancel it. On completion, an
absent connection causes the response to be discarded; replicated deduplication
state still allows a safe retry.

## 8. Raft write lifecycle

```text
leader receives PUT / DELETE
          |
          v
construct command {client_id, request_id, fingerprint, operation}
          |
          v
append at (index, current_term) -----> leader WAL append + group fdatasync
          |                                      |
          +---------- AppendEntries ------------+----> followers
                                                         |
                                              append + fdatasync WAL
                                                         |
                                               successful response
          <---------------- durable match indexes -------+
          |
          v
current-term quorum rule advances commitIndex
          |
          v
apply committed entries in index order
          |
          +-- duplicate/stale policy is evaluated deterministically
          |
          v
state mutation and dedup result recorded atomically in logical state
          |
          v
reply to client after leader application
```

The leader may batch disk flushes and replication messages, but batching cannot
weaken per-entry acknowledgment conditions. A follower's `matchIndex` advances
for quorum counting only through its durable response. The leader counts itself
only after its own durable completion. Commitment follows Raft's current-term
rule; older entries become committed transitively when a current-term entry is
committed.

The state machine stores, for each client, the highest accepted request ID, its
command fingerprint, and its result. Initial clients must serialize mutations
per `client_id` and assign monotonically increasing IDs:

- ID greater than the stored ID: apply once and replace the dedup record.
- ID equal with the same fingerprint: return the stored result; do not mutate.
- ID equal with a different fingerprint: return a permanent request-ID-reuse
  error.
- ID lower than the stored ID: return a permanent stale-request error.

Because the rule is evaluated during deterministic application, speculative
leader-side duplicate checks are optional optimizations, not correctness
dependencies. The per-client record is included in snapshots. Admission limits
cap the number of active client identities; safe reclamation will require an
explicit session-expiration command rather than local time-based eviction.

## 9. Linearizable read lifecycle

```text
GET reaches leader
      |
      v
verify leader has committed an entry in its current term
      |
      v
start ReadIndex context and contact voting quorum
      |
      +-- leadership lost / timeout --> retryable error, no value returned
      |
      v
capture readIndex after quorum confirms current leadership
      |
      v
wait until state-machine lastApplied >= readIndex
      |
      v
apply thread performs lookup
      |
      v
return value / NOT_FOUND
```

The ReadIndex quorum exchange is not replaced by elapsed-time or clock
assumptions. Concurrent writes may linearize before or after the read at a point
consistent with the returned state. A follower never serves a public `GET` in
the initial design, even if it believes it is caught up.

## 10. WAL and hard-state lifecycle

The storage layer maintains:

- checksummed hard state: current term, voted-for node, and generation;
- immutable, numbered WAL segments containing ordered Raft entries;
- a manifest identifying the active format, snapshot, and segment set;
- checksummed snapshots with last-included index and term.

Before granting a vote or sending any response whose safety depends on updated
term, vote, or log contents, the corresponding state is durably flushed. WAL
records are length-delimited and checksummed over metadata and payload. Segment
headers bind files to the cluster, node, format version, and first index.

```text
logical entry
    |
encode + checksum
    |
append to active segment
    |
optional group with adjacent appends
    |
fdatasync
    |
durable completion(index) -> Raft owner
    |
roll segment when threshold reached
    |
fsync new file and parent directory before manifest publication
```

A short final record caused by interruption is ignored and truncated to the
last complete record during recovery. A checksum failure, invalid index
sequence, or corruption away from an incomplete tail is not silently repaired;
the node fails closed and requires peer-based repair or operator action.

## 11. Recovery process

Startup does not accept client traffic until local validation and Raft rejoin
are complete:

1. Acquire an exclusive data-directory lock and validate node and cluster IDs.
2. Read both generations of hard state, select the newest valid generation, and
   fail if no valid copy exists.
3. Load the manifest and newest fully published snapshot; validate length,
   checksum, index, term, and format.
4. Restore KV and deduplication state from the snapshot.
5. Scan required WAL segments in order, enforcing continuous indexes and valid
   checksums. Truncate only an incomplete final record.
6. Reconstruct the Raft log. Presence in the WAL alone does not prove an entry
   committed.
7. Apply only entries covered by a durable commit watermark, if present. After
   joining a leader, apply additional entries only when Raft communicates the
   commit index.
8. Reconcile with the cluster as a follower, accepting AppendEntries or an
   InstallSnapshot. Do not campaign until persistent term/vote state is ready.
9. Enable client handling. Until leadership is known, return a retryable
   no-leader response rather than stale data.

Recovery is deterministic for the same valid on-disk bytes. No wall clock,
directory enumeration order, or arrival order of concurrent connections changes
the recovered logical state.

## 12. Snapshot and compaction process

Snapshot creation is triggered by WAL bytes or applied-entry count, not by a
correctness-critical wall-clock deadline.

1. The apply thread selects an applied index and term and creates an immutable
   logical image containing KV data and deduplication state.
2. The apply pause is bounded and measured. The initial implementation may make
   a copy; it must reject or defer a snapshot if memory headroom is insufficient.
3. The snapshot worker serializes entries in deterministic key order to a unique
   temporary file while computing a checksum.
4. It flushes the file, atomically renames it to its final name, and fsyncs the
   parent directory.
5. It publishes a new checksummed manifest generation and fsyncs the directory.
6. Only after publication may obsolete local WAL segments be deleted. The node
   retains enough state to install the snapshot on lagging followers.

A crash before publication leaves the old snapshot authoritative. Temporary
files are ignored and cleaned after validation on a later startup. Snapshot
installation also uses temporary-file, verification, flush, rename, and
directory-flush steps before changing the live state.

## 13. Metrics path

Hot paths update fixed-cardinality per-thread counters and histograms. The
exporter periodically aggregates them without acquiring Raft or storage owner
locks.

```text
reactor / Raft / WAL / apply / snapshot
             |
       local counters
             |
       periodic aggregate
             |
      metrics snapshot
             |
      scrape / log output
```

Required families include connection counts and bytes, queue occupancy and
backpressure, request outcomes and latency, Raft role/term/commit/applied index,
peer replication lag, WAL append/flush latency and bytes, snapshot duration and
size, recovery outcomes, and deduplication outcomes. Keys, client IDs, request
IDs, and peer addresses are not metric labels. Logging and metrics failures do
not change consensus decisions.

## 14. Shutdown sequence

Shutdown is explicit and idempotent:

1. Enter `DRAINING`; stop accepting new connections and disable new requests on
   existing client connections.
2. Return retryable shutdown responses where output capacity permits. Stop new
   campaigns and, if leader, optionally send a bounded leadership-transfer hint;
   correctness never depends on transfer succeeding.
3. Drain already accepted request-executor work for a configured deadline.
4. Let Raft finish or abandon in-flight proposals. Abandonment drops only local
   waiters; committed entries remain authoritative.
5. Drain committed commands through the apply thread and persist any hard state
   that must precede protocol responses.
6. Flush and close the WAL worker. Stop snapshot creation; either finish an
   already publishing snapshot or leave its temporary file unpublished.
7. Route final responses while connections still exist, then close sockets in
   the reactor thread.
8. Stop metrics and logging after all producers have joined.
9. Release objects in reverse ownership order and finally release the data lock.

If the deadline expires, the process may exit immediately. Crash recovery must
make this equivalent to a crash at that point; orderly shutdown is not a safety
requirement.

## 15. Risk register

| Rank | Risk | Failure mode | Primary mitigation and evidence required |
|---:|---|---|---|
| 1 | Raft implementation error | split brain, lost commit, divergent logs | executable state-machine model, paper-conformance tests, randomized histories, invariant assertions |
| 2 | Incorrect flush ordering | acknowledged write disappears after restart | fault-injection at every write/flush boundary; power-cut tests on supported filesystems |
| 3 | Recovery accepts torn/corrupt data | silent state divergence | checksums, strict index validation, fail-closed policy, corpus and fuzz tests |
| 4 | ReadIndex mistake | stale value returned as linearizable | model tests across partitions and leadership changes; never substitute a lease initially |
| 5 | Queue or buffer growth | out-of-memory under overload or slow peers | item and byte caps at every boundary; backpressure integration and soak tests |
| 6 | Deduplication ambiguity | retry applies a mutation twice or rejects a valid request | serialized client contract, replicated fingerprints/results, crash/retry history tests |
| 7 | Snapshot/WAL race | missing entries or premature log deletion | publication protocol, index/term checks, crash injection at every snapshot step |
| 8 | Shutdown lifetime race | use-after-free, lost completion, deadlock | explicit ownership, connection generations, phased joins, sanitizer stress tests |
| 9 | Single-thread bottleneck | high tail latency or election instability under load | priority for Raft messages, bounded executor work, metrics, profile before sharding |
| 10 | Operational identity/config error | node joins wrong cluster or reuses another node's disk | persisted cluster/node IDs, startup validation, immutable initial membership |

Security, dynamic membership, online backup, and disaster recovery are important
future work. They do not weaken the safety rules above and must receive separate
design review before implementation.
