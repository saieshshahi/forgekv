# ForgeKV Invariants

Status: normative Phase 0 safety contract

An implementation is incorrect if it can violate an invariant below under the
faults described in [failure-model.md](failure-model.md). Assertions, tests, and
model checking should reference invariant identifiers.

## 1. Raft election and log safety

- **R-1 Single leader per term:** At most one node can be elected leader in a
  term.
- **R-2 One vote per term:** A node never grants more than one vote in the same
  term, and it durably records the term and vote before granting the vote.
- **R-3 Monotonic term:** A node's current term never decreases, including
  across restart.
- **R-4 Log matching:** If two logs contain entries with the same index and
  term, those entries and every preceding entry have identical terms and command
  bytes.
- **R-5 Leader completeness:** Every entry committed in a term is present in the
  log of every leader elected in a later term.
- **R-6 State-machine safety:** No two nodes apply different commands at the
  same log index.
- **R-7 Current-term commitment:** A leader advances `commitIndex` by counting a
  quorum only for an entry from its current term; prior entries commit
  transitively.
- **R-8 Ordered application:** A node applies committed entries exactly in
  increasing, gap-free log-index order.
- **R-9 No speculative application:** A node never applies an entry merely
  because it is present or locally durable; it must be known committed.
- **R-10 Stable quorum accounting:** A peer index counts toward the strict
  acknowledgment quorum only after that peer reports the entry durably flushed.
- **R-11 Append consistency:** A follower accepts new entries only after the
  preceding index and term match its log, and it removes conflicting uncommitted
  suffix entries before appending replacements.
- **R-12 Snapshot boundary:** Installing a snapshot never moves applied or
  committed state backward.

## 2. Client-visible consistency

- **C-1 Linearizable mutations:** Each successful `PUT` and `DELETE` has one
  linearization point at Raft commitment.
- **C-2 Applied-before-reply:** The leader does not send a success response for
  a mutation until its state machine has applied that committed entry.
- **C-3 Linearizable reads:** A successful `GET` executes on the leader after a
  quorum-confirmed ReadIndex and after `lastApplied >= readIndex`.
- **C-4 No follower reads:** A follower does not serve a public `GET`, even when
  its applied index equals its known commit index.
- **C-5 Timeout uncertainty:** A network timeout or disconnect never implies
  that a mutation failed, succeeded, or was cancelled.
- **C-6 Redirect is not execution:** A redirecting node has not accepted the
  operation into its state machine.
- **C-7 Read result:** `GET(k)` returns the value from the latest mutation of `k`
  ordered before the read's linearization point, or `NOT_FOUND` if none exists.
- **C-8 Delete result:** A committed `DELETE(k)` deterministically records
  whether `k` existed at its application index.

## 3. Duplicate request safety

- **D-1 Command identity:** Every mutation contains `client_id`, monotonically
  increasing `request_id`, and a fingerprint covering the complete logical
  command.
- **D-2 Serialized client stream:** A client has at most one unresolved mutation
  per `client_id`; retries reuse its ID and exact command bytes.
- **D-3 Apply-once effect:** Applying the same latest
  `(client_id, request_id, fingerprint)` more than once changes user state at
  most once and returns the originally stored result.
- **D-4 No ID aliasing:** Reuse of the latest request ID with a different
  fingerprint is a permanent error and never mutates user state.
- **D-5 Stale rejection:** A request ID lower than the client's replicated
  highest ID is rejected and never mutates user state.
- **D-6 Replicated dedup state:** Deduplication metadata changes atomically with
  its command's state-machine effect and is included in snapshots.
- **D-7 No unsafe eviction:** Deduplication state is never discarded using local
  wall-clock time or an unreplicated cache policy. Reclamation requires a future
  replicated session-expiration protocol.
- **D-8 Bounded admission:** The node rejects new client identities before
  deduplication metadata can grow beyond its configured capacity.

## 4. Persistent storage and durability

- **S-1 Leader-local stability:** The leader's copy of an acknowledged entry was
  durably flushed before acknowledgment.
- **S-2 Quorum stability:** An acknowledged mutation's log entry was durably
  flushed on a majority of configured voting nodes.
- **S-3 Crash survival:** Under the stated storage assumptions, acknowledged
  mutations survive process restart and loss of fewer than a quorum of disks.
- **S-4 Stable-before-response:** A follower never confirms a vote, term update,
  or AppendEntries result before all persistent state required for that response
  is flushed.
- **S-5 Monotonic indexes:** WAL entry indexes and local durable-index completion
  notifications are monotonic; entries contain no gaps within a retained log.
- **S-6 Complete-record rule:** A partially written WAL record is never decoded
  or applied as a complete record.
- **S-7 Corruption detection:** WAL, hard-state, manifest, and snapshot data are
  checksummed and structurally validated; invalid data is never silently
  accepted.
- **S-8 Fail closed:** Corruption other than an incomplete final append prevents
  the node from serving until it is repaired from a trustworthy peer or backup.
- **S-9 Publication ordering:** A snapshot or manifest is authoritative only
  after its content is flushed, atomically published, and its parent directory
  is flushed.
- **S-10 Safe compaction:** A WAL segment is deleted only after an authoritative
  snapshot covers every entry whose only local copy the segment held.
- **S-11 Identity binding:** Persistent files are bound to the expected node ID,
  cluster ID, and storage format before use.

## 5. Recovery and snapshots

- **X-1 Deterministic recovery:** Identical valid persistent bytes and cluster
  input produce identical recovered Raft and state-machine state.
- **X-2 No commit inference:** Recovery never infers commitment solely from an
  entry's presence in the WAL.
- **X-3 Snapshot consistency:** A snapshot contains KV data and deduplication
  data from one exact applied index and identifies its matching term.
- **X-4 Replay boundary:** Entries at or below the installed snapshot index are
  not applied again; later known-committed entries are applied once in order.
- **X-5 Temporary-file isolation:** An unpublished snapshot temporary file is
  never selected for recovery.
- **X-6 Install verification:** A received snapshot is fully validated and
  durably published before it replaces live local state.
- **X-7 No service while uncertain:** A recovering node does not serve clients
  until persistent validation is complete and its Raft role is established.

## 6. Concurrency and resource bounds

- **T-1 Declared ownership:** Every mutable object has exactly one owner thread
  or an explicitly documented synchronization policy.
- **T-2 No blocking reactor:** A network callback never synchronously performs
  disk I/O or any operation with unbounded duration.
- **T-3 No blocking Raft owner:** The Raft thread requests storage and network
  work asynchronously and remains able to process timers and completions.
- **T-4 Bounded queues:** Every inter-thread queue has item and byte limits; no
  producer can enqueue unlimited work.
- **T-5 Bounded connections:** Input bytes, output bytes, parsed requests, and
  in-flight operations are bounded per connection and globally.
- **T-6 Backpressure propagation:** Saturation stops or rejects upstream work
  before allocating beyond the next boundary's capacity.
- **T-7 Stable completion routing:** An asynchronous completion cannot access a
  destroyed or reused connection; routing includes a generation token.
- **T-8 Ordered shutdown:** An object is not destroyed until every producer that
  can reference it has stopped and all relevant owner threads have joined.
- **T-9 Snapshot immutability:** The snapshot worker sees an immutable logical
  image; concurrent application cannot change the bytes represented by it.

## 7. Protocol and network safety

- **N-1 Stream semantics:** Correctness never assumes one `recv` call equals one
  protocol frame.
- **N-2 Validate before allocate:** Attacker-controlled lengths and enum values
  are validated against hard limits before allocation, copying, or dispatch.
- **N-3 Frame integrity:** Truncated, oversized, malformed, unsupported-version,
  and integrity-invalid frames never reach Raft or the state machine.
- **N-4 Delivery is not commitment:** A successful socket write or parse is
  never treated as durable replication, commitment, or application.
- **N-5 Stale RPC rejection:** Terms and request correlation prevent delayed or
  duplicated Raft responses from advancing current peer progress incorrectly.
- **N-6 Peer identity:** A connection cannot claim a voting identity not present
  in the immutable configured membership.
- **N-7 Slow-consumer bound:** A peer or client that stops reading cannot cause
  unbounded retained response data.

## 8. Observability and shutdown

- **O-1 Passive metrics:** Metrics collection and export do not take ownership
  locks or influence consensus outcomes.
- **O-2 Bounded cardinality:** User keys, values, client IDs, request IDs, and
  arbitrary peer-provided strings are never metrics labels.
- **O-3 Logging independence:** Logging failure, throttling, or queue saturation
  cannot block indefinitely or change protocol state.
- **O-4 Crash-equivalent stop:** Forced termination at any shutdown step is
  recoverable using the same rules as an unplanned process crash.
- **O-5 Idempotent shutdown:** Repeated shutdown signals do not restart work,
  double-close owned resources, or violate destruction order.

## 9. Availability boundary

Safety takes priority over availability. Without a mutually communicating
majority of voting nodes with usable persistent storage, ForgeKV does not
complete linearizable reads or writes. A minority partition may redirect or
return retryable errors but never serves stale data as successful output.
