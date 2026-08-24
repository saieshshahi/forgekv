# ADR 0005: Leader-Only Linearizable Client Operations

- Status: Accepted
- Date: 2026-08-24

## Context

ForgeKV must define what clients can infer from successful operations, redirects,
timeouts, retries, and leadership changes. Serving local follower state is fast
but can return stale values. Leader leases can avoid a quorum round but require a
carefully proven clock and timing model. Retrying a timed-out mutation can create
duplicate effects unless logical request identity is replicated with user state.

## Decision

All initial successful client operations are linearizable and are served by the
Raft leader.

- `PUT(key, value)` orders an overwrite command through Raft.
- `DELETE(key)` orders a delete command through Raft and returns whether the key
  existed at application.
- `GET(key)` uses quorum-confirmed Raft ReadIndex, waits until the apply thread
  has reached that index, and then reads the state machine.

Followers never serve public reads. They return `REDIRECT` with a leader endpoint
only when known from current Raft state; otherwise they return a retryable
no-leader/unavailable response. A redirect is a hint, not proof that the target
will still be leader.

## Linearization points

| Operation | Linearization point |
|---|---|
| Successful `PUT` | commitment of its Raft log entry |
| Successful `DELETE` | commitment of its Raft log entry |
| Successful `GET` | a point after quorum confirmation represented by ReadIndex and before the state-machine lookup completes |
| Duplicate retry result | the original command's commitment/application; retry itself has no new effect |
| Rejected stale or reused ID | validation during deterministic application; no user-state mutation |

The leader waits for application before returning a mutation result, so a
subsequent successful read routed to any current leader can observe the committed
write according to real-time ordering.

## ReadIndex policy

The leader does not serve a `GET` merely because it currently believes it is
leader. It first ensures it has committed an entry in its current term, then uses
a unique ReadIndex context in heartbeat/AppendEntries exchanges with a quorum.
On quorum confirmation it captures `readIndex`. The apply thread services the
lookup only after `lastApplied >= readIndex`.

Leadership loss, higher-term observation, quorum timeout, or shutdown before
lookup completion yields a retryable error and no value. The initial system does
not use clock-based leader leases.

## Request identity and duplicate policy

Every mutation carries:

- `client_id`: a stable, high-entropy client identity;
- `request_id`: a monotonically increasing integer within that identity; and
- a command fingerprint covering operation, key, value, and semantic options.

Initial clients must allow only one unresolved mutation per `client_id`. A retry
uses the same request ID and exact command. The replicated state machine stores
the highest processed request ID, its fingerprint, and its result for each
admitted client:

| Incoming ID | Fingerprint | Result |
|---|---|---|
| greater than stored | any valid | apply command once; store ID, fingerprint, and result |
| equal to stored | equal | return stored result without changing user state |
| equal to stored | different | permanent `REQUEST_ID_REUSE` error; no mutation |
| less than stored | any | permanent `STALE_REQUEST` error; no mutation |

This rule is evaluated at log application, even if the leader performs an
earlier cache check. Consequently two leaders or duplicate log entries cannot
apply the logical command twice.

Deduplication state is included in snapshots and restored before service. It is
never evicted by a local LRU or wall-clock TTL. A configured maximum number of
client identities bounds memory; new identities receive `BUSY`/resource-limit
errors at capacity. Safe reclamation requires a future replicated session-close
or epoch protocol.

`GET` carries transport correlation but does not require mutation deduplication.

## Timeout and cancellation behavior

A client timeout or connection loss means **outcome unknown**. It does not
cancel a command already accepted by Raft. The command might not exist, might be
uncommitted, might later commit, or might already have applied with its response
lost.

The client should:

1. keep the same `client_id`, `request_id`, and command bytes;
2. follow a current redirect or rediscover the leader;
3. retry with bounded exponential backoff and jitter; and
4. avoid sending the next mutation ID until this result is definitive.

The server does not promise that every accepted request completes: loss of a
quorum can prevent progress indefinitely. It does promise never to invent a
success response to resolve uncertainty.

## Session guarantees

With a serialized mutation stream and leader-only linearizable reads, a client
receives:

- read-your-writes after a successful write response;
- monotonic reads among non-overlapping successful reads;
- no stale successful follower responses; and
- at-most-once state-machine effect for conforming same-ID mutation retries.

This is not a transaction system. Separate operations from one or more clients
can interleave. A future batch command can become one Raft entry and one
state-machine transition without changing the consistency model.

## Alternatives considered

### Follower stale reads

Rejected initially because a single `GET` operation would have two consistency
meanings and clients could accidentally consume arbitrarily old state during a
partition.

### Follower linearizable reads

Possible using a leader-mediated barrier or read-index forwarding, but it adds an
RPC hop and more failure states without reducing leader involvement. Deferred.

### Leader lease reads

Potentially lower latency, but correctness depends on rigorously bounded clock
behavior and lease/election interaction. Rejected until a separate proof and ADR
exist.

### No duplicate suppression because PUT/DELETE are idempotent

Rejected. Repeated absolute operations can still reorder around later commands,
and future batches may not be naturally idempotent. Logical identity should be
correct from the first protocol version.

### Arbitrary concurrent request IDs per client

This requires a retained response window or sparse set whose safe garbage
collection needs explicit acknowledgments. The initial serialized stream keeps
deduplication deterministic and compact. Concurrent client workloads can use
multiple client IDs until a session-window protocol is designed.

## Consequences

Every `GET` normally costs a quorum communication round and all traffic targets
the leader. This limits read scalability but makes the public API simple and
testable. Deduplication consumes bounded per-client replicated state and imposes
a serialized mutation contract. Those tradeoffs are accepted for initial
correctness.

## Required evidence

History tests must cover concurrent clients, response loss, same-ID retries to a
new leader, ID reuse with changed content, stale IDs, leadership changes during
ReadIndex, minority partitions, lagging application, and snapshot/recovery of
deduplication state. A linearizability checker should validate successful public
histories generated under randomized failures.
