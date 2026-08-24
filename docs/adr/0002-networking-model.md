# ADR 0002: Level-Triggered epoll Networking

- Status: Accepted
- Date: 2026-08-24

## Context

ForgeKV needs persistent client and Raft connections on Linux. TCP can return
partial frames, combine frames, stop making progress, and fail after either side
has handed bytes to its kernel. Networking must remain responsive while storage
flushes and Raft work are pending, and slow consumers must not create unlimited
buffers.

## Decision

Use one nonblocking, level-triggered `epoll` reactor for both client and peer
listeners. Client and Raft traffic use separate configured listening sockets and
message namespaces. The reactor owns every socket and all connection-local
state.

```text
client listener ---+
                   |
peer listener -----+--> level-triggered epoll loop
                             |
          +------------------+------------------+
          |                  |                  |
       accept             readable           writable
          |                  |                  |
    connection state   input + parser    drain output queue
                             |
                       bounded dispatch
```

Each connection tracks:

- file descriptor and monotonically increasing generation;
- connection kind, state, authenticated/configured peer identity when relevant;
- incremental parser state and buffered input bytes;
- ordered output chunks and current partial-write offset;
- in-flight request count and buffered output bytes;
- last monotonic activity time;
- whether read interest is paused by local backpressure; and
- close reason and graceful-close deadline.

All sockets use nonblocking accept, read, and write. The loop handles `EINTR`,
`EAGAIN`, partial writes, half-close, reset, and error readiness explicitly.
`SO_REUSEADDR` is enabled on listeners. `TCP_NODELAY` is enabled by default for
latency-sensitive client responses and Raft control messages; batching occurs at
the protocol and WAL layers rather than through Nagle delays.

## Level-triggered choice

Level-triggered readiness is selected because it is more forgiving and easier to
reason about: if work remains, readiness is reported again. The handler still
reads or writes until `EAGAIN` within a bounded per-iteration byte budget. The
budget prevents one hot connection from starving timers or other connections.

Edge-triggered epoll can reduce readiness notifications but requires every
transition and drain loop to be exact. A missed drain can stall a connection
indefinitely. That complexity is not justified before profiles show readiness
notification overhead is material.

## Framing and dispatch

TCP is treated only as an ordered stream within one live connection. An
incremental parser accepts arbitrary chunks and emits complete validated frames.
It must support headers and payloads split across any number of reads and
multiple frames in one read.

Before allocation or dispatch, the parser validates magic, version, namespace,
message type, declared lengths, arithmetic overflow, integrity fields, and hard
key/value/frame limits. Exact byte layout is deferred to the wire-protocol phase.
Unknown versions and invalid enum values are protocol errors.

Frame correlation IDs route responses; mutation identity is separately defined
by `client_id` and `request_id`. A transport reconnect or new correlation ID does
not create a new logical mutation.

## Backpressure

Every limit has a configurable hard cap and lower resume threshold:

- input bytes per connection;
- output bytes per connection;
- requests in flight per connection;
- total connections;
- global input/output bytes;
- global outstanding client work; and
- reserved global capacity for Raft peer work.

When downstream capacity is unavailable, the reactor removes read interest for
affected client connections. It resumes only below the low-water mark. If a full
request is known and response capacity exists, the server may return `BUSY`.
Malformed or persistently abusive clients are disconnected. Output exceeding its
hard cap closes the connection; responses are never buffered without bound.

Peer traffic cannot bypass byte limits, but it receives reserved capacity and
priority so client overload does not starve heartbeats. A peer falling too far
behind is caught up with a snapshot rather than retaining unlimited queued log
bytes.

## Connection failure and retry

Client disconnect does not cancel accepted Raft proposals. The result is retained
through replicated deduplication state and may be recovered by retrying the same
logical request.

Peer connections reconnect with capped exponential backoff and jitter. Node IDs
and cluster IDs are validated during a peer handshake. Simultaneous duplicate
peer connections are resolved deterministically; correctness relies on Raft RPC
identity and terms, not on which TCP connection survives.

Successful socket write means only that bytes were accepted by the local kernel.
It never proves peer receipt, WAL durability, Raft commitment, or state-machine
application.

## Alternatives considered

### Blocking socket per thread

Rejected because connection count maps directly to thread count and stacks, and
blocking handlers still contend on central consensus state.

### Edge-triggered epoll

Deferred because it increases state-transition risk for limited demonstrated
benefit at the initial cluster and connection scale.

### `io_uring`

Deferred. It can unify network and storage submission but adds kernel-version,
cancellation, buffer-lifetime, and completion-order complexity. The initial
performance question should be measured with epoll first.

### Reactor per core

Deferred until one reactor is measured as a bottleneck. Adding reactors requires
a design for connection distribution, global admission, cross-thread response
routing, and shutdown.

## Consequences

The initial reactor is intentionally simple and observable, but it is a possible
single-core limit. Level-triggered operation can emit more readiness events than
edge-triggered operation. Per-iteration work budgets and queue metrics are
required to prevent fairness problems and determine whether later sharding is
justified.

## Required evidence

Integration tests must cover real TCP fragmentation, coalesced frames, partial
writes, peer half-close, reconnect, slow readers, slow senders, queue saturation,
and file-descriptor reuse. Metrics must expose active connections, accepted and
closed totals, bytes read/written, buffer bytes, requests in flight, reactor-loop
delay, and backpressured connections.
