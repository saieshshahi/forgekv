# Phase 13: Operability Design

## Operator surface

Each process owns a separate bounded HTTP administrative listener. It binds to
loopback by default and supports only `GET /health`, `GET /ready`, and
`GET /metrics`. `--admin-port` selects a stable port; zero asks the kernel for an
ephemeral port and the selected value appears in the startup line. The listener
has one joinable worker, a fixed request-size limit, a one-second I/O deadline,
and never executes consensus code.

`/health` answers 200 when startup completed and the owner has not faulted or
begun shutdown. It does not require leadership or a quorum. `/ready` answers 200
only when the process is healthy, currently believes it is the Raft leader, and
its peer transport is locally enabled, because this deployment's expected
client traffic consists of leader-only writes and reads. A follower is healthy
but not ready and exposes its current role in the body. An externally imposed
partition can remain invisible until the old leader observes a higher term;
check-quorum is a documented future improvement rather than an implied current
guarantee.

## Metric ownership

Hot paths update only fixed-cardinality relaxed atomics and fixed-bucket latency
histograms. The scrape path takes snapshots of those values and existing network
counters; it never posts work to or locks the Raft owner. The owner publishes
Raft gauges after each state transition. Peer IDs are labels drawn only from the
fixed configured voter set. Keys, client IDs, request IDs, endpoints, and error
text are never labels.

Process CPU, RSS, open descriptors, and thread count are sampled from Linux
`/proc` at scrape time. Connection rate is derived from the monotonic accepted
connection counter using Prometheus `rate()`, rather than exported as a
scrape-interval-dependent local calculation.

Request latency and queueing/sync/snapshot duration use cumulative Prometheus
histograms with fixed microsecond buckets. Operations and error types are closed
enums. Queue admission failures and backpressure events are counters; depths and
in-flight work are gauges.

## Storage and Raft observations

The persistence driver reports every durable synchronization duration through a
non-throwing observer after the durability decision is complete. Observer
failure is swallowed so telemetry cannot change consensus behavior. Recovery
duration is measured around durable node open. WAL bytes come from the current
journal file size, while retained record count comes from the published Raft
snapshot. Background snapshot duration is recorded by the snapshot worker.

Raft metrics include role, term, known leader, commit/applied indexes, retained
log size, elections, leadership changes, AppendEntries traffic, and per-peer
replication lag. Lag is zero for the local member and, on a leader, is the
difference between its last log index and each peer's match index. A follower
does not claim knowledge it lacks; peer lag is omitted there.

## Logging

Default logs are single-line JSON containing UTC timestamp, severity, and an
escaped message. Lifecycle, leadership, persistence/snapshot failure, and
unexpected internal request failures are logged. Successful requests are not
logged at normal verbosity; request ID is included on exceptional request paths.
Synchronous per-success logging is deliberately avoided because output locks,
formatting, and terminal/disk writes serialize hot workers and amplify p99
latency under load.

## Failure semantics

Administrative parsing errors receive bounded 400/404/405 responses. A failed
metrics scrape cannot fault Raft. If `/proc` fields are unavailable, their
metrics are omitted rather than invented as zero. The admin listener is stopped
and joined during normal shutdown; Phase 20 will extend the broader shutdown
ordering and drain contract.
