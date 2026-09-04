# Operability

ForgeKV exposes a separate, loopback-bound administrative HTTP listener. Set
it with `--admin-bind` and `--admin-port`; port `0` asks the kernel for an
ephemeral port and the selected value appears in the startup line.
The listener is intentionally small: one joinable worker, a 4 KiB request
limit, one-second socket deadlines, and only three `GET` routes.

## Health and readiness

- `GET /health` returns 200 when startup completed, the Raft owner is running,
  the process has not faulted, and shutdown has not begun. A follower is
  healthy.
- `GET /ready` returns 200 only for a healthy leader whose peer transport is
  locally enabled. This deployment sends leader-only reads and writes, so a
  follower returns 503 even though it remains healthy.
- `GET /metrics` returns Prometheus text format. Scraping reads atomics and
  Linux `/proc`; it does not post work to or lock the Raft owner.

Readiness is not a proof of external reachability. A network partition that is
invisible to the process can leave an old leader reporting ready until it
observes loss of leadership. Locally injected peer isolation is reflected
immediately. A future check-quorum mechanism can make readiness sensitive to
recent majority contact.

## Metrics

Process gauges are `forgekv_process_rss_bytes`,
`forgekv_process_open_fds`, and `forgekv_process_threads`. Cumulative CPU time
is `forgekv_process_cpu_seconds_total`. If Linux `/proc` cannot be sampled,
these series are omitted instead of reported as zero.

Network metrics cover client listener sockets, inbound peer listener sockets,
and the short-lived outbound peer RPC sockets:

- `forgekv_network_active_connections`
- `forgekv_network_connections_total`; use Prometheus `rate()` for connection
  rate
- `forgekv_network_rx_bytes_total` and `forgekv_network_tx_bytes_total`
- `forgekv_network_requests_in_flight`
- `forgekv_network_backpressured_connections`
- `forgekv_network_backpressure_events_total`
- `forgekv_network_rejected_requests_total`, counting TCP admission and
  outbound peer-queue capacity rejection

Request metrics are `forgekv_requests_total{op}`, fixed-bucket
`forgekv_request_latency_seconds{op}`, `forgekv_errors_total{type}`, and
`forgekv_requests_in_flight`. Front-door BUSY responses are included even when
the worker cannot invoke the cluster handler. Operations are the closed set
`put`, `get`, `delete`, and `ping`; error labels are `not_found`, `invalid`,
`redirect`, `busy`, `internal`, `request_id_reuse`, `stale_request`, and
`capacity_exceeded`. Keys, values, client IDs, request IDs, addresses, and
free-form error text are never labels.

Request latency begins when a worker invokes the cluster handler and therefore
excludes TCP executor queue time. Front-door BUSY rejections are counted in
request and error totals but deliberately have no invented latency sample.
Handler completion records latency and releases the in-flight gauge, while the
final bounded-queue decision records the response outcome. If that queue
replaces a response with BUSY, the request and latency remain counted once and
only BUSY is added to the monotonic error counters.
`forgekv_queueing_latency_seconds` separately measures time in the Raft-owner
queue. Phase 16 will use an end-to-end client benchmark to measure the complete
latency, including all admission queues.

`forgekv_queue_depth` and `forgekv_queueing_latency_seconds` describe the
single-owner Raft work queue. `forgekv_queue_rejected_total` aggregates
rejections from that queue, TCP admission queues, and the outbound peer queue.

Raft metric names are `forgekv_raft_role{role}` (gauge),
`forgekv_raft_term`, `forgekv_raft_leader_id`, `forgekv_raft_commit_index`,
`forgekv_raft_last_applied`, `forgekv_raft_log_size`,
`forgekv_raft_elections_total`, `forgekv_raft_leadership_changes_total`,
`forgekv_raft_append_entries_total`, and
`forgekv_raft_replication_lag{peer}`. An unknown leader ID is encoded as zero.
Leadership changes count both local transitions into leader and local
transitions out of leader; they are process events, not a deduplicated
cluster-wide leader count.
AppendEntries counts inbound RPCs and attempted outbound sends. Peer labels come
only from the fixed voter set. Lag is emitted only while this node is leader;
the local voter is emitted with zero lag.

Storage metrics are `forgekv_storage_wal_bytes`,
`forgekv_storage_wal_records`, `forgekv_storage_sync_count_total`,
`forgekv_storage_sync_latency_seconds`,
`forgekv_storage_recovery_duration_seconds`, and
`forgekv_storage_snapshot_duration_seconds`. WAL records means retained Raft
records, not historical superseded journal records.

All latency histograms use cumulative buckets at 50, 100, 250, 500, 1,000,
2,500, 5,000, 10,000, 25,000, 50,000, 100,000, and 500,000 microseconds, plus
`+Inf`. The fixed buckets and label sets bound telemetry memory usage.

## Logging

The default sink writes one JSON object per line with UTC timestamp, severity,
escaped message, and an optional numeric `request_id`. Lifecycle, leadership,
and exceptional request paths are logged; successful requests are not logged at
normal verbosity. Logging failures are swallowed so an observability sink
cannot change request or consensus behavior.

Synchronous success logging would serialize workers on formatting and output
locks, add disk or terminal latency to the hot path, and amplify queueing at
saturation. That cost disproportionately damages p99 latency, so metrics—not a
log line per success—are the normal high-volume signal.

Useful Prometheus queries include:

```promql
sum(rate(forgekv_requests_total[5m])) by (op)
histogram_quantile(0.99, sum(rate(forgekv_request_latency_seconds_bucket[5m])) by (le, op))
rate(forgekv_network_connections_total[5m])
max(forgekv_raft_replication_lag) by (peer)
rate(forgekv_queue_rejected_total[5m])
```
