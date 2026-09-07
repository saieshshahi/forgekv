# Deterministic chaos testing

`forgekv-chaos` drives real ForgeKV server processes through concurrent client
traffic, directed network faults, process faults, recovery, a complete durable
restart, and exact verification of acknowledged client state.

## Run a campaign

```bash
./build/debug/chaos/forgekv-chaos \
  --nodes 5 --clients 32 --duration 120 --seed 12345 \
  --server ./build/debug/src/forgekv-server \
  --artifacts ./chaos-artifacts --keep-success
```

Bounds are deliberately finite: 3 or 5 nodes, 1–256 clients, 1–3600 seconds,
and 50–60000 ms between actions. Invalid or duplicate options fail before any
server starts. Exit 0 means all invariants passed, exit 1 means the campaign ran
but failed an invariant, and exit 2 means command-line or replay input was
invalid.

The scheduler can kill or pause nodes, restart dead nodes, partition a node,
isolate a leader, add latency or jitter, reset a seeded percentage of streams,
heal links, and force rapid leader churn. Every directed peer path has its own
bounded TCP proxy. A partition closes existing streams and refuses traffic until
healed. The historical timeline spelling `set_loss` means a deterministic proxy
chunk drop followed by a connection reset; it is retained for replay
compatibility and is **not** kernel packet loss. An over-capacity chunk also
fails the stream instead of silently creating unbounded memory pressure.

Use `--no-chaos` to keep the concurrent client workload, child monitoring,
convergence checks, exact state verification, log scanning, and complete durable
restart while scheduling zero process or proxy faults. Phase 15 uses this mode
under real kernel packet impairment. See
[network fault injection](network-fault-injection.md).

Rapid leader churn is admitted only when the remaining running voters can form
a quorum. It heals peer links first, kills the current leader, waits for its
successor, restarts the old leader, then kills and restarts the successor.

## What is verified

Each client owns one key and one stable 128-bit client ID. A timed-out or reset
mutation is retried with the same monotonically increasing request ID until its
outcome is definitive. After faults stop, the harness heals links, resumes and
restarts nodes, resolves ambiguous operations, waits for one ready leader and
fully applied replicas, restarts every process, waits for convergence again,
and performs linearizable reads of all client-owned keys. A pass therefore
demonstrates that the final durable state equals the state implied by definitive
client acknowledgements for that finite run.

This is effectively-once validation within ForgeKV's documented deduplication
retention limits. It does not claim impossible exactly-once delivery, exhaustive
fault coverage, or production readiness.

## Artifacts and replay

Failed campaigns are always retained. Successful campaigns keep compact
`config.json`, `seed.txt`, and `summary.json` evidence by default; add
`--keep-success` to retain the full passing campaign. The full directory
contains:

- `config.json`: bounded campaign inputs.
- `seed.txt`: the exact scheduler/proxy/client seed.
- `children.txt`: server process IDs captured for cleanup diagnostics.
- `timeline.jsonl`: planned actions plus observed start/finish times.
- `history.jsonl`: every physical request attempt and logical request ID.
- `logs/node-N.log`: server stdout and structured logs.
- `metrics/node-N.prom`: final per-node Prometheus snapshots.
- `data/node-N/`: durable node state retained for failed/full campaigns.
- `summary.json`: result, counts, convergence, restart status, and diagnostic.
- `replay.txt`: a ready-to-run replay command.

Replay realized choices with:

```bash
./build/debug/chaos/forgekv-chaos \
  --replay ./chaos-artifacts/timeline.jsonl \
  --server ./build/debug/src/forgekv-server \
  --artifacts ./chaos-replay --keep-success
```

Replay loads the original node count, client count, duration, action interval,
and seed from the adjacent `config.json`. The seed makes scheduler, client IDs,
cluster ID, and proxy reset decisions stable, while the timeline fixes realized
actions. OS scheduling, TCP timing, election timing, and process interleavings
are intentionally not claimed to be bit-for-bit reproducible.

The harness asks Linux for ephemeral ports and releases each reservation before
the server binds it. A concurrent process can rarely claim that port in the
small handoff window; such a launch is reported as a failed campaign. Use
separate network namespaces for highly parallel campaign farms.

High-volume campaigns synchronously persist attempt history for crash-useful
diagnostics. That durability has measurable throughput cost and is a test-tool
choice, not a recommendation for the production request path.

Failure summaries retain the first evidence and use stable categories:
`configuration`, `launch`, `client protocol`, `invariant`,
`convergence timeout`, `child crash`, `artifact I/O`, or `internal` (with
`signal` used for an operator interruption). Failed runs still produce one
bounded metrics file per node; an unavailable node gets a collection-error
marker rather than silently omitting the artifact.
