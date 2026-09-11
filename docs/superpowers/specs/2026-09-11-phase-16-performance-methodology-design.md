# Phase 16: Performance Methodology Design

## Status

Approved. The project owner has pre-approved the recommended design for each
roadmap phase. This document defines the Phase 16 measurement contract; it does
not weaken ForgeKV's correctness or durability contracts.

## Purpose

Phase 16 builds a repeatable benchmark harness for finding the sustainable
operating region of a real ForgeKV cluster. Its primary outputs are raw,
machine-readable evidence and an honest methodology, not a single impressive
number. The harness must reveal throughput saturation, latency growth, queueing,
and resource limits while preserving enough metadata to decide whether two runs
are comparable.

## Approaches considered

### Native C++ experiment runner — selected

A native runner owns real ForgeKV server processes, generates protocol-correct
traffic, samples observability and Linux process counters, and writes versioned
evidence. It reuses the bounded process lifecycle and protocol code already in
the repository. This has more initial implementation cost than a script, but it
keeps parsing, cleanup, numeric bounds, and tests in one language.

### Shell or Python orchestration around the existing load generator

This would be quick to prototype, but the current load generator emits one
ad-hoc line and sends only `PING`. Reliable process ownership, HTTP metric
scraping, strict schemas, and Windows/WSL path handling would accumulate in a
second orchestration stack. Rejected for the authoritative harness.

### Google Benchmark only

Google Benchmark remains useful for isolated storage, protocol, Raft, and
snapshot microbenchmarks. It does not model a multi-process cluster, elections,
TCP queues, or end-to-end client latency. Rejected as the Phase 16 system
methodology.

## Scope and semantic honesty

The harness has two explicit scopes rather than an invalid Cartesian product:

1. `cluster` runs 1, 3, or 5 real server processes under ForgeKV's only cluster
   durability contract: strict quorum fsync before acknowledgment. The evidence
   names this `quorum-sync`.
2. `storage` compares the existing standalone `async`, `sync`, and
   `group-commit` modes. These results do not include Raft, networking, or
   quorum replication and must never be presented as cluster results.

Phase 16 will not add relaxed cluster acknowledgment modes merely to fill a
benchmark table. That would change the product's failure semantics and conflict
with ADR 0004. A future relaxed mode needs its own design, failure tests, and
visibly different guarantee.

The required cluster variables are:

- node count: 1, 3, 5;
- client concurrency: 1, 4, 16, 64, 256;
- value size: 100 B, 1 KiB, 4 KiB, 64 KiB;
- mixes: 100% read, 95/5, 80/20, 50/50, and 100% write;
- durability label: `quorum-sync`.

The storage scope uses the same concurrency and value sizes for 100% writes in
all three standalone durability modes. Existing Google Benchmark microcases
remain the detailed single-operation storage evidence.

## Architecture

The implementation adds four focused units under `bench/system/`:

- `workload`: validated scenario definitions, deterministic operation selection,
  request construction, and bounded latency histograms;
- `client_driver`: persistent TCP workers, leader redirection, warmup and
  measurement barriers, response validation, and per-worker counters;
- `sampler`: admin metric scraping plus `/proc/<pid>` resource snapshots and
  time-series maxima;
- `benchmark_runner`: matrix expansion, process-cluster ownership, prefill,
  repeated trials, aggregation, evidence publication, and CLI behavior.

`chaos::ProcessCluster` remains the one owner of server PIDs and data
directories. It gains support for a one-node voter set and read-only access to
node endpoints/PIDs already used by the sampler. Benchmark operation generation
does not depend on the Phase 14 fault scheduler or proxy.

The existing `forgekv_load_generator` becomes a thin single-scenario frontend
over the reusable workload/client-driver library. `forgekv_benchmark` is the
matrix runner. This avoids two traffic implementations producing subtly
different results.

## Scenario and request model

Every scenario is immutable after validation and includes scope, node count,
concurrency, key/value sizes, read and write percentages, durability label,
warmup duration, measurement duration, trial count, seed, and optional CPU set.
Bounds reject zero durations/counts, unsupported percentages or sizes, more
than 256 clients, and combinations whose encoded payload exceeds protocol
limits. Duplicate or unknown CLI options are errors.

Each worker has a stable nonzero `client_id` derived from the scenario and
worker index, and monotonically increasing `request_id` values. Writes use the
normal replicated command encoding so deduplication semantics are exercised.
Reads use the linearizable GET path. A deterministic counter-based selector
chooses operations, so scheduling does not change the intended mix. Each worker
uses a distinct deterministic PRNG stream for keys and values.

Before a read-bearing trial, the runner fills a bounded keyspace through
acknowledged writes and verifies readiness. Warmup traffic is then issued and
its measurements discarded. Measurement begins only after all workers pass a
barrier. The default pipeline depth is one because request latency must include
the actual end-to-end service and queueing delay; a nondefault pipeline depth is
recorded and cannot be compared silently with depth one.

Redirects reconnect to the advertised leader endpoint within a bounded retry
budget. `BUSY`, timeout, invalid response identity, connection loss, and all
other non-success outcomes are counted by type. A trial with protocol errors,
lost responses, failed server processes, missing evidence, or an incomplete
measurement window is invalid rather than quietly included in aggregates.

## Timing and latency statistics

The default publishable preset uses 10 seconds of warmup, 30 seconds of
measurement, and five trials per scenario. A `smoke` preset uses one warmup
second, two measurement seconds, and one trial and is always labeled
`publishable=false`.

Timing uses `CLOCK_MONOTONIC`/`steady_clock`. Throughput divides successful
measurement-window completions by the actual measurement duration. Latency
starts immediately before the client sends a request and ends after the full,
validated response is parsed, so it includes network, server admission, Raft,
storage, apply, and response queueing.

A mergeable bounded histogram records latency without retaining one allocation
per operation. Its range is 1 microsecond through 60 seconds with enough
sub-buckets to keep reported quantile error below one percent. Results include
p50, p95, p99, maximum, and p99.9 only when at least 10,000 successful samples
exist. The histogram reports overflow explicitly and any overflow invalidates
the latency distribution.

Across trials the aggregate reports arithmetic mean, sample standard deviation,
coefficient of variation, minimum, and maximum for throughput and each emitted
latency percentile. It retains every raw trial; aggregate numbers never replace
the observations.

## Resource and queue measurements

The sampler captures a baseline immediately before measurement, samples every
100 ms during measurement, and captures a final snapshot. It records:

- process CPU time and normalized CPU utilization;
- RSS and peak sampled RSS;
- voluntary and involuntary context-switch deltas from `/proc/<pid>/status`;
- read/write byte deltas from `/proc/<pid>/io`;
- ForgeKV network receive/transmit byte deltas;
- Raft-owner queue depth samples, maximum, and time-weighted mean;
- queue rejections and network/request error deltas;
- storage sync count plus the fixed-bucket fdatasync latency distribution;
- per-node and cluster totals, with leader and follower roles identified.

Missing Linux or admin metrics remain null with a reason; they are never encoded
as zero. CPU is reported both as total process CPU seconds and as percentage of
one logical CPU. Disk and network bandwidth divide byte deltas by measurement
duration. Queue depth is a gauge sampled through time, not a monotonic delta.

The Phase 13 histogram cannot produce exact fsync p99.9 beyond its fixed bucket
resolution. Phase 16 reports the containing bucket and sample count rather than
inventing precision.

## Capacity bounds and bottleneck investigation

For each scenario the report computes a transparent application-payload lower
bound. A write with value size `V` in an `N`-node cluster requires roughly `V`
bytes of client ingress and `(N-1) * V` bytes of leader replication egress,
before keys, framing, acknowledgments, retransmission, and WAL encoding. The
calculation is labeled a lower bound, not measured network traffic.

The first campaign must inspect, without speculative optimization:

- leader versus follower CPU and network asymmetry;
- fdatasync latency buckets and syncs per acknowledged write;
- throughput and p99 versus concurrency to identify the knee;
- queue-depth and queue-wait growth near saturation;
- allocation and copy points along socket → parser → command → Raft log → WAL →
  peer frame → state machine;
- existing mutex ownership and any measurable wait evidence;
- request, syscall, and context-switch rates in representative 4 KiB cases.

Phase 16 documents hypotheses and measured evidence. It does not add pools,
sharding, batching, or networking backends unless a later phase validates them.

## Evidence layout and crash-safe publication

An absent output directory is created for each campaign. The runner writes
bounded temporary files, flushes them, renames them, and publishes a manifest
last. Interrupted campaigns retain raw completed trials but have no valid final
manifest.

```text
campaign/
  environment.json
  matrix.json
  trials.jsonl
  aggregates.csv
  bottlenecks.md
  manifest.json
  trials/<scenario-id>/<trial>/
    samples.jsonl
    node-logs/
```

Schemas carry `format_version=1`. Scenario IDs are canonical hashes of semantic
settings, not user-controlled paths. Text fields are escaped and every file,
line, scenario, sample series, and diagnostic has an explicit size/count bound.
Generated campaign evidence stays under ignored `build/benchmark-system/`;
only reviewed summaries and small source data needed for documentation are
committed.

## Environment and comparability

`environment.json` records UTC time, git commit and dirty state, OS/kernel, CPU
model and logical CPU count, memory, compiler and version, CMake build type and
effective C++ flags, executable SHA-256 values, filesystem mount/type, storage
device information when discoverable, CPU affinity, cluster topology, TCP
pipeline depth, and every benchmark option.

A canonical `environment_fingerprint` covers all fields that can materially
change results. Aggregation refuses to combine trials with different
fingerprints. Reports may compare different fingerprints only in separate
groups with the changed fields displayed explicitly.

Optional CPU pinning accepts an explicit validated CPU list and applies affinity
to the runner before children/worker threads are created. It is off by default
because pinning an already constrained WSL/CI environment can make results less
representative. The exact affinity is always recorded.

## Testing

Unit tests cover scenario expansion and bounds, deterministic mixes, client and
request IDs, histogram merging/quantiles/overflow, Prometheus and `/proc`
parsing, counter wrap/regression rejection, aggregation/variance, environment
fingerprints, schema escaping, and capacity arithmetic.

Integration tests run smoke trials against real 1-, 3-, and 5-node clusters;
exercise reads, writes, redirects, prefill, warmup exclusion, metric sampling,
process cleanup, and invalid evidence on server failure; and verify that the
standalone durability cases remain labeled separately. CLI tests cover help,
unknown/duplicate options, unsafe output reuse, presets, filters, and bounds.

The final gate runs debug, release, ASan, UBSan, and TSan suites, repeated smoke
campaigns, and two independent reviews. A short real campaign publishes the
first methodology evidence but is not labeled a full capacity result unless it
uses the publishable timing/trial preset.

## Known limitations

- WSL2 on DrvFS is useful for regression evidence but not representative of a
  native Linux NVMe deployment.
- Kernel scheduling, TCP behavior, background host work, and storage firmware
  prevent bit-for-bit performance reproducibility even with a fixed seed.
- `/proc` I/O counters and application network counters are process-level and do
  not equal physical device or NIC traffic.
- Fixed-bucket server histograms bound telemetry cost but limit percentile
  precision; client latency remains the authoritative end-to-end distribution.
- Phase 16 locates the saturation region. Phase 17 defines overload policy,
  Phase 18 profiles networking, Phase 19 performs deeper CPU/cache analysis, and
  Phase 24 produces the final public graphs.
