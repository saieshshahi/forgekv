# Phase 16 Performance Methodology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible system benchmark harness that locates ForgeKV's sustainable throughput/latency region and emits honest, versioned evidence.

**Architecture:** A reusable C++ benchmark library defines scenarios, drives protocol-correct clients, samples real cluster/process metrics, aggregates repeated trials, and publishes bounded artifacts. A `forgekv-benchmark` frontend owns real clusters through `chaos::ProcessCluster`; the existing load generator becomes a single-scenario frontend over the same driver. Cluster evidence remains strict `quorum-sync`, while existing standalone storage durability modes are measured in a separately labeled scope.

**Tech Stack:** C++20, CMake/CTest, GoogleTest, POSIX sockets/process APIs, Linux `/proc`, ForgeKV admin Prometheus endpoint, JSONL/CSV/Markdown.

**Spec:** `docs/superpowers/specs/2026-09-11-phase-16-performance-methodology-design.md`

## Global Constraints

- Do not change ADR 0004's strict quorum-fsync cluster acknowledgment contract.
- Cluster sizes are exactly 1, 3, and 5; concurrency values are 1, 4, 16, 64, and 256.
- Value sizes are 100 B, 1 KiB, 4 KiB, and 64 KiB; mixes are 100/0, 95/5, 80/20, 50/50, and 0/100 read/write.
- Publishable defaults are 10 seconds warmup, 30 seconds measurement, and five trials; smoke is 1/2/1 and always non-publishable.
- p99.9 is emitted only with at least 10,000 successful samples.
- Generated evidence lives below ignored `build/benchmark-system/`; only reviewed summaries and small source data are committed.
- Every mutation follows test → observed failure → minimal implementation → green focused tests → commit → push.

---

### Task 1: Scenario model and bounded latency histogram

**Files:**

- Create: `bench/system/workload.h`
- Create: `bench/system/workload.cpp`
- Create: `bench/system/latency_histogram.h`
- Create: `bench/system/latency_histogram.cpp`
- Create: `tests/unit/benchmark_workload_test.cpp`
- Modify: `bench/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

- Produces: `enum class BenchmarkScope { cluster, storage };`
- Produces: `enum class DurabilityLabel { quorum_sync, async, sync, group_commit };`
- Produces: `struct WorkloadMix { uint8_t reads; uint8_t writes; };`
- Produces: `struct Scenario`, `validate(const Scenario&)`, `scenario_id(const Scenario&)`, and `required_cluster_matrix()`.
- Produces: `class OperationSelector { Operation next() noexcept; };` with deterministic counter-based selection.
- Produces: `class LatencyHistogram` with `observe(std::chrono::microseconds)`, `merge(const LatencyHistogram&)`, `std::optional<std::chrono::microseconds> quantile(double)`, `count()`, `maximum()`, and `overflow_count()`.

- [x] **Step 1: Write scenario and determinism tests**

Add tests that require the exact 300-scenario cluster matrix, reject mixed scope/durability labels, and prove the first 1,000 operations for a 95/5 selector contain exactly 950 reads and 50 writes and repeat for the same seed.

```cpp
TEST(BenchmarkScenarioTest, RequiredClusterMatrixIsExactAndStable) {
  const auto matrix = required_cluster_matrix();
  ASSERT_EQ(matrix.size(), 3U * 5U * 4U * 5U);
  EXPECT_EQ(matrix.front().durability, DurabilityLabel::quorum_sync);
  EXPECT_EQ(scenario_id(matrix.front()), scenario_id(matrix.front()));
}

TEST(BenchmarkScenarioTest, SelectorRealizesExactMixPerThousand) {
  OperationSelector selector({.reads = 95, .writes = 5}, 160016U, 7U);
  std::size_t reads = 0;
  for (std::size_t i = 0; i < 1000; ++i) reads += selector.next() == Operation::get;
  EXPECT_EQ(reads, 950U);
}
```

- [x] **Step 2: Run the tests and observe the missing benchmark interfaces**

Run: `cmake --build build/debug -j2 && build/debug/tests/forgekv_unit_tests --gtest_filter='BenchmarkScenarioTest.*:LatencyHistogramTest.*'`

Expected: compilation fails because `bench/system/workload.h` and histogram types do not exist.

- [x] **Step 3: Implement validation, canonical IDs, and deterministic selection**

Use fixed arrays for the required matrix, reject zero/unsupported sizes and durations, cap concurrency at 256, require percentages sum to 100, require cluster scope to use `quorum_sync`, and require storage scope to use one of the three standalone modes. Canonical IDs include every semantic field and contain only `[a-z0-9-]`.

- [x] **Step 4: Add histogram boundary, merge, accuracy, and overflow tests**

```cpp
TEST(LatencyHistogramTest, MergesWithBoundedQuantileError) {
  LatencyHistogram left, right;
  for (int us = 1; us <= 10000; ++us) (us % 2 ? left : right).observe(1us * us);
  left.merge(right);
  EXPECT_EQ(left.count(), 10000U);
  const auto p99 = left.quantile(0.99);
  ASSERT_TRUE(p99.has_value());
  EXPECT_NEAR(p99->count(), 9900.0, 99.0);
  EXPECT_TRUE(left.quantile(0.999).has_value());
}
```

Implement fixed logarithmic buckets spanning 1 microsecond to 60 seconds with sub-buckets that bound quantile error to one percent. Saturate counters safely and record underflow/overflow instead of indexing outside the array.

- [x] **Step 5: Run focused and full unit tests**

Run the focused filter above, then `ctest --test-dir build/debug --output-on-failure -R 'Benchmark|Latency|Raft|Storage'`.

Expected: all selected tests pass.

- [x] **Step 6: Commit and push**

```bash
git add bench/system tests/unit/benchmark_workload_test.cpp bench/CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: define phase 16 benchmark scenarios"
git push origin main
```

### Task 2: One-node process clusters and metric sampling

**Files:**

- Modify: `chaos/process_cluster.cpp`
- Modify: `tests/unit/chaos_process_cluster_test.cpp`
- Create: `bench/system/metrics_sampler.h`
- Create: `bench/system/metrics_sampler.cpp`
- Create: `tests/unit/benchmark_metrics_test.cpp`
- Modify: `bench/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

- Consumes: `chaos::ProcessCluster::{admin_port,process_id,refresh}`.
- Produces: one-node support in `ProcessClusterConfig::node_count` without changing 3/5-node behavior.
- Produces: `struct ProcessSample`, `struct NodeSample`, `struct SampleSeries`, and `struct SampleResult { NodeSample sample; std::string error; bool ok() const noexcept; };`.
- Produces: `parse_proc_status(std::string_view, ProcessSample&) -> std::optional<std::string>`, `parse_proc_io(std::string_view, ProcessSample&) -> std::optional<std::string>`, `parse_prometheus(std::string_view, NodeSample&) -> std::optional<std::string>`, and `MetricsSampler::sample(std::span<const NodeEndpoint>) -> SampleResult`.

- [ ] **Step 1: Add a failing one-node lifecycle test**

```cpp
TEST(ProcessClusterTest, OwnsARealSingleVoterCluster) {
  ClusterDirectory directory;
  auto config = test_config(directory.path());
  config.node_count = 1U;
  ProcessCluster cluster(std::move(config));
  ASSERT_TRUE(cluster.prepare().ok());
  ASSERT_TRUE(cluster.start_all().ok());
  EXPECT_NE(cluster.process_id(1), 0);
  EXPECT_TRUE(cluster.stop_all().ok());
}
```

Run the focused test and confirm construction currently rejects node count one.

- [ ] **Step 2: Permit exactly 1, 3, or 5 nodes**

Change the constructor guard only. Retain unique endpoint allocation, fixed
membership generation, process ownership, and bounded stop semantics. Run all
`ProcessClusterTest.*` and a real one-node health/readiness smoke test.

- [ ] **Step 3: Add strict parser tests for `/proc` and Prometheus samples**

Tests cover whitespace, reordered fields, duplicate metric names, NaN/Inf,
negative values, missing optional fields, counter regression, overflow, unknown
labels, and bounded input. Use fixtures containing exact requested metrics.

```cpp
const auto sample = parse_proc_status(
    "VmRSS:\t2048 kB\nThreads:\t7\nvoluntary_ctxt_switches:\t9\n"
    "nonvoluntary_ctxt_switches:\t3\n");
ASSERT_TRUE(sample.ok());
EXPECT_EQ(sample.rss_bytes, 2U * 1024U * 1024U);
```

- [ ] **Step 4: Implement bounded sampling and delta computation**

Read `/proc/<pid>/stat`, `/status`, and `/io` with 64 KiB caps. Fetch `/metrics`
through the existing bounded admin HTTP helper. Preserve unavailable values as
`std::optional` with a diagnostic. Compute deltas only when both samples exist
and cumulative counters did not regress. Aggregate leader/follower/cluster
values without losing per-node samples.

- [ ] **Step 5: Verify focused tests and sanitizers**

Run `BenchmarkMetricsTest.*`, `ProcessClusterTest.*`, ASan focused tests, and
UBSan focused tests. Expected: all pass and a stopped cluster leaves no child.

- [ ] **Step 6: Commit and push**

```bash
git add chaos/process_cluster.cpp bench/system/metrics_sampler.* tests/unit/chaos_process_cluster_test.cpp tests/unit/benchmark_metrics_test.cpp bench/CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: sample benchmark cluster resources"
git push origin main
```

### Task 3: Protocol-correct benchmark client driver

**Files:**

- Create: `bench/system/client_driver.h`
- Create: `bench/system/client_driver.cpp`
- Create: `tests/unit/benchmark_client_test.cpp`
- Create: `tests/integration/benchmark_client_test.cpp`
- Replace: `bench/client/load_generator.cpp`
- Modify: `bench/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

- Consumes: `Scenario`, `OperationSelector`, `LatencyHistogram`, cluster client endpoints, and ForgeKV protocol serializers/parsers.
- Produces: `struct ClientDriverOptions`, `struct TrialCounters`, `struct ClientTrialResult`, and `ClientDriver::run(const ClientDriverOptions&)`.
- Produces: `forgekv_load_generator` as a strict single-scenario CLI over `ClientDriver`.

- [ ] **Step 1: Add failing request-identity and warmup tests**

Use a scripted TCP peer to require stable nonzero client IDs, monotonic request
IDs, exact read/write mix, response-ID matching, redirect following, and no
warmup samples in the returned measurement histogram.

```cpp
TEST(BenchmarkClientTest, WarmupIsExcludedAndResponsesMustMatch) {
  ScriptedServer server({ok(1), ok(2), wrong_id(99)});
  const auto result = ClientDriver{}.run(options_for(server.port(), 1, 2));
  EXPECT_EQ(result.measurement.completed, 1U);
  EXPECT_EQ(result.measurement.protocol_errors, 1U);
  EXPECT_EQ(result.latency.count(), 1U);
}
```

- [ ] **Step 2: Implement persistent depth-one workers**

Each worker owns its socket/parser and bounded buffers. Use a start barrier,
steady-clock deadlines, deterministic key/value generation, normal replicated
mutation encoding, linearizable GET, and exact response validation. Classify
redirect, busy, timeout, connection, protocol, and server errors separately.
Retries preserve `(client_id, request_id)` for an ambiguous mutation.

- [ ] **Step 3: Add real-cluster prefill/read/write integration tests**

Start 1-, 3-, and 5-node smoke clusters. Prefill a small keyspace, run each
required mix, assert successful operations and nonempty latencies, and verify
the final acknowledged state after complete restart for the write-bearing case.
Force one follower endpoint as the initial target and assert redirect recovery.

- [ ] **Step 4: Replace the ad-hoc PING load generator frontend**

Keep strict `--name=value` parsing, add scope/mix/duration/seed options, print
one versioned JSON result, and return nonzero for incomplete/invalid runs. Its
default local-server PING mode is removed because it is not a ForgeKV cluster
benchmark; protocol microbenchmarks already cover that path.

- [ ] **Step 5: Run client, cluster, protocol, ASan, and TSan tests**

Expected: deterministic counts, no leaks/races, bounded shutdown, and zero
unreaped children.

- [ ] **Step 6: Commit and push**

```bash
git add bench/system/client_driver.* bench/client/load_generator.cpp tests/unit/benchmark_client_test.cpp tests/integration/benchmark_client_test.cpp bench/CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: drive reproducible cluster benchmarks"
git push origin main
```

### Task 4: Standalone durability trials and aggregation

**Files:**

- Create: `bench/system/storage_driver.h`
- Create: `bench/system/storage_driver.cpp`
- Create: `bench/system/aggregate.h`
- Create: `bench/system/aggregate.cpp`
- Create: `tests/unit/benchmark_storage_test.cpp`
- Create: `tests/unit/benchmark_aggregate_test.cpp`
- Modify: `bench/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

- Consumes: `storage::KvStore`, `Scenario`, and `LatencyHistogram`.
- Produces: `StorageDriver::run(const StorageTrialOptions&) -> StorageTrialResult`.
- Produces: `aggregate_trials(std::span<const TrialResult>) -> AggregateResult` with mean, sample standard deviation, coefficient of variation, min, and max.

- [ ] **Step 1: Add durability-label and flush-count tests**

Run small async/sync/group-commit cases against temporary WALs. Assert the
result scope is `storage`, labels are exact, operation counts match, async has
no forced sync, sync flushes per mutation, and group commit reduces sync count
under concurrent load without claiming cluster semantics.

- [ ] **Step 2: Implement the bounded storage driver**

Construct one `KvStore` per trial, use worker barriers and existing storage
options, observe end-to-end call latency, collect flush observer counts/bytes,
close deterministically, and reopen to validate the documented recovery result.

- [ ] **Step 3: Add aggregation and environment-mismatch tests**

```cpp
TEST(BenchmarkAggregateTest, UsesSampleVarianceAndRejectsMixedEnvironments) {
  auto trials = three_trials({100.0, 110.0, 90.0}, "env-a");
  const auto result = aggregate_trials(trials);
  ASSERT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(result.throughput.mean, 100.0);
  EXPECT_DOUBLE_EQ(result.throughput.standard_deviation, 10.0);
  trials.back().environment_fingerprint = "env-b";
  EXPECT_FALSE(aggregate_trials(trials).ok());
}
```

- [ ] **Step 4: Implement finite numeric aggregation**

Reject empty sets, invalid trials, nonfinite values, scenario mismatches, and
environment mismatches. Use checked online variance to avoid overflow and emit
null coefficient of variation when the mean is zero.

- [ ] **Step 5: Run focused, recovery, ASan, and TSan tests**

Expected: all pass in all three durability modes with no retained worker.

- [ ] **Step 6: Commit and push**

```bash
git add bench/system/storage_driver.* bench/system/aggregate.* tests/unit/benchmark_storage_test.cpp tests/unit/benchmark_aggregate_test.cpp bench/CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: benchmark storage durability modes"
git push origin main
```

### Task 5: Versioned evidence and matrix runner

**Files:**

- Create: `bench/system/evidence.h`
- Create: `bench/system/evidence.cpp`
- Create: `bench/system/benchmark_runner.h`
- Create: `bench/system/benchmark_runner.cpp`
- Create: `bench/system/main.cpp`
- Create: `tests/unit/benchmark_evidence_test.cpp`
- Create: `tests/integration/benchmark_runner_test.cpp`
- Modify: `bench/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Modify: `.gitignore`
- Modify: `scripts/bench.sh`

**Interfaces:**

- Consumes: process cluster, client/storage drivers, sampler, and aggregation.
- Produces: `EnvironmentRecord`, `environment_fingerprint`, `EvidenceWriter`, `BenchmarkRunner`, and `forgekv-benchmark`.
- Produces: `environment.json`, `matrix.json`, `trials.jsonl`, `aggregates.csv`, `bottlenecks.md`, per-trial samples/logs, and final `manifest.json`.

- [ ] **Step 1: Add failing schema, escaping, and atomic-publication tests**

Require `format_version=1`, stable field order, JSON/CSV escaping, line/file
bounds, canonical scenario directories, executable hashes, dirty-tree state,
and manifest-last publication. Inject failure before each rename; recovery must
retain completed raw trials but never publish a valid manifest.

- [ ] **Step 2: Implement exact environment capture**

Record kernel, CPU, logical CPUs, memory, compiler, build type/effective flags,
git SHA/dirty state, executable hashes, mount/filesystem/device data when
available, affinity, topology, pipeline depth, and options. Hash the canonical
semantic JSON to produce the comparability fingerprint.

- [ ] **Step 3: Add strict CLI/matrix tests**

Cover `--help`, unknown/duplicate options, smoke/full presets, scenario filters,
unsupported values, reused output directories, unsafe roots, absent server,
interruption, and bounded diagnostics. Assert `full` expands the exact required
matrix and `smoke` is always `publishable=false`.

- [ ] **Step 4: Implement orchestration and capacity arithmetic**

For each scenario/trial: create fresh data, start cluster, wait for one ready
leader, prefill if needed, sample baseline, run warmup, sample measurement every
100 ms, capture final values, stop/reap, validate, append raw evidence, then
aggregate matching valid trials. Compute `value_bytes * node_count` as the
minimum aggregate write payload movement and separately expose leader ingress
and replication egress lower bounds.

- [ ] **Step 5: Add failure integration tests**

Kill a server during measurement and require invalid trial evidence, no final
manifest, bounded client exit, reaped PIDs, and retained diagnostics. Interrupt
a multi-scenario smoke run and require the same cleanup behavior.

- [ ] **Step 6: Update the benchmark script and run focused E2E tests**

`scripts/bench.sh system --preset=smoke --nodes=3 --value-size=4096` invokes
the release `forgekv-benchmark`; the
existing default still runs Google Benchmark microcases. Run 1/3/5-node smoke
filters and inspect every output file with a strict test parser.

- [ ] **Step 7: Commit and push**

```bash
git add bench/system bench/CMakeLists.txt tests scripts/bench.sh .gitignore
git commit -m "feat: add reproducible system benchmark runner"
git push origin main
```

### Task 6: Real methodology campaign and bottleneck report

**Files:**

- Create: `docs/performance-methodology.md`
- Create: `docs/performance/phase16-summary.csv`
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-09-11-phase-16-performance-methodology.md`

**Interfaces:**

- Consumes: release `forgekv-benchmark` evidence.
- Produces: reviewed measured methodology, capacity bounds, saturation findings, and exact reproduction commands.

- [ ] **Step 1: Build release and run repeated calibration**

Run smoke across 1/3/5 nodes, all five mixes, and 100 B/4 KiB values. Repeat
one scenario five times to validate variance. Reject any invalid trial before
using the campaign for documentation.

- [ ] **Step 2: Run a controlled saturation slice**

Use 3 nodes, 4 KiB values, 50/50 and 100% write, concurrency 1/4/16/64/256,
10-second warmup, 30-second measurement, and five trials where host time permits.
If the full slice is infeasible, retain completed trials and label the reduced
campaign non-publishable; never silently shorten it.

- [ ] **Step 3: Run standalone durability comparisons**

Measure async/sync/group-commit at 4 KiB for concurrency 1/4/16/64 with the same
warmup, measurement, trial count, build, filesystem, and affinity. Confirm scope
labels prevent combination with cluster rows.

- [ ] **Step 4: Inspect bottleneck evidence**

For representative 4 KiB scenarios record leader/follower CPU and bytes,
fdatasync bucket distributions, syncs/write, queue-depth/latency growth,
context-switch rates, error rates, and application payload lower bounds. Use
`perf stat`/`strace -c` only as supporting evidence and record their exact
commands/environment.

- [ ] **Step 5: Write the methodology report**

Document environment, matrix, warmup/trials/variance, comparability rules,
sustainable knee rather than maximum throughput, leader capacity arithmetic,
copy/allocation path, lock/queue observations, limitations, and reproduction.
The committed CSV contains aggregate values plus the environment fingerprint;
raw bulky artifacts remain ignored.

- [ ] **Step 6: Validate docs, commit, and push**

Run a link/path check, parse the CSV, compare every stated number to raw
evidence, and run `git diff --check`.

```bash
git add docs/performance-methodology.md docs/performance/phase16-summary.csv README.md docs/superpowers/plans/2026-09-11-phase-16-performance-methodology.md
git commit -m "docs: publish phase 16 methodology evidence"
git push origin main
```

### Task 7: Full verification and independent review

**Files:**

- Modify: `docs/superpowers/plans/2026-09-11-phase-16-performance-methodology.md`

**Interfaces:**

- Consumes: all Phase 16 code, tests, docs, and retained evidence.
- Produces: final verified Phase 16 commit with explicit gate results.

- [ ] **Step 1: Run all normal suites**

Run all unit, integration, failure, CLI, simulator, benchmark parser, and real
cluster tests in debug and release. Root-only Phase 15 tests may remain skipped
in these non-root suites because their latest privileged evidence is already
recorded.

- [ ] **Step 2: Run sanitizer suites serially**

Run `./scripts/test-asan.sh`, `./scripts/test-ubsan.sh`, and
`./scripts/test-tsan.sh` one at a time to avoid the resource-contention timing
artifact recorded in Phase 15.

- [ ] **Step 3: Repeat benchmark lifecycle tests**

Run the 1/3/5-node smoke campaign three times, SIGTERM during warmup and
measurement, and forced server death. Verify no benchmark/server PIDs, sockets,
or non-temporary artifacts remain and every failure retains invalid evidence.

- [ ] **Step 4: Request two independent reviews**

One reviewer checks requirements/methodology and one checks correctness,
resource bounds, process ownership, parsing, and evidence integrity. Fix all
justified P0/P1 and evidence-backed P2 findings test-first, then rerun affected
and full gates.

- [ ] **Step 5: Record exact results and close Phase 16**

Update this section with test counts, sanitizer results, campaign path/hash,
review verdicts, measured limitations, and any deliberately deferred work. Run
`git diff --check` and verify no generated raw evidence is staged.

- [ ] **Step 6: Commit, push, and verify the remote**

```bash
git add docs/superpowers/plans/2026-09-11-phase-16-performance-methodology.md
git commit -m "docs: complete phase 16 verification"
git push origin main
git status --short --branch
git rev-parse HEAD
git rev-parse origin/main
```

Expected: clean `main`, and local/remote commit IDs are identical.
