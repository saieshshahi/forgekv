# Phase 14 Chaos Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a seeded, replayable multi-process ForgeKV chaos harness that validates acknowledged state after real process and network faults and preserves diagnostic artifacts.

**Architecture:** A native C++ `forgekv-chaos` process owns server children, concurrent clients, and bounded directed TCP fault proxies. Small core types isolate deterministic scheduling, history/artifact persistence, process lifecycle, client semantics, and convergence verification so each can be tested before the end-to-end harness is assembled.

**Tech Stack:** C++20, POSIX processes/signals/sockets/poll, ForgeKV binary protocol and cluster admin endpoints, CMake/Ninja, GoogleTest, ASan/UBSan/TSan.

---

## File structure

- Create `chaos/CMakeLists.txt`: build `forgekv_chaos_core` and `forgekv-chaos`.
- Create `chaos/types.h`: bounded options, action, history, response, node, and summary types.
- Create `chaos/scheduler.h` and `chaos/scheduler.cpp`: stable seeded action generation and replay selection.
- Create `chaos/artifacts.h` and `chaos/artifacts.cpp`: JSONL appenders, atomic summaries, replay parsing, and JSON escaping.
- Create `chaos/fault_proxy.h` and `chaos/fault_proxy.cpp`: joinable directed TCP proxy with bounded delayed chunks and link policies.
- Create `chaos/process_cluster.h` and `chaos/process_cluster.cpp`: port allocation, server argument construction, fork/exec, signals, reaping, and bounded stop.
- Create `chaos/client_worker.h` and `chaos/client_worker.cpp`: protocol requests, redirects, same-ID retries, per-key expected state, and attempt history.
- Create `chaos/verifier.h` and `chaos/verifier.cpp`: admin metric parsing, cluster convergence, durable restart, log scan, and final key verification.
- Create `chaos/harness.h` and `chaos/harness.cpp`: lifecycle orchestration, action application, cleanup, and run result.
- Create `chaos/main.cpp`: strict command-line parsing and exit codes.
- Create `tests/unit/chaos_scheduler_test.cpp`, `tests/unit/chaos_artifacts_test.cpp`, `tests/unit/chaos_client_state_test.cpp`, and `tests/unit/chaos_verifier_test.cpp`: deterministic pure-component tests.
- Create `tests/integration/fault_proxy_test.cpp` and `tests/integration/chaos_harness_test.cpp`: real socket/process coverage.
- Create `docs/chaos-testing.md`: operator contract, examples, guarantees, limits, and replay instructions.
- Modify `CMakeLists.txt`, `tests/CMakeLists.txt`, and `README.md`: build, register, and document the harness.

### Task 1: Bounded chaos types and deterministic scheduler

**Files:**
- Create: `chaos/types.h`
- Create: `chaos/scheduler.h`
- Create: `chaos/scheduler.cpp`
- Create: `tests/unit/chaos_scheduler_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing scheduler tests**

Define tests that require the same seed and cluster state to produce identical
actions, different seeds to diverge, inapplicable actions to become explicit
no-ops, and all parameters to remain within configured limits:

```cpp
TEST(ChaosSchedulerTest, SameSeedProducesStableBoundedTimeline) {
  ChaosScheduler left(12345, SchedulerLimits{});
  ChaosScheduler right(12345, SchedulerLimits{});
  ClusterView view = five_live_nodes_with_leader(2);
  for (std::uint64_t step = 0; step < 500; ++step) {
    EXPECT_EQ(left.next(step * 100'000, view),
              right.next(step * 100'000, view));
  }
}

TEST(ChaosSchedulerTest, RestartWithoutDeadNodeBecomesRecordedNoOp) {
  ChaosScheduler scheduler(7, SchedulerLimits{});
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::restart_node, .node = 2},
                three_live_nodes_with_leader(1))
                .kind,
            ActionKind::no_op);
}
```

- [ ] **Step 2: Run the new test and confirm it fails to compile**

Run:

```bash
cmake --build build/debug --target forgekv_unit_tests -j2
```

Expected: failure because `chaos/scheduler.h` and its types do not exist.

- [ ] **Step 3: Implement the minimal stable scheduler**

Use fixed-width fields and an explicitly implemented xorshift64* generator,
not `std::uniform_*`, so library changes cannot alter the sequence:

```cpp
enum class ActionKind : std::uint8_t {
  no_op, kill_leader, kill_follower, restart_node, partition_node,
  partition_leader_majority, set_latency, set_jitter, set_loss,
  heal_network, pause_node, resume_node, rapid_leader_churn
};

struct ChaosAction final {
  ActionKind kind{ActionKind::no_op};
  std::uint64_t planned_offset_us{};
  std::uint64_t node{};
  std::uint64_t peer{};
  std::uint32_t value{};
  bool operator==(const ChaosAction&) const = default;
};
```

Validate `nodes` as 3 or 5, clients in `[1, 256]`, duration in `[1, 3600]`,
and action intervals in `[50, 60'000]` milliseconds. `normalize()` must never
select a dead leader, restart a live node, pause a dead node, or resume a node
that is not paused.

- [ ] **Step 4: Run scheduler tests**

Run:

```bash
build/debug/tests/forgekv_unit_tests --gtest_filter='ChaosSchedulerTest.*'
```

Expected: all scheduler tests pass with a stable golden prefix for seed 12345.

### Task 2: Crash-useful artifact writer and timeline replay

**Files:**
- Create: `chaos/artifacts.h`
- Create: `chaos/artifacts.cpp`
- Create: `tests/unit/chaos_artifacts_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing artifact tests**

Require JSON escaping, append-only operation/timeline records, strict replay
parsing, and atomic final files:

```cpp
TEST(ChaosArtifactsTest, EscapesUntrustedFieldsAndRoundTripsTimeline) {
  TemporaryDirectory directory;
  ArtifactWriter writer(directory.path(), ArtifactLimits{});
  const ChaosAction action{.kind = ActionKind::set_latency,
                           .planned_offset_us = 250'000,
                           .node = 1, .peer = 3, .value = 50};
  ASSERT_TRUE(writer.append_action(action, 251'000, 252'000).ok());
  const auto replay = read_timeline(directory.path() / "timeline.jsonl");
  ASSERT_TRUE(replay.ok());
  EXPECT_EQ(replay.actions, std::vector<ChaosAction>{action});
}

TEST(ChaosArtifactsTest, RejectsTruncatedUnknownAndOversizedReplay) {
  EXPECT_FALSE(read_timeline_fixture("{\"kind\":\"kill_leader\"").ok());
  EXPECT_FALSE(read_timeline_fixture("{\"kind\":\"invented\"}\n").ok());
}
```

- [ ] **Step 2: Run the tests and verify the missing API failure**

Run the unit target and expect compilation to fail on `ArtifactWriter`.

- [ ] **Step 3: Implement bounded append and atomic publication**

Open JSONL files with `O_APPEND | O_CREAT | O_CLOEXEC`. Count records and bytes
before every write. Encode one record into a bounded local string and use a
complete-write loop. Publish `config.json`, `summary.json`, and `replay.txt` by
writing `<name>.tmp`, calling `fdatasync`, renaming, and syncing the artifact
directory. The replay parser accepts only fields emitted by this version and
rejects duplicate fields, overflow, truncation, and trailing content.

- [ ] **Step 4: Run artifact tests and inspect a golden artifact directory**

Expected: tests pass; every JSONL line is independently parseable and
`replay.txt` contains `forgekv-chaos --replay=<absolute timeline path>` plus the
recorded cluster/client options.

### Task 3: Directed bounded fault proxy

**Files:**
- Create: `chaos/fault_proxy.h`
- Create: `chaos/fault_proxy.cpp`
- Create: `tests/integration/fault_proxy_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing real-socket proxy tests**

Use a loopback echo server behind one proxy and verify healthy forwarding,
prompt partition close, fixed delay bounds, stable seeded loss decisions,
bounded queued bytes, idempotent stop, and thread join:

```cpp
TEST(FaultProxyTest, PartitionClosesExistingStreamAndHealReconnects) {
  EchoServer destination;
  FaultProxy proxy(proxy_config(destination.port()));
  ASSERT_TRUE(proxy.start().ok());
  auto client = connect_to(proxy.port());
  EXPECT_EQ(round_trip(client, "before"), "before");
  proxy.set_policy(LinkPolicy{.partitioned = true});
  EXPECT_TRUE(peer_closed(client));
  proxy.set_policy(LinkPolicy{});
  EXPECT_EQ(round_trip(connect_to(proxy.port()), "after"), "after");
  proxy.stop();
  proxy.stop();
}
```

- [ ] **Step 2: Run the integration target and confirm the proxy API is missing**

Expected: compilation fails before any production code is added.

- [ ] **Step 3: Implement one joinable poll-based proxy owner**

The proxy owns listener, wake descriptor, accepted upstream, downstream, and a
min-heap of delayed chunks. Cap active streams at one and queued bytes at 4 MiB
per direction. Policy is copied under a short mutex; descriptors and queues are
owned only by the proxy thread. A dropped chunk closes both ends. Partition
changes wake the owner and close both ends. Stop wakes, joins, then closes all
descriptors; no thread is detached.

- [ ] **Step 4: Run the proxy tests repeatedly**

Run:

```bash
build/debug/tests/forgekv_integration_tests \
  --gtest_filter='FaultProxyTest.*' --gtest_repeat=20 --gtest_break_on_failure
```

Expected: every iteration passes without leaked descriptors.

### Task 4: Server child lifecycle and topology construction

**Files:**
- Create: `chaos/process_cluster.h`
- Create: `chaos/process_cluster.cpp`
- Create: `tests/unit/chaos_process_cluster_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing lifecycle tests**

Require unique ports, source-specific proxy peer arguments, start/kill/restart,
STOP/CONT, nonblocking status refresh, TERM with KILL fallback, and destructor
cleanup:

```cpp
TEST(ProcessClusterTest, BuildsDirectedProxyTopologyForEverySource) {
  ProcessCluster cluster(test_config(3));
  ASSERT_TRUE(cluster.prepare().ok());
  EXPECT_NE(cluster.proxy_port(1, 2), cluster.proxy_port(3, 2));
  EXPECT_TRUE(cluster.server_arguments(1).contains(
      "--peer 2=127.0.0.1:" + std::to_string(cluster.proxy_port(1, 2))));
}
```

- [ ] **Step 2: Verify tests fail for the missing lifecycle component**

Build the unit target; expected failure names `ProcessCluster`.

- [ ] **Step 3: Implement strict ownership and signal methods**

Adapt the proven `ServerProcess` logic from
`tests/integration/real_cluster_test.cpp`, but keep PID state in one owner and
surface `NodeState {dead, running, paused}`. Reserve all ports before fork,
create data/log directories, construct each source node's peer list with its
directed proxy ports, redirect stdout/stderr to `logs/node-N.log`, and exec the
configured absolute server path. All wait loops use deadlines and handle
`EINTR`; children are always reaped.

- [ ] **Step 4: Run lifecycle tests including repeated destruction**

Expected: 50 repeated prepare/start/stop cycles leave no live child PIDs.

### Task 5: Client protocol, same-ID retry, and per-key state machine

**Files:**
- Create: `chaos/client_worker.h`
- Create: `chaos/client_worker.cpp`
- Create: `tests/unit/chaos_client_state_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing state and response tests**

Cover request encoding, redirect endpoint validation, timeout ambiguity,
same-ID retry, definitive duplicate results, GET validation, and final expected
state:

```cpp
TEST(ChaosClientStateTest, TimeoutRetriesSameIdBeforeNextOperation) {
  ClientState state(client_id(7), "client-7");
  const auto first = state.begin_put("v1");
  state.observe(first, AttemptResult::timeout());
  EXPECT_EQ(state.next_attempt().request_id, first.request_id);
  state.observe(first, AttemptResult::ok());
  EXPECT_EQ(state.expected_value(), "v1");
  EXPECT_GT(state.begin_get().request_id, first.request_id);
}
```

- [ ] **Step 2: Confirm missing client state fails compilation**

- [ ] **Step 3: Implement the pure state model, then socket execution**

Encode client IDs and lengths exactly as `cluster/codecs.cpp` expects. Use one
request per connection initially for simple failure boundaries. Apply 1-second
send/receive timeouts, validate response namespace/request ID, follow only
well-formed loopback redirects, and classify OK/not-found/error as definitive;
transport failure and timeout are ambiguous. Append every physical attempt to
the artifact sink before deciding the next attempt.

- [ ] **Step 4: Run state tests and a fake-endpoint retry test**

Expected: a dropped first response produces two attempts with one logical
request ID and exactly one expected-state transition.

### Task 6: Metrics parsing and convergence verifier

**Files:**
- Create: `chaos/verifier.h`
- Create: `chaos/verifier.cpp`
- Create: `tests/unit/chaos_verifier_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing parser and convergence tests**

Provide golden `/health`, `/ready`, and `/metrics` responses. Reject duplicate,
missing, malformed, negative, NaN, and unknown-role samples. Require one ready
leader, all nodes healthy, equal commit/applied indexes, and every expected
leader lag sample equal to zero.

```cpp
TEST(ChaosVerifierTest, ConvergedRequiresOneLeaderAndEqualAppliedIndexes) {
  auto views = converged_views(3, 91);
  EXPECT_TRUE(evaluate_convergence(views).ok());
  views[2].last_applied = 90;
  EXPECT_FALSE(evaluate_convergence(views).ok());
}
```

- [ ] **Step 2: Verify the tests fail on the absent verifier**

- [ ] **Step 3: Implement bounded HTTP fetch and strict metric extraction**

Read at most 4 MiB per response. Parse HTTP status and the exact fixed metric
names emitted by Phase 13. Poll until a configured deadline with 100 ms
intervals. After convergence, issue a linearizable GET for every client-owned
key and compare OK/not-found and bytes to `ClientState`.

- [ ] **Step 4: Run verifier unit tests**

Expected: all malformed metrics fail closed with a diagnostic naming the node
and sample.

### Task 7: Harness orchestration and action application

**Files:**
- Create: `chaos/harness.h`
- Create: `chaos/harness.cpp`
- Create: `tests/integration/chaos_harness_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write failing short end-to-end tests**

Add scripted tests for follower kill/restart, leader-majority partition,
pause/resume plus delay/loss, all-node restart, convergence, exact keys, and
failure artifact retention. Use 3 nodes, 4 clients, a 3-second action window,
and a 30-second overall deadline.

```cpp
TEST(ChaosHarnessTest, ScriptedFailoverRestartsAndVerifiesAcknowledgedState) {
  HarnessOptions options = short_options();
  options.script = {kill_follower_at(300ms), restart_node_at(700ms),
                    partition_leader_at(1100ms), heal_at(1800ms)};
  const auto result = ChaosHarness(options).run();
  ASSERT_TRUE(result.ok()) << result.diagnostic;
  EXPECT_GT(result.summary.acknowledged_writes, 0U);
  EXPECT_TRUE(result.summary.converged);
  EXPECT_TRUE(result.summary.restart_verified);
}
```

- [ ] **Step 2: Run the test and verify the missing orchestrator failure**

- [ ] **Step 3: Implement the bounded run state machine**

Use explicit phases `prepare`, `start`, `warmup`, `chaos`, `resolve`,
`durable_restart`, `converge`, `verify`, `collect`, and `cleanup`. Apply actions
only through `ProcessCluster` and `FaultProxy`; record observed timestamps even
when an action normalizes to no-op. Stop clients before resolve. Heal and resume
first, restart dead nodes, resolve each outstanding logical request, then stop
and restart all nodes once before convergence and key checks.

- [ ] **Step 4: Run scripted integration tests repeatedly**

Expected: 10/10 passes; injected verification failure retains all required
artifacts and reports a usable replay command.

### Task 8: CLI, replay mode, build integration, and operator documentation

**Files:**
- Create: `chaos/main.cpp`
- Create: `docs/chaos-testing.md`
- Modify: `chaos/CMakeLists.txt`
- Modify: `CMakeLists.txt`
- Modify: `README.md`

- [ ] **Step 1: Write failing CLI smoke tests in CMake**

Register `ChaosCliHelp`, `ChaosCliRejectsInvalidBounds`, and a short seeded
smoke run. Invalid values must exit 2; invariant/run failures exit 1; success
exits 0.

- [ ] **Step 2: Implement strict CLI parsing**

Accept `--nodes`, `--clients`, `--duration`, `--seed`, `--server`,
`--artifacts`, `--action-interval-ms`, `--keep-success`, and `--replay` in both
`--name value` and `--name=value` forms. Reject duplicates and incompatible
`--seed`/`--replay`. Print the seed and artifact directory before launching.

- [ ] **Step 3: Document exact usage and claims**

Document every action, bound, artifact, exit code, final invariant, and replay
limitation. Include:

```bash
./build/debug/chaos/forgekv-chaos \
  --nodes 5 --clients 32 --duration 120 --seed 12345 \
  --server ./build/debug/src/forgekv-server \
  --artifacts ./chaos-artifacts
```

State explicitly that seeded decisions and realized timelines are reproducible
but OS scheduling is not, and that finite campaigns do not establish production
readiness.

- [ ] **Step 4: Run CLI and documentation checks**

Expected: help succeeds, invalid bounds fail before creating children, and a
short seeded run prints `result=pass converged=true restart_verified=true`.

### Task 9: Phase 14 verification, review, and checkpoint

**Files:**
- Modify: `docs/superpowers/plans/2026-09-04-phase-14-chaos-harness.md` (check completed steps)

- [ ] **Step 1: Run focused and repeated chaos tests**

```bash
build/debug/tests/forgekv_unit_tests --gtest_filter='Chaos*'
build/debug/tests/forgekv_integration_tests \
  --gtest_filter='FaultProxyTest.*:ChaosHarnessTest.*' \
  --gtest_repeat=10 --gtest_break_on_failure
```

Expected: all pass.

- [ ] **Step 2: Run seeded end-to-end campaigns**

Run at least seeds `1`, `12345`, `0xC0FFEE`, `0xDEADBEEF`, and
`0x9E3779B97F4A7C15`, using 3 nodes/8 clients/10 seconds. Run one 5-node/32
client/30-second campaign. Every campaign must converge, restart, and validate
acknowledged state; preserve summaries as Phase 14 evidence.

- [ ] **Step 3: Run complete build matrix**

```bash
ctest --test-dir build/debug --output-on-failure -j2
ctest --test-dir build/release --output-on-failure -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  ctest --test-dir build/asan --output-on-failure -j1
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build/ubsan --output-on-failure -j1
./scripts/test-tsan.sh
```

Expected: debug, release, ASan, and UBSan pass. TSan passes or its documented
runtime/platform limitation is captured with the exact diagnostic.

- [ ] **Step 4: Request specification and quality review**

Reviewers must trace every Phase 14 requirement to source/tests/artifacts and
rank remaining findings. Fix all P0/P1 and justified measured P2 findings, then
rerun affected and full verification.

- [ ] **Step 5: Commit and push Phase 14**

```bash
git add CMakeLists.txt README.md chaos docs tests
git commit -m "test: add deterministic multi-process chaos harness"
git push origin main
```

Expected: local `HEAD` equals `origin/main`, the worktree is clean, and the
final commit contains implementation, tests, documentation, and checked plan.
