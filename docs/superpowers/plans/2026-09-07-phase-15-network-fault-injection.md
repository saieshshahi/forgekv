# Phase 15: Network Fault Injection Implementation Plan

> Execute each task test-first and keep the Phase 15 design document as the
> semantic source of truth.

**Goal:** Add a safe, kernel-backed netem experiment runner and produce real
ForgeKV evidence for the required latency and packet-loss matrix.

**Architecture:** The existing chaos harness gains a stable-workload switch.
A new C++ netem runner creates a uniquely named disposable Linux network
namespace, applies validated qdiscs only to its loopback device through direct
`fork`/`exec` argument vectors, launches the real chaos workload in that
namespace, records bounded results, and unconditionally removes the namespace.
Phase 14 proxy partitions/resets remain separate and retain deterministic replay.

**Technology:** C++23, Linux network namespaces, `iproute2` (`ip`, `tc netem`),
CMake/CTest, GoogleTest, JSONL/Markdown evidence.

---

## Task 1: Stable workload mode

**Files:**

- Modify: `chaos/harness.h`
- Modify: `chaos/harness.cpp`
- Modify: `chaos/main.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/integration/chaos_harness_test.cpp`

1. Add a failing integration test that runs a short 3-node campaign with action
   scheduling disabled and asserts zero actions, convergence, durable restart,
   and successful writes.
2. Add `HarnessOptions::enable_chaos` and the strict, duplicate-safe CLI flag
   `--no-chaos`.
3. Make the run loop generate no scheduled actions in stable mode while keeping
   clients, deadlines, child-exit checks, artifact bounds, convergence, log
   scans, full restart, and final state verification active.
4. Persist the mode in campaign config/summary and reject replay combinations
   whose metadata disagrees.
5. Run focused harness/CLI tests and commit the task.

## Task 2: Netem domain model and validation

**Files:**

- Create: `chaos/netem.h`
- Create: `chaos/netem.cpp`
- Create: `tests/unit/netem_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

1. Add failing tests for the seven required profiles, 0.1% basis-point
   precision, jitter/reorder smoke profiles, stable semantic labels, and every
   numeric bound.
2. Define `NetemProfile`, `NetemFaultKind`, and validation results with no
   privileged behavior.
3. Generate a fixed-token `tc qdisc replace dev lo root netem ...` argument
   vector. Never generate a shell command string.
4. Add bounded parsers for the one-line chaos result and `tc -s qdisc` counters.
5. Run focused unit tests and commit the task.

## Task 3: Safe namespace lifecycle

**Files:**

- Create: `chaos/process_runner.h`
- Create: `chaos/process_runner.cpp`
- Create: `chaos/netem_runner.h`
- Create: `chaos/netem_runner.cpp`
- Create: `tests/unit/netem_runner_test.cpp`
- Modify: `chaos/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

1. Add failing tests around a fake command executor for namespace-name
   validation, exact command ordering, refusal of non-root/default-interface
   targets, and cleanup after create/apply/workload failures.
2. Implement bounded direct process execution with explicit argv, timeout,
   signal forwarding, combined-output cap, and exit-status reporting.
3. Implement an RAII namespace guard. The only accepted namespace prefix is
   `fkv-netem-`; the only qdisc interface is `lo`; delete only after successful
   ownership-creating `ip netns add`.
4. Treat `ip netns del` failure as a surfaced reliability error while still
   retaining experiment evidence.
5. Run unit tests plus ASan/UBSan focused tests and commit the task.

## Task 4: Experiment CLI and bounded evidence

**Files:**

- Create: `chaos/netem_main.cpp`
- Modify: `chaos/CMakeLists.txt`
- Create: `tests/integration/netem_runner_test.cpp`
- Modify: `tests/CMakeLists.txt`

1. Add CLI tests for help, invalid/duplicate options, unsafe paths, missing
   tools, non-root execution, selected profiles, and output bounds.
2. Add `forgekv-netem` with strict options for server path, chaos path, output,
   nodes, clients, duration, seed, and repeatable known profile names.
3. For each profile, use a fresh namespace and artifact directory; enable
   loopback, apply qdisc when non-baseline, execute
   `forgekv-chaos --no-chaos --keep-success`, collect qdisc counters, and clean
   up regardless of the workload result.
4. Atomically write bounded `environment.txt`, `results.jsonl`, and `summary.md`.
   Record an unsuccessful profile before returning nonzero.
5. Add a privileged disposable-namespace smoke test that explicitly skips when
   effective UID/capability/tool support is absent.
6. Run focused integration and CLI tests and commit the task.

## Task 5: Documentation and semantic cleanup

**Files:**

- Modify: `docs/chaos-testing.md`
- Create: `docs/network-fault-injection.md`
- Modify: `README.md`
- Modify: `chaos/types.h`
- Modify: `chaos/artifacts.cpp`

1. Add tests for compatibility parsing and user-facing serialization of the
   historical proxy `set_loss` action.
2. Preserve its serialized spelling for old timelines, but label it in docs and
   diagnostics as a seeded connection reset/chunk-drop mechanism.
3. Document partition, connection reset, and packet impairment side by side,
   including TCP retransmission and non-deterministic kernel timing.
4. Document the safe root invocation for WSL/Linux, the output schema, cleanup,
   supported profiles, and known limits.
5. Run documentation/link checks and `git diff --check`; commit the task.

## Task 6: Required real-kernel campaign

**Files:**

- Generate evidence under a newly created private root-owned directory such as
  `/root/forgekv-phase15-netem/`
- Update: `docs/network-fault-injection.md`

1. Build the release runner and server.
2. As root, run the seven-profile 3-node matrix with concurrent clients. Use a
   fixed seed and retain every campaign artifact.
3. Confirm every namespace is removed and the default namespace has no qdisc
   created by ForgeKV.
4. Inspect result records and qdisc counters. Record attempts/second,
   acknowledged mutations/second, convergence, durable restart, observed drops,
   and diagnostics without interpreting the quick run as a statistically sound
   benchmark.
5. If a profile fails, preserve evidence, diagnose, add the smallest regression
   test, fix, and rerun the entire matrix.
6. Commit the measured documentation update.

## Task 7: Full verification and review

1. Run all unit, integration, failure, CLI, and simulation tests in debug and
   release builds.
2. Run full ASan, UBSan, and TSan suites.
3. Run repeated namespace lifecycle and TERM cleanup tests; verify no
   `fkv-netem-` namespace remains.
4. Run `git diff --check` and inspect the final Phase 15 diff for unrelated
   changes or generated artifacts.
5. Request two independent source reviews: one for correctness/safety and one
   against the Phase 15 requirements. Fix all justified P0/P1 findings and
   evidence-backed P2 issues, then rerun affected and full gates.
6. Mark this plan complete with exact evidence, commit, push `main`, and verify
   `HEAD == origin/main` with a clean worktree.

## Execution evidence

- Tasks 1–6 are complete. Stable mode, profile validation, bounded direct
  process execution, namespace ownership/cleanup, the experiment CLI, atomic
  evidence, semantic documentation, and real-kernel regressions are implemented.
- The required release matrix passed all seven profiles with 3 nodes, 8 clients,
  three traffic seconds, and seed 150015. Netem observed 20, 79, and 189 drops at
  0.1%, 1%, and 5%; every profile converged and verified a complete restart.
- Real jitter and reordering smoke profiles passed. Their qdisc descriptions were
  `delay 10ms 5ms` and `delay 10ms reorder 1% 25%`.
- The first matrix exposed proxy-amplified latency at 100 ms; stable kernel tests
  now bypass Phase 14 proxies and use a bounded profile-derived request timeout.
  The second exposed a one-shot metrics scrape under 5% loss; evidence collection
  now retries within the existing overall deadline. Both endpoints are permanent
  privileged regressions.
- Task 7 remains in progress until all debug/release/sanitizer gates and two
  independent reviews are green.
