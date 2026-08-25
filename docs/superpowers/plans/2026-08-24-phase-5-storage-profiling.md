# Phase 5 Storage Profiling Implementation Plan

**Goal:** Produce reproducible evidence about ForgeKV storage costs and keep or
reject candidate optimizations based on measured results.

**Spec:** `docs/superpowers/specs/2026-08-24-storage-profiling-design.md`

### Task 1: Establish profiling prerequisites and immutable baseline

**Files:** Modify `.gitignore`; create `scripts/profile-storage.sh`.

- [x] Install or locate `perf`, Valgrind, `strace`, and FlameGraph scripts in the
  WSL environment without vendoring third-party tools.
- [x] Record compiler, kernel, CPU, filesystem, and profiling-permission details.
- [x] Run the existing complete release benchmark matrix and retain the Phase 4
  baseline as the comparison point.
- [x] Add a script that selects named existing benchmarks, uses repeated runs,
  writes raw captures only under `build/profile-storage`, and fails clearly when
  a profiler is unavailable.

### Task 2: Capture CPU, syscall, allocation, cache, and lock evidence

**Files:** Create `docs/performance/flamegraphs/*.svg` and ignored raw outputs.

- [x] Capture `perf stat` and `perf record` for ASYNC PUT, SYNC PUT,
  GROUP_COMMIT, GET, and recovery.
- [x] Generate flamegraphs from the call-stack captures.
- [x] Capture `strace -f -c` syscall counts and time for the same write modes.
- [x] Capture allocation/heap evidence with Massif and cache evidence with perf
  counters; use futex/context-switch evidence for lock contention.
- [x] Identify costs for checksum, serialization, copies, hash-table access,
  allocation, write, sync, and synchronization without proposing changes first.

### Task 3: Evaluate candidates scientifically

**Files:** Create `docs/performance-storage.md`; optionally modify storage code
and add focused tests only after a hypothesis clears the evidence gate.

- [x] For each requested candidate, record baseline, hypothesis, proposed
  experiment, measurement, result, and regression risk.
- [x] Reject mmap, direct I/O, alignment, pooling, and other complexity when the
  evidence does not justify them.
- [x] If one bottleneck exceeds the threshold, write a behavior test,
  make one minimal optimization, rerun the identical benchmark, and keep it only
  when it meets the acceptance threshold.
- [x] Preserve the simplest correct implementation if no change clears the gate.

### Task 4: Verify and publish

- [x] Run the profiling script from a clean release build and verify committed
  SVG artifacts render.
- [x] Run all normal and sanitizer suites if production code changed; otherwise
  run the full debug/release tests and benchmark smoke.
- [x] Run `git diff --check` and prepare the verified Phase 5 artifacts for
  publication on `main`.
