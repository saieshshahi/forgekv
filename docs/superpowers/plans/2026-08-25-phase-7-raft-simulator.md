# Phase 7 Deterministic Raft Simulator Implementation Plan

**Goal:** Build a reproducible in-process Raft cluster scheduler that injects
time and communication faults and checks safety continuously.

**Spec:** `docs/superpowers/specs/2026-08-25-raft-simulator-design.md`

### Task 1: Add restartable abstract persistent state

**Files:** Modify `src/raft/types.h`, `src/raft/raft_node.h`,
`src/raft/raft_node.cpp`, `tests/unit/raft_node_test.cpp`.

- [x] Add a validated `RaftPersistentState` constructor path.
- [x] Restore term, vote, and gap-free log at a supplied logical restart time.
- [x] Test restart deadlines, retained vote/log, and volatile commit reset.

### Task 2: Build explicit deterministic controls

**Files:** Create `src/sim/raft_simulator.h`, `src/sim/raft_simulator.cpp`,
`tests/unit/raft_simulator_test.cpp`; modify CMake files.

- [x] Control logical time, selected delivery, ordering, delay, and drop.
- [x] Control links, node partitions, crashes, and abstract-durable restarts.
- [x] Bound pending messages and retained trace.
- [x] Prove identical seeds and operations produce identical traces.

### Task 3: Check safety after every transition

- [x] Track historical leaders and immutable committed entries.
- [x] Check log matching, commit agreement, apply bounds/prefix agreement, and
  committed-entry retention.
- [x] Produce a self-contained failure dump with seed, trace, messages, node
  state, logs, terms, commit indexes, and persistent state.

### Task 4: Add randomized histories and CLI

**Files:** Create `src/sim/raft_sim_main.cpp`; modify `src/CMakeLists.txt`; create
`docs/raft-simulator.md`.

- [x] Add seeded randomized time, delivery, loss, delay, partition, crash,
  restart, and proposal operations.
- [x] Run at least 50,000 operations across many seeds in the test suite.
- [x] Add `forgekv_raft_sim --seed N --steps N` with replayable failure output.

### Task 5: Verify and publish

- [x] Run focused deterministic and randomized tests, debug/release suites,
  ASan, UBSan, and TSan.
- [x] Run `git diff --check`, commit Phase 7, and push `main`.
