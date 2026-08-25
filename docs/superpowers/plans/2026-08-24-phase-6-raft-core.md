# Phase 6 Deterministic Raft Core Implementation Plan

**Goal:** Implement a network-independent Raft state machine with deterministic
logical time, typed actions, safe elections, replication, repair, commitment,
and application.

**Spec:** `docs/superpowers/specs/2026-08-24-raft-core-design.md`

### Task 1: Define the pure Raft surface

**Files:** Create `src/raft/types.h`, `src/raft/raft_node.h`; modify
`src/CMakeLists.txt`, `tests/CMakeLists.txt`; create `tests/unit/raft_node_test.cpp`.

- [x] Add strongly typed roles, log entries, RPCs, messages, and output actions.
- [x] Validate fixed membership and timing configuration.
- [x] Add tests proving invalid configuration is rejected and time is logical
  and monotonic.

### Task 2: Implement election transitions test-first

**Files:** Create `src/raft/raft_node.cpp`; modify the Raft unit test.

- [x] Test timeout-to-candidate action ordering and randomized deadline bounds.
- [x] Test vote freshness, duplicate votes, split vote, and re-election.
- [x] Test stale-term rejection and higher-term step-down from every role.
- [x] Test leader initialization and current-term no-op replication.

### Task 3: Implement replication and log repair test-first

- [x] Test heartbeat, successful append, duplicate append, and follower commit.
- [x] Test missing/mismatched previous entries, conflict hints, suffix truncation,
  and repair.
- [x] Test delayed/reordered responses cannot move peer progress backward.
- [x] Test an old leader rejoins and is repaired by the new leader.

### Task 4: Implement safe commit and application test-first

- [x] Test a majority commits current-term entries and emits gap-free apply
  actions.
- [x] Test a minority cannot commit.
- [x] Test previous-term entries are not directly quorum-committed but become
  committed through a current-term entry.
- [x] Assert state invariants after every public input.

### Task 5: Document, verify, and publish

**Files:** Create `docs/raft.md`; update `docs/invariants.md` only if clarification
is required.

- [x] Document messages, action ordering, timer behavior, commitment, and Phase
  6 persistence limitations.
- [x] Run focused tests, complete debug/release suites, ASan, UBSan, and TSan.
- [x] Run `git diff --check` and prepare the verified Phase 6 files for
  publication on `main`.
