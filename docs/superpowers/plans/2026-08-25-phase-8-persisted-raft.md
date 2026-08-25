# Phase 8 Persisted Raft Implementation Plan

**Goal:** Make Raft term, vote, and log changes crash-safe and enforce durable
ordering before dependent peer messages.

**Spec:** `docs/superpowers/specs/2026-08-25-persisted-raft-design.md`

### Task 1: Specify and test durable byte formats

**Files:** Create `src/raft/raft_storage.h`, `src/raft/raft_storage.cpp`, and
`tests/unit/raft_storage_test.cpp`.

- [x] Write red tests for hard-state generations, log suffix replay, incomplete
  tails, checksums, node identity, and bounds.
- [x] Implement explicit big-endian, versioned, checksummed formats.
- [x] Implement write-then-sync APIs and deterministic recovery.

### Task 2: Gate Raft output on durable persistence

**Files:** Create `src/raft/persisted_raft_node.h`,
`src/raft/persisted_raft_node.cpp`, and unit tests.

- [x] Prove vote grants, successful appends, and leader replication are emitted
  only after the required sync.
- [x] Buffer non-persistence actions until every persistent action succeeds.
- [x] Fault the driver after storage failure and emit no dependent output.

### Task 3: Kill processes at every ordering boundary

**Files:** Create a crash helper and `tests/failure/raft_persistence_crash_test.cpp`;
modify test CMake.

- [x] Crash before persist, after write, after sync, before response, and after
  response for vote and log transitions.
- [x] Restart from the same directory and verify election/log safety.
- [x] Prove any recorded outbound acknowledgment has durable backing.

### Task 4: Document and verify

**Files:** Create `docs/raft-persistence.md`; update README and CMake.

- [x] Document exact mutation/write/sync/response ordering for every persistent
  Raft operation and the strict durability boundary.
- [x] Run focused tests, full debug/release suites, ASan, UBSan, and TSan.
- [x] Review the diff, run `git diff --check`, commit Phase 8, and push `main`.
