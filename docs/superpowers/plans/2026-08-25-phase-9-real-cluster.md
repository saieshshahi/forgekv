# Phase 9 Real Cluster Implementation Plan

**Goal:** Run a crash-safe multi-process Raft cluster over TCP and return client
mutation success only after durable majority commit and state-machine apply.

**Spec:** `docs/superpowers/specs/2026-08-25-real-cluster-design.md`

### Task 1: Define bounded peer and command codecs

- [ ] Add exact Raft peer envelope and replicated KV command codecs.
- [ ] Test round trips plus malformed identity, lengths, enums, and entries.

### Task 2: Build the node runtime

- [ ] Add one-owner Raft event loop, monotonic timers, bounded input queue, and
  off-owner outbound RPC workers.
- [ ] Apply committed commands in order and correlate client completion with
  the applied log index.
- [ ] Implement leader redirect/retry behavior and local pre-Phase-10 reads.

### Task 3: Add the server executable

- [ ] Parse immutable node, cluster, data-directory, client-port, peer-port,
  and peer endpoint configuration.
- [ ] Start separate client and peer listeners and shut down safely.

### Task 4: Prove the real process lifecycle

- [ ] Spawn three real server processes and discover a leader over client TCP.
- [ ] Verify write/read, leader SIGKILL, reelection, continued writes, old-node
  restart, log catch-up, and consistent applied values.

### Task 5: Document, review, verify, commit, and push

- [ ] Document exact PUT completion and Phase 9 read limitations.
- [ ] Run focused and complete debug/release/ASan/UBSan/TSan suites.
- [ ] Obtain independent review, run `git diff --check`, commit, and push main.

