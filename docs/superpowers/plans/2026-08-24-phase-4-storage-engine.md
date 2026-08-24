# Phase 4 Standalone Storage Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a correct standalone in-memory key-value engine backed by a checksummed append-only WAL with deterministic crash recovery and three explicit durability modes.

**Architecture:** One writer thread assigns LSNs, writes WAL records, applies mutations in LSN order, and fulfills callers. SYNC flushes each operation; GROUP_COMMIT drains bounded batches behind one flush; ASYNC acknowledges after complete write syscalls. Concurrent GETs use a shared lock over the in-memory map.

**Tech Stack:** C++20, POSIX file I/O, GoogleTest, Google Benchmark.

**Spec:** `docs/architecture.md`, `docs/failure-model.md`, `docs/adr/0003-storage-model.md`, and `docs/adr/0004-durability-semantics.md`.

## Global Constraints

- Key maximum 1 KiB; value maximum 1 MiB.
- WAL records contain magic/version, header/record length, LSN, operation, key/value lengths, payload, and CRC-32.
- A partial record is never interpreted as valid.
- Incomplete EOF tails are truncated; complete-record checksum or structural corruption fails closed.
- No Raft code is added in this phase.

---

### Task 1: WAL record codec

**Files:** Create `src/storage/wal_record.h`, `src/storage/wal_record.cpp`, `tests/unit/wal_record_test.cpp`; modify CMake.

**Interfaces:** `WalRecord{lsn, operation, key, value}`, `encode_record`, `decode_record`, and `DecodeStatus{complete,incomplete,corrupt}`.

- [ ] Write golden-byte tests for PUT/DELETE and rejection tests for lengths, enums, overflow, checksum, and truncation.
- [ ] Verify RED, implement fixed-width little-independent encoding using explicit byte order, and pass tests.

### Task 2: WAL append and recovery

**Files:** Create `src/storage/wal.h`, `src/storage/wal.cpp`, `tests/unit/wal_test.cpp`, `tests/failure/wal_recovery_test.cpp`.

**Interfaces:** `Wal::open(path)`, `append(record)`, `sync()`, `recover(callback)`, `durable_lsn()`, and typed storage errors.

- [ ] Write tests for empty WAL, normal replay, reopen replay, monotonic LSNs, duplicate/out-of-order LSN rejection, and binary keys/values.
- [ ] Verify RED, implement EINTR-safe partial write/read loops and explicit `fdatasync`.
- [ ] Add deterministic tests for every truncation offset in header/payload/checksum and implement final-tail truncation.
- [ ] Add checksum/structural corruption tests and implement fail-closed recovery.

### Task 3: KV engine and durability modes

**Files:** Create `src/storage/kv_store.h`, `src/storage/kv_store.cpp`, `tests/unit/kv_store_test.cpp`, `tests/integration/storage_recovery_test.cpp`.

**Interfaces:** `DurabilityMode{async,sync,group_commit}`, `StorageOptions`, `KvStore::open`, `put`, `get`, `erase`, `close`, and `last_lsn`.

- [ ] Write tests for overwrite, missing GET/DELETE, bounds, ordered persistence, restart recovery, and idempotent close; verify RED.
- [ ] Implement the single writer and shared-read state machine for ASYNC and SYNC.
- [ ] Write concurrent batching tests using an injectable flush observer; verify RED.
- [ ] Implement GROUP_COMMIT knobs `max_batch_entries`, `max_batch_bytes`, and `max_batch_wait_us`, with one flush and ordered application per batch.
- [ ] Run concurrency tests under TSan and recovery tests under ASan/UBSan.

### Task 4: Failure harness, benchmarks, and documentation

**Files:** Create `tests/failure/storage_crash_helper.cpp`, `tests/failure/storage_crash_test.cpp`, `bench/micro/storage_benchmark.cpp`, `docs/storage.md`; modify CMake and README.

- [ ] Add subprocess kill tests around append and sync boundaries where deterministic hooks are possible; verify only fully valid records recover.
- [ ] Benchmark PUT/GET/DELETE and recovery for 100 B, 1 KiB, 4 KiB, 64 KiB, and 1 MiB values over varying key counts.
- [ ] Report ops/sec, bytes/sec, p50, p95, p99, maximum latency, and process CPU utilization as benchmark counters.
- [ ] Document record bytes, recovery policy, all durability modes, batching knobs, and the throughput/latency tradeoff.
- [ ] Run clean full builds, all tests and sanitizers, benchmark smoke, `git diff --check`, commit Phase 4, and push `main`.
