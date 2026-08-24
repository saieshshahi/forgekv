# Phase 2 Wire Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a bounded, incremental, checksummed binary frame protocol for client and future Raft messages.

**Architecture:** A 24-byte fixed-width, big-endian header precedes an opaque bounded payload. A stateful parser consumes arbitrary byte chunks, validates before allocating payload storage, and emits complete frames without assuming TCP read boundaries.

**Tech Stack:** C++20, GoogleTest, Google Benchmark, optional libFuzzer target with Clang.

**Spec:** `docs/architecture.md`, especially sections 6 and 7.

## Global Constraints

- Header fields: magic, version, namespace, message type, flags, request ID, payload length, CRC-32.
- Maximum payload is 1 MiB plus bounded operation metadata; maximum frame is constant.
- Client operations: PUT, GET, DELETE, PING. Responses: OK, NOT_FOUND, ERROR, REDIRECT, BUSY.
- Raft uses a separate namespace; unknown enum values are rejected.
- No allocation is sized from an unvalidated attacker-controlled length.

---

### Task 1: CRC-32 and frame model

**Files:** Create `src/protocol/checksum.h`, `src/protocol/checksum.cpp`, `src/protocol/frame.h`, `tests/unit/checksum_test.cpp`; modify CMake lists.

**Interfaces:** `std::uint32_t crc32(std::span<const std::byte>)`; `FrameHeader`, `Frame`, `Namespace`, `MessageType`, and protocol limit constants.

- [ ] Write known-vector and empty-vector checksum tests and verify they fail to link.
- [ ] Implement the table-driven IEEE CRC-32 function and pass targeted tests.
- [ ] Add enum and limit compile-time assertions.

### Task 2: Fixed-width serializer

**Files:** Create `src/protocol/serializer.h`, `src/protocol/serializer.cpp`, `tests/unit/serializer_test.cpp`.

**Interfaces:** `SerializeResult serialize(const Frame&)`; errors distinguish invalid enum, unsupported flags, payload too large, and invalid message/namespace pairing.

- [ ] Write tests for exact golden bytes, zero payload, maximum payload, request ID byte order, enum validation, and checksum.
- [ ] Verify RED, implement bounded big-endian serialization, then verify GREEN.
- [ ] Refactor header encode helpers without changing golden bytes.

### Task 3: Incremental parser

**Files:** Create `src/protocol/parser.h`, `src/protocol/parser.cpp`, `tests/unit/parser_test.cpp`.

**Interfaces:** `Parser::consume(std::span<const std::byte>) -> ParseBatch`; batch contains frames, consumed-byte-independent parser state, and a terminal `ParseError` when invalid.

- [ ] Write tests for one-byte chunks, randomized fragmentation, concatenated frames, zero/maximum payload, truncated state, malformed magic/version/namespace/type/length, integer boundaries, and corrupted checksum.
- [ ] Verify the parser tests fail because the API is absent.
- [ ] Implement a header-first state machine with a fixed 24-byte header buffer and payload allocation only after validation.
- [ ] Pass targeted tests under normal, ASan, and UBSan builds.

### Task 4: Protocol documentation, fuzzing, and benchmarks

**Files:** Create `docs/protocol.md`, `fuzz/protocol_parser_fuzz.cpp`, `bench/micro/protocol_benchmark.cpp`; modify CMake lists and README.

- [ ] Document every header byte, byte order, checksum coverage, namespace/type table, payload limits, parser errors, and fixed-width/varint/TLV tradeoffs.
- [ ] Add a Clang-only opt-in libFuzzer target that feeds arbitrary chunks to `Parser`.
- [ ] Benchmark serialization and parsing separately for 0 B, 100 B, 1 KiB, 4 KiB, and maximum payload.
- [ ] Run full tests and benchmark smoke, `git diff --check`, commit Phase 2, and push `main`.
