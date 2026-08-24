# Phase 3 Linux Networking Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-reactor, nonblocking Linux TCP server with incremental framing, bounded dispatch, response routing, backpressure, metrics, integration tests, and a load generator.

**Architecture:** One level-triggered epoll thread owns listeners and connections. Parsed requests enter a bounded executor queue; worker completions return through a bounded queue and wake the reactor via eventfd. Connection ID plus generation prevents late completions from targeting reused file descriptors.

**Tech Stack:** C++20 Linux sockets, epoll, eventfd, GoogleTest, existing ForgeKV protocol library.

**Spec:** `docs/architecture.md` and `docs/adr/0002-networking-model.md`.

## Global Constraints

- No blocking disk or arbitrary handler work runs on the reactor.
- Use level-triggered epoll, nonblocking sockets, `SO_REUSEADDR`, and `TCP_NODELAY`.
- Bound request size, input/output bytes per connection, in-flight requests, and global outstanding work.
- Handle `EINTR`, `EAGAIN`, fragmented reads, coalesced frames, partial writes, peer half-close, and connection errors.
- Slow consumers never create unbounded output queues.

---

### Task 1: Configuration, tokens, and metrics

**Files:** Create `src/net/reactor_config.h`, `src/net/connection_token.h`, `src/net/metrics.h`, `src/net/metrics.cpp`, `tests/unit/net_config_test.cpp`, `tests/unit/net_metrics_test.cpp`; modify CMake.

**Interfaces:** `ReactorConfig::validate()`, `ConnectionToken{id,generation}`, `Metrics::snapshot()` returning all required fixed-cardinality counters/gauges.

- [ ] Write validation and monotonic metrics tests and verify RED.
- [ ] Implement exact defaults and invariants, then pass tests.

### Task 2: Bounded request executor

**Files:** Create `src/server/request_executor.h`, `src/server/request_executor.cpp`, `tests/unit/request_executor_test.cpp`.

**Interfaces:** `RequestExecutor(worker_count, max_items, max_bytes, Handler)`, `try_submit(Request)`, `stop()`. `Handler` returns a protocol frame and a completion callback receives connection token and frame.

- [ ] Write tests for normal execution, item/byte rejection, exception-to-ERROR conversion, drain, and idempotent stop.
- [ ] Verify RED, implement with condition variables and bounded queue, then pass under TSan.

### Task 3: epoll reactor and server

**Files:** Create `src/net/reactor.h`, `src/net/reactor.cpp`, `src/net/connection.h`, `src/net/socket_ops.h`, `src/net/socket_ops.cpp`, `src/server/tcp_server.h`, `src/server/tcp_server.cpp`, `tests/integration/tcp_server_test.cpp`.

**Interfaces:** `TcpServer(ReactorConfig, Handler)`, `start(bind_address, port)`, `bound_port()`, `stop()`, `metrics()`.

- [ ] Write a real-socket PING round-trip integration test and verify RED.
- [ ] Implement listener creation, epoll/eventfd loop, accept, input parsing, dispatch, completion wakeup, and ordered output.
- [ ] Add failing tests then implementations for fragmented frames, multiple frames/read, partial/slow reads, peer shutdown, reconnect, and clean stop.
- [ ] Add failing saturation tests then implement input/output/in-flight/global backpressure and BUSY/close policies.
- [ ] Assert required metrics in integration tests and run TSan.

### Task 4: Load generator and operational documentation

**Files:** Create `bench/client/load_generator.cpp`; modify CMake, README, and `docs/adr/0002-networking-model.md` only for concrete defaults/results.

- [ ] Add CLI options for host, port, concurrency, persistent/reconnect mode, pipeline depth, operation count, key bytes, and value bytes.
- [ ] Use the protocol serializer/parser and report operations/sec, bytes/sec, errors, and latency percentiles.
- [ ] Start a local PING server fixture, run smoke and bounded-load tests, and record measured host/environment output without claiming general capacity.
- [ ] Run full tests, sanitizer suites, `git diff --check`, commit Phase 3, and push `main`.
