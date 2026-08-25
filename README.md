# ForgeKV

ForgeKV is a Linux-first replicated key-value store built in staged, testable
increments. The current repository begins with the engineering harness; the
architecture and correctness contracts live in [`docs/`](docs/).

## Prerequisites

On Ubuntu 22.04 or newer:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build git
```

CMake downloads pinned GoogleTest and Google Benchmark sources during the first
configure. Later builds reuse the copies under the selected build directory.

On Windows, run all project commands inside WSL2 because the networking phase
uses Linux `epoll`:

```powershell
wsl -d Ubuntu
cd /mnt/c/path/to/forgekv
```

## Build and test

Every script resolves the repository from its own location, so it can be invoked
from any working directory.

```bash
./scripts/build.sh             # debug build
./scripts/build.sh release     # optimized build
./scripts/test.sh              # debug build and all tests
./scripts/test.sh release      # release build and all tests
./scripts/bench.sh             # release build and benchmarks
```

To prove a clean configure, remove only the desired build preset directory and
run its script again:

```bash
rm -rf build/debug
./scripts/test.sh debug
```

The configured build targets are:

- `forgekv_common`: shared, narrowly scoped primitives such as logging;
- `forgekv_raft`: deterministic Raft core plus its crash-safe persistence
  driver with durable term/vote/log ordering;
- `forgekv_sim` and `forgekv_raft_sim`: deterministic cluster simulator library
  and seeded stress runner;
- `forgekv_unit_tests`: GoogleTest unit test executable; and
- `forgekv_benchmarks`: Google Benchmark executable.

## Sanitizers and hardening

```bash
./scripts/test-asan.sh
./scripts/test-ubsan.sh
./scripts/test-tsan.sh
```

ASan, UBSan, and TSan use separate build directories and are not combined.
Sanitizer runtime support depends on the Linux kernel/toolchain combination; CI
on Ubuntu is the authoritative baseline. On WSL2, install `clang` for TSan;
`test-tsan.sh` uses Clang and disables address-space randomization for the test
process to avoid WSL shadow-memory collisions. Native Ubuntu CI uses GCC TSan.
Debug builds enable
`_GLIBCXX_ASSERTIONS` by default. It can be set explicitly with:

```bash
cmake --preset debug -DFORGEKV_GLIBCXX_ASSERTIONS=ON
```

All ForgeKV targets compile with:

```text
-Wall -Wextra -Wpedantic -Wconversion -Wshadow
```

## Logging

`common/logging.h` provides severity levels from `trace` through `critical`, a
runtime minimum, and an injectable sink. `FORGEKV_LOG` avoids evaluating a
message that is filtered at runtime. Define `FORGEKV_COMPILED_LOG_LEVEL` to the
numeric `Severity` value to compile out lower levels; the default `0` retains all
levels.

## Repository layout

```text
src/                 production libraries
tests/unit/          focused unit tests
tests/integration/   real component and socket tests
tests/failure/       crash/recovery tests
tests/model/         consensus/model tests
bench/micro/         in-process microbenchmarks
bench/client/        load generators
cmake/               project CMake modules
scripts/             local and CI entry points
docs/                architecture, invariants, ADRs, and formats
```

CI calls the same build/test scripts shown above for debug, release, ASan,
UBSan, and TSan configurations.

The version 1 binary framing contract is documented byte-for-byte in
[`docs/protocol.md`](docs/protocol.md). Build the optional parser fuzz target
with Clang using:

```bash
cmake -S . -B build/fuzz -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DFORGEKV_BUILD_TESTS=OFF \
  -DFORGEKV_BUILD_BENCHMARKS=OFF \
  -DFORGEKV_BUILD_FUZZERS=ON
cmake --build build/fuzz
```

Run the Phase 3 networking load generator against an in-process epoll server by
leaving `port` at zero:

```bash
./scripts/build.sh release
./build/release/bench/forgekv_load_generator \
  --port=0 --concurrency=4 --pipeline=16 --requests=10000 \
  --key-size=16 --value-size=100 --persistent=1
```

Set `--host` and a nonzero `--port` to target an external server. Output reports
operations/sec, bytes/sec, errors, and p50/p95/p99/maximum response latency.

## Standalone storage benchmark

Phase 4 adds a checksummed append-only WAL and in-memory KV engine with ASYNC,
SYNC, and bounded GROUP_COMMIT modes. The byte format and recovery behavior are
documented in [`docs/storage.md`](docs/storage.md).

```bash
./build/release/bench/forgekv_benchmarks --benchmark_filter=Storage
```

The matrix covers PUT, GET, DELETE, and recovery across supported value sizes
and reports throughput, tail latency, byte rate, and process CPU utilization.

## Current scope

The implementation proceeds in phases. The pure Raft core, deterministic fault
simulator, and crash-safe local Raft persistence driver are implemented, but
Raft is not yet connected to the production network path. See
[`docs/raft.md`](docs/raft.md),
[`docs/raft-simulator.md`](docs/raft-simulator.md), and
[`docs/raft-persistence.md`](docs/raft-persistence.md), plus
[`docs/architecture.md`](docs/architecture.md) for the current and target
systems.
