# Storage Profiling Design

## Scope

Phase 5 investigates the standalone Phase 4 storage engine. It does not change
the WAL format, durability guarantees, public storage API, or Raft design. The
output is reproducible evidence, flamegraphs, a decision record for each
candidate optimization, and—only if evidence clears the acceptance threshold—a
small measured optimization.

## Performance objective

There is no arbitrary product throughput target yet. The objective is to locate
where time and allocations are spent in the existing ASYNC, SYNC,
GROUP_COMMIT, GET, and recovery paths. A change is meaningful when a stable A/B
benchmark shows at least a 10% improvement in the metric governed by the
identified bottleneck, does not regress p99 by more than 5% unless explicitly
trading latency for throughput, and leaves all correctness and sanitizer suites
green.

## Measurement approach

Use the existing release benchmark executable so the profiled path is identical
to the published Phase 4 baseline. Run sufficiently many repetitions for each
profile instead of adding a second synthetic storage implementation.

- `perf stat` measures cycles, instructions, context switches, cache and branch
  behavior.
- `perf record` with DWARF call stacks produces CPU flamegraphs.
- `strace -f -c` attributes wall time and call counts to `write`, `fdatasync`,
  `futex`, allocation-related mappings, and file lifecycle calls.
- Valgrind Massif records heap growth and allocation behavior when available.
- Existing Google Benchmark counters provide throughput and latency before and
  after any experiment.

The primary workloads are 4 KiB ASYNC PUT, 4 KiB SYNC PUT, 4 KiB
GROUP_COMMIT with eight writers, 4 KiB GET with 64 keys, and recovery of 64
one-MiB values. Small-value and one-MiB checks identify size-dependent shifts.

## Candidate decisions

Each candidate receives a row containing baseline, hypothesis, experiment,
measurement, conclusion, and regression risks.

- `writev`: useful only if metadata and payload are naturally separate and copy
  cost is material.
- preallocated encoding buffers: useful only if allocation/resize cost is hot.
- buffer pooling: considered only after preallocation because it adds ownership
  and lifetime complexity.
- batched WAL writes: overlaps existing GROUP_COMMIT and is considered if
  syscall counts remain dominant within a batch.
- aligned records: rejected unless cache or I/O measurements show a concrete
  alignment problem; it increases disk bytes and format complexity.
- `mmap`: rejected by default because truncation, dirty-page lifetime, SIGBUS,
  and flush semantics make correctness harder.
- direct I/O: rejected by default because alignment and page-cache bypass add
  complexity and the current workload has no evidence of cache pollution.

## Artifacts and reproducibility

`scripts/profile-storage.sh` performs named workload captures into an ignored
build directory. Generated SVG flamegraphs copied into
`docs/performance/flamegraphs/` are committed with
`docs/performance-storage.md`, which includes environment details, exact
commands, measurements, conclusions, and limitations of WSL2. Raw `perf.data`,
Valgrind, and trace files remain untracked.

## Safety

Profiling never weakens storage behavior. Any accepted optimization is written
test-first, benchmarked one variable at a time, and verified under debug,
release, ASan, UBSan, and TSan. A result that is noisy, environment-specific,
or below threshold is documented and not merged as an optimization.
