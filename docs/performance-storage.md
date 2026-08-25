# Phase 5: Storage Performance Investigation

## Outcome

Profiling identified software CRC32 as the dominant standalone storage cost.
Replacing the byte-at-a-time table loop with a portable slicing-by-8 loop kept
the WAL checksum format unchanged and improved recovery throughput by 3.4x.
The same change improved synchronous writes by 45% and group-commit writes by
20%. No requested storage or durability behavior was weakened.

## Environment

Measurements were taken on 2026-08-24 in Ubuntu 22.04 under WSL2:

- kernel: `6.18.33.2-microsoft-standard-WSL2`
- CPU: AMD Ryzen 7 7730U, 8 cores / 16 logical CPUs
- compiler: GCC 11.4.0, CMake `Release`
- repository filesystem: WSL `9p`/DrvFS mounted at `/mnt/c`, `noatime`
- perf: Microsoft WSL kernel-matched `6.18.33.2.gc21a03b2943d`
- `kernel.perf_event_paranoid=2`, Valgrind 3.18.1, strace 5.16

These numbers characterize this machine and filesystem, not every Linux host.
In particular, durability latency on a 9p-mounted Windows drive must not be
presented as native NVMe latency.

## Reproduction

The capture script builds the release preset and profiles one named workload:

```sh
./scripts/profile-storage.sh async-put 100
./scripts/profile-storage.sh sync-put 30
./scripts/profile-storage.sh group-put 50
./scripts/profile-storage.sh get 1000
./scripts/profile-storage.sh recovery 3
```

It writes raw JSON, `perf stat`, `perf.data`, collapsed stacks, strace summaries,
Massif output, and SVG flamegraphs under ignored `build/profile-storage/`.
FlameGraph's `stackcollapse-perf.pl` and `flamegraph.pl` must be present under
`build/tools/FlameGraph/`. On WSL, the script prefers a kernel-matched executable
named `build/tools/perf-wsl-*`. It records `perf.data` in `/tmp` before copying
it because perf's memory-mapped writer returns `EFAULT` on DrvFS.

## Baseline evidence

The values below are repeated-run means for the original byte-at-a-time CRC32.
Throughput and p99 come from Google Benchmark's manual-time counters.

| Workload | Repetitions | Throughput | p50 | p99 | Peak heap |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 KiB async PUT | 100 | 8,892 ops/s | 108.6 us | 167.4 us | 364 KiB |
| 4 KiB sync PUT, 8 clients | 30 | 229 ops/s | 18.5 ms | 54.6 ms | 763 KiB |
| 4 KiB group PUT, 8 clients | 50 | 1,422 ops/s | 4.45 ms | 14.8 ms | 816 KiB |
| 4 KiB GET | 1,000 | 3.08M ops/s | 0.287 us | 1.05 us | 359 KiB |
| Recover 64 x 1 MiB values | 3 | 150.6 MiB/s | 412 ms | 539 ms | 67.1 MiB |

CPU profiles showed:

- async PUT: CRC32 was 53% of sampled CPU; copies were 9%; futex waits were
  72% of traced syscall time and writes were 14%.
- sync PUT: CRC32 was 23% of sampled CPU and `fdatasync` 13%; 160 sync calls
  consumed 11% of traced syscall time, while client/writer coordination consumed
  most waiting time.
- group PUT: 40 `fdatasync` calls replaced the sync workload's 160 calls for the
  traced batch; coordination remained the largest waiting cost.
- GET: copies were 23% of sampled CPU. The owning-string API and benchmark setup
  account for much of this; there is no disk read in the steady-state lookup.
- recovery: CRC32 was 89% of sampled CPU; copies were 9%. The 1,284 `pread64`
  calls consumed only 12% of traced syscall time, making checksum CPU the clear
  bottleneck.

Committed baseline flamegraphs:

- [async PUT](performance/flamegraphs/async-put-baseline.svg)
- [sync PUT](performance/flamegraphs/sync-put-baseline.svg)
- [group PUT](performance/flamegraphs/group-put-baseline.svg)
- [GET](performance/flamegraphs/get-baseline.svg)
- [recovery](performance/flamegraphs/recovery-baseline.svg)

The cache-event ratios reported by WSL perf were retained in raw captures but
not used to justify a change: the 52–55% ratios on short async/read runs were
not accompanied by a controllable cache-specific experiment.

## Accepted experiment: slicing-by-8 CRC32

The experiment expands the compile-time lookup table from 1 KiB to 8 KiB and
processes eight input bytes per loop. It uses the same reflected IEEE polynomial
and initial/final XOR, requires no CPU-specific instructions, and preserves
incremental `update()` behavior. Standard vectors, all 256 byte values, every
split boundary, WAL corruption, and recovery tests protect compatibility.

| Workload | Baseline | Slicing-by-8 | Change | Baseline p99 | New p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| async PUT | 8,892 ops/s | 9,680 ops/s | +8.9% | 167 us | 159 us |
| sync PUT | 229 ops/s | 332 ops/s | +44.9% | 54.6 ms | 40.6 ms |
| group PUT | 1,422 ops/s | 1,713 ops/s | +20.5% | 14.8 ms | 13.6 ms |
| recovery | 150.6 MiB/s | 511.2 MiB/s | +239.5% | 539 ms | 187 ms |

The optimized recovery profile still attributes 56% of sampled CPU to CRC32,
but total recovery time is far lower and copies are now visible at 35%.
[Compare the optimized recovery flamegraph](performance/flamegraphs/recovery-slicing-by-8.svg).

The change is retained because recovery and both durable-write modes exceed the
10% acceptance threshold and every measured p99 improved. The async result is
reported as below-threshold rather than treated as a separate success claim.

## Candidate decisions

| Candidate | Evidence and experiment | Decision | Regression risk |
| --- | --- | --- | --- |
| `writev` | WAL encoding already produces one contiguous record and one `write`; serialization/copy cost was not the recovery bottleneck. | Reject for now. | Partial-vector writes and more complex retry bookkeeping. |
| preallocated encoding buffers | Allocation appeared only in low single-digit CPU samples; Massif peaks were small outside the intentionally large recovery state. | Reject for now. | Capacity retention and harder ownership. |
| buffer pooling | No allocator bottleneck remained after CRC optimization. | Reject. | Lifetime bugs, contention, unbounded retained memory. |
| batched WAL writes | Existing group commit reduced 160 durability calls to 40 and was already 6.2x faster than sync in the baseline. | Keep existing design; no second batching layer. | Longer queue delay and tail-latency spikes. |
| aligned records | No alignment-specific cache or syscall evidence. | Reject. | Larger WAL and format migration. |
| `mmap` WAL | The measured problem was checksum CPU, not `write`; mapping does not remove durability requirements. | Reject. | SIGBUS/truncation hazards and subtle dirty-page lifetime. |
| direct I/O | No page-cache pollution evidence and the current format is not sector-aligned. | Reject. | Alignment complexity, portability loss, worse small writes. |
| hardware CRC | SSE4.2 CRC32 uses CRC32C, not the persisted IEEE CRC32 polynomial. PCLMUL implementations add CPU dispatch and validation complexity. | Defer until portable slicing-by-8 is insufficient. | Cross-CPU behavior and checksum compatibility mistakes. |

## Limits and next measurements

Profiles include benchmark fixture setup, so very short GET runs expose setup
copies and writes in addition to the timed lookup. A future native-Linux run
should pin CPUs, record host power state, use a native ext4/XFS filesystem, and
repeat confidence intervals before setting service-level performance targets.
The Phase 16 methodology will add multi-client saturation and longer steady-state
runs; Phase 18 will separately profile the network path.
