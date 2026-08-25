# Standalone Storage Engine

Phase 4 provides a Linux C++ storage library with an in-memory hash table and a
single append-only WAL. It deliberately has no Raft dependency. A later phase
can place this engine behind the Raft-owned persistence boundary without
coupling consensus code to file I/O details.

## WAL record format

Every integer is unsigned and encoded in network byte order. The checksum is
IEEE CRC-32 over header bytes 0–31 followed by the key and value; the checksum
field itself is excluded.

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | magic `FWAL` (`0x4657414c`) |
| 4 | 2 | format version (`1`) |
| 6 | 2 | header size (`36`) |
| 8 | 4 | complete record size |
| 12 | 8 | gap-free LSN, starting at 1 |
| 20 | 1 | operation (`1` PUT, `2` DELETE) |
| 21 | 3 | reserved, must be zero |
| 24 | 4 | key length |
| 28 | 4 | value length; zero for DELETE |
| 32 | 4 | CRC-32 |
| 36 | variable | key followed by value |

Keys are 1–1024 bytes and values are 0–1,048,576 bytes. Binary bytes are valid.
Lengths, operation, reserved fields, LSN, and checksum are validated before a
record reaches the state machine.

## Recovery policy

Recovery scans from byte zero and accepts only a continuous sequence beginning
at LSN 1. EOF inside the final header, payload, or checksum is an interrupted
append and is truncated to the last complete record. A complete record with a
bad checksum, invalid structure, duplicate LSN, gap, or reordering is corruption
and startup fails closed. Recovery never searches past invalid bytes.

## Threading and bounds

One writer thread assigns LSNs, appends records, performs required flushes,
applies mutations in order, and completes callers. GET takes a shared lock over
the in-memory map. The submission queue has entry and byte caps; producers wait
for capacity instead of allocating without bound. Shutdown stops admission,
drains accepted mutations, joins the writer, and is idempotent.

## Durability modes

| Mode | Acknowledgment boundary | Intended use |
|---|---|---|
| `ASYNC` | complete append system calls | throughput experiments where power-loss durability is not promised |
| `SYNC` | `fdatasync` for each mutation | simplest durable standalone behavior and latency baseline |
| `GROUP_COMMIT` | one successful `fdatasync` covering the batch | durable operation with amortized flush cost |

Group commit is bounded by `max_batch_entries`, `max_batch_bytes`, and
`max_batch_wait`. The first record always makes progress even if it alone is
larger than the preferred batch byte limit. Queue limits are independent and
remain hard admission bounds.

These standalone modes are measurement tools. The replicated product contract
in ADR 0004 remains strict quorum durability: a later Raft layer may batch local
flushes, but it may not acknowledge a client based on `ASYNC` storage.

## Failure and performance evidence

Tests cover every possible truncation point in a final record, complete-record
checksum corruption, duplicate replay, and abrupt process death before append,
mid-append, after append, before sync, and after sync. The benchmark matrix
measures PUT, GET, DELETE, and recovery for 100 B, 1 KiB, 4 KiB, 64 KiB, and
1 MiB values with one and 64-key working sets. It reports operations and bytes
per second, p50/p95/p99/maximum latency, and process CPU utilization. A separate
eight-writer durability matrix compares ASYNC, per-request SYNC, and
GROUP_COMMIT at 100 B and 4 KiB to make the batching throughput/latency tradeoff
visible.

### Measured release baseline

The complete matrix was run on 2026-08-24 under WSL2 on a 16-logical-CPU host.
These numbers are environment baselines, not hardware-independent promises.

| Case | ops/sec | p50 | p95 | p99 | max | bytes/sec | process CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| PUT, 100 B, 1 key, ASYNC | 13,804 | 66 us | 103 us | 127 us | 130 us | 1.38 MB/s | 91% |
| PUT, 1 MiB, 1 key, ASYNC | 295 | 3.35 ms | 3.65 ms | 4.88 ms | 5.52 ms | 309 MB/s | 100% |
| GET, 100 B, 1 key | 11.15 M | 0.08 us | 0.09 us | 0.43 us | 0.78 us | 1.11 GB/s | 108% |
| recovery, 1 MiB, 64 keys | 2.03 | 489 ms | 536 ms | 536 ms | 536 ms | 136 MB/s | 100% |
| eight writers, 100 B, ASYNC | 6,578 | 102 us | 126 us | 161 us | 186 us | 658 kB/s | 180% |
| eight writers, 100 B, SYNC | 355 | 11.9 ms | 22.2 ms | 24.3 ms | 24.9 ms | 35.5 kB/s | 13% |
| eight writers, 100 B, GROUP_COMMIT | 2,107 | 3.10 ms | 4.42 ms | 4.61 ms | 4.64 ms | 211 kB/s | 49% |

In this run, grouping eight writers delivered about 5.9 times the durable write
throughput of per-request sync while keeping the same flush-before-acknowledgment
rule. It paid a configured batching delay and therefore remained slower than
the relaxed ASYNC mode.
