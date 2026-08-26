# Linearizable reads

ForgeKV GET requests are leader-only and use one replicated no-op barrier for
each admitted read. The leader returns a value only after its exact barrier is
durably stored,
replicated to a majority, committed, and applied. Followers redirect GETs. A
leader isolated from its quorum returns `BUSY` after the configured client
deadline instead of serving possibly stale local state.

This remains safe across partitions, leadership changes, and delayed messages.
Pending reads are keyed by the exact barrier log index and are failed if the
process observes leadership loss or shuts down. Timed-out waiters are canceled
and detached. Timed-out barrier slots remain accounted for until commit or
leadership loss, so `max_pending_reads` bounds log growth on an isolated leader
and later reads receive `BUSY` once the cap is reached.

## Strategy comparison

| Strategy | Read path | Assumptions | Current status |
| --- | --- | --- | --- |
| Replicated barrier | Durable log append plus quorum | Raft safety only | Implemented |
| ReadIndex | Quorum heartbeat plus apply-index wait | Correct response correlation and current-term proof | Planned optimization |
| Leader lease | Local lookup while lease is valid | Bounded clock drift and timing assumptions | Rejected for now |

The barrier prioritizes an auditable proof over throughput. ReadIndex is the
preferred future optimization because it avoids read-generated log growth and
can batch concurrent confirmations without introducing clock assumptions.

## Benchmark

`forgekv_benchmarks --benchmark_filter='LocalReadBaseline|DurableReadBarrier'`
compares a local map lookup with a leader-side durable barrier and simulated
immediate majority acknowledgement. The barrier number is a lower bound for a
real deployment because it excludes follower disk, TCP, and scheduling delay.

Reference WSL development measurement on 2026-08-26 (optimized build, fixed
iterations):

| Path | Wall time per operation |
| --- | ---: |
| Local map lookup | 141 ns |
| Durable leader barrier plus simulated immediate quorum acknowledgement | 3.38 ms |

The selected correctness-first path is about 24,000 times slower than the local
lookup in this microbenchmark. The number is intentionally not presented as
cluster throughput: the benchmark includes the leader's durable append but not
follower persistence, TCP, or queueing. It demonstrates why a carefully tested,
batched ReadIndex path is the most valuable future read optimization.
