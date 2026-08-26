# Phase 10: Linearizable Reads Implementation Plan

1. Add failing Raft tests proving a leader barrier appends a current-term no-op,
   persists it before peer output, commits only with a majority, and rejects on a
   follower.
2. Add `read_barrier()` to the pure and persisted Raft APIs using the same durable
   action driver as command proposals.
3. Add failing cluster tests for leader-only GETs, stale isolated leaders, and
   reads after failover; update the existing catch-up assertions for redirects.
4. Track pending reads by exact barrier index and complete them only when that
   no-op is applied. Fail pending reads on leadership loss and shutdown.
5. Add a Raft barrier microbenchmark and document measured cost plus the future
   ReadIndex optimization.
6. Run focused tests, real-process end-to-end tests, debug and release suites,
   ASan, UBSan, TSan, diff checks, and independent review.
7. Commit and push Phase 10 to `main` before starting Phase 11.
