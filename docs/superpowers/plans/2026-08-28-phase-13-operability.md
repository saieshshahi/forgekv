# Phase 13: Operability Plan

1. Add failing unit tests for fixed histograms, operational snapshots,
   Prometheus rendering, process sampling, and distinct health/readiness.
2. Implement fixed-cardinality operational metrics with passive atomic reads.
3. Add persistence sync/recovery and background snapshot observations without
   allowing observer failures to affect Raft.
4. Publish request, queue, Raft, peer-lag, storage, and network measurements
   from their owning paths.
5. Add a bounded joinable HTTP administrative listener and tests for request
   limits, `/health`, `/ready`, and `/metrics` content types/statuses.
6. Wire `--admin-bind` and `--admin-port`, expose the bound port, and extend the
   real cluster test to prove followers are healthy/not-ready while the leader
   is healthy/ready.
7. Convert default logs to structured JSON and add sparse lifecycle/error logs
   with correlation IDs on exceptional request paths.
8. Document metric names, types, label bounds, queries, endpoint semantics, and
   why synchronous success logging is excluded.
9. Run focused tests, repeated endpoint tests, complete debug/release suites,
   ASan, UBSan, and Clang TSan; audit for cardinality and lock coupling.
10. Commit and push Phase 13 before starting Phase 14.
