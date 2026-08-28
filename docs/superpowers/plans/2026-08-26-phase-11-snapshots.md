# Phase 11: Snapshots Implementation Plan

1. Add failing snapshot-format tests for deterministic bytes, identity binding,
   format version, checksum, temporary-file recovery, and atomic crash points.
2. Implement the bounded snapshot codec/store and deterministic KV image codec.
3. Add failing Raft tests for a nonzero log base, local compaction, chunked
   InstallSnapshot, delayed/wrong RPC responses, and suffix preservation rules.
4. Refactor Raft index access around the snapshot sentinel and extend peer codecs
   and persisted action ordering for snapshot installation.
5. Seed Raft storage replay from a valid snapshot and atomically rewrite the log
   suffix only after snapshot publication.
6. Add the bounded background snapshot worker and cluster trigger. Restore the
   state machine from the snapshot before processing post-snapshot log entries.
7. Extend the real-process test: stop a follower, compact the leader, restart the
   follower, install in chunks, catch up, fail over, and verify durable state.
8. Add crash-boundary tests and snapshot pause/duration benchmarks by dataset
   size; document measured results and temporary stop-the-world copy behavior.
9. Run focused tests, full debug/release suites, ASan, UBSan, TSan, diff checks,
   and independent review. Commit and push Phase 11 before Phase 12.
