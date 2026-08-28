# Phase 12: Request Deduplication Plan

1. Add failing deterministic state-machine tests for duplicate, reuse, stale,
   capacity, and restored-result behavior.
2. Implement a single-owner replicated KV state machine with bounded latest-ID
   records and exact canonical command matching.
3. Add failing snapshot-codec tests for version-2 deduplication state and
   version-1 compatibility, then implement the new format.
4. Connect apply, reads, startup restore, snapshot install, and background
   snapshot creation to the combined state.
5. Add stable client error mappings and protocol documentation.
6. Extend the real three-node scenario with response drop, leader change, exact
   retry, changed reuse, stale rejection, and snapshot/restart evidence.
7. Run focused tests, repeated live tests, complete debug/release suites, and
   ASan/UBSan/Clang-TSan.
8. Request blocker-focused review, update guarantees and limitations, then
   commit and push Phase 12 before beginning Phase 13.

