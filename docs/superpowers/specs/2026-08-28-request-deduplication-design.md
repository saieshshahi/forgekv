# Phase 12: Request Deduplication Design

## Guarantee

ForgeKV gives conforming mutation clients an at-most-once state-machine effect
across response loss, retries, leader changes, snapshots, and restart. A client
uses a stable 16-byte `client_id`, assigns monotonically increasing nonzero
`request_id` values, permits only one unresolved mutation per identity, and
retries the exact same command bytes for an unresolved ID.

This is effectively-once behavior within that client contract. It is not a
claim of end-to-end exactly-once delivery: a timeout still leaves the outcome
unknown until the same logical request is retried.

## Deterministic apply rule

The replicated state machine retains the latest request for each admitted
client: request ID, exact canonical replicated command bytes, and its response.
Every node evaluates the same rule while applying committed entries:

- greater ID: apply once and replace the retained record;
- equal ID and equal bytes: return the retained response without mutation;
- equal ID and different bytes: reject permanent request-ID reuse;
- lower ID: reject as stale.

Exact bytes avoid relying on a collision-prone local hash for correctness. PUT
stores an empty successful result. DELETE stores whether the key existed, so a
retry returns the original answer rather than applying the delete again.

## Retention and resource bound

The initial state machine retains one record for at most 1,024 client identities
and at most 64 MiB of canonical command/result bytes. It never evicts records by
local time or LRU order. A request that would exceed either deterministic bound
receives a resource-limit error before user-state mutation. Safe reclamation
requires a future replicated close-session or epoch command.

## Snapshots and compatibility

State-machine snapshot payload version 2 contains sorted key/value entries and
sorted client deduplication records. Version 1 payloads remain readable and
restore an empty deduplication table; therefore retries of mutations committed
before a version-1 snapshot cannot be proven duplicate after such an upgrade.
New snapshots always use version 2.

Snapshot installation replaces user and deduplication state together before
the follower acknowledges completion. Background snapshot copying captures both
maps at the same applied Raft boundary.

## Client responses

Successful duplicates use the normal operation response. Permanent validation
failures use protocol `ERROR` with stable codes:

- 3: request ID reused with a different command;
- 4: stale request ID;
- 5: client-identity retention capacity reached.

## Required evidence

Unit tests cover apply-once DELETE results, exact duplicate replay, changed
same-ID rejection, stale rejection, capacity, snapshot round trip, version-1
compatibility, and restart restoration. The real-cluster test drops an applied
response, changes leaders, retries the exact request, and verifies the original
result is returned without a second state-machine effect.
