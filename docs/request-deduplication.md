# Request Deduplication

Status: Phase 12 implemented

## Guarantee and client contract

ForgeKV gives conforming mutation clients an at-most-once state-machine effect
when responses are lost and requests are retried. This guarantee survives
leader changes, process restarts, log compaction, and snapshot installation.

Each client uses a stable 16-byte `client_id`, assigns monotonically increasing
nonzero `request_id` values, and has at most one unresolved mutation for that
identity. A retry must preserve the client ID, request ID, operation, key, and
value exactly. Clients needing concurrent mutations use separate identities.

This is effectively-once behavior within that contract, not end-to-end
exactly-once delivery. A timeout still means outcome unknown until the same
logical request receives a definitive response.

## Apply rule

For each admitted client identity, the replicated state machine retains the
latest request ID, exact canonical command bytes, and exact response:

| Incoming request | Deterministic result |
|---|---|
| ID greater than retained ID | Apply once and replace the retained record |
| Same ID and exact command bytes | Return the retained response; do not mutate |
| Same ID and different command bytes | `ERROR` code 3; do not mutate |
| ID lower than retained ID | `ERROR` code 4; do not mutate |

PUT retains its empty success payload. DELETE retains whether the key existed,
so a duplicate DELETE returns the original `00` or `01` result rather than the
result of deleting a second time. The rule runs while applying committed Raft
entries on every member; a leader-local cache is not trusted for correctness.

## Retention limit

The initial implementation retains one latest record for at most 1,024 client
identities and at most 64 MiB of canonical command/result bytes. It never evicts
by local time or LRU order, because independently timed eviction would make
replicas disagree and could admit a duplicate effect. A request that would
exceed either bound receives `ERROR` code 5 before changing user state.

Safe reclamation is future work and requires a replicated close-session or
epoch transition. There is no time-based retention promise in this version.

## Snapshot and upgrade behavior

State-machine snapshot payload version 2 stores sorted key/value records and
sorted deduplication records at one applied Raft boundary. Installation and
startup restore both maps together. Version 1 snapshots remain readable but do
not contain retry history; consequently a request whose only surviving evidence
is a version 1 snapshot cannot receive the Phase 12 duplicate guarantee after
upgrade. Every newly written snapshot uses version 2.

## Demonstrated cases and limits

Unit coverage exercises original DELETE-result replay, changed same-ID reuse,
stale IDs, capacity, snapshot encoding corruption, version 1 compatibility, and
snapshot restore. The real three-process test sends a DELETE, closes the socket
without reading its committed response, takes and installs a snapshot, changes
leaders, retries the exact DELETE, and receives its original result without a
second effect.

The implementation does not provide multi-operation transactions, concurrent
request windows within one identity, automatic identity reclamation, or a way
to recover a lost client identity.
