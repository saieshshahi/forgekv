# Real Raft Cluster Design

## Scope

Phase 9 connects the persisted deterministic Raft node to real TCP and exposes
an executable `forgekv-server`. Each process has a client listener and a peer
listener. The implementation targets a fixed three-to-seven-node membership;
membership changes, TLS, snapshots, and linearizable read barriers remain later
phases.

## Ownership and queues

One node contains:

- a client `TcpServer` reactor and bounded request workers;
- a peer `TcpServer` reactor and bounded request workers;
- one Raft owner thread containing `PersistedRaftNode` and the applied KV map;
- a bounded outbound-peer queue with worker threads that perform blocking,
  timeout-bounded TCP RPCs away from the Raft owner.

Client proposals, local reads, inbound peer requests, and peer responses enter
the Raft owner through one ordered queue. Timer advancement also occurs on that
thread using monotonic elapsed milliseconds. No socket or worker thread mutates
Raft or state-machine state.

## Peer protocol

Raft uses the existing version-1 frame header and Raft namespace over the
separate peer port. A checksummed frame payload binds cluster ID, source node,
destination node, and exact Raft message kind. All integer fields are bounded
big-endian values. Append entries bound entry count, command length, total
payload length, enum values, and gap-free indexes before allocation or dispatch.

The first implementation opens a timeout-bounded TCP connection per RPC. This
is intentionally simple and correct. Persistent peer connections, handshakes,
reconnect backoff, and batching are future optimizations after profiling.

## Client write completion

A successful PUT or DELETE means all of the following have happened:

1. the leader validated the client frame and encoded a deterministic command;
2. the entry was appended to the leader's Raft log and locally synchronized;
3. enough followers accepted and synchronized the entry for a voting majority;
4. the leader advanced `commitIndex` under Raft's current-term commit rule;
5. the leader emitted `ApplyEntry` in log order;
6. the leader applied that exact command to its state machine; and
7. only then did the waiting client handler produce `OK`.

A local socket write, leader-local append, or follower receipt alone never
produces client success. Disconnecting a client does not cancel an accepted
proposal. Phase 12 adds replicated request deduplication for safe retries.

Followers return `REDIRECT` for mutations when the leader endpoint is known and
a retryable `BUSY` response when it is unknown. Pending proposals are failed as
retryable if leadership is lost before apply.

## Reads in Phase 9

GET is routed through the Raft owner and observes that process's applied local
state. A follower may therefore return an older value, and a process that still
believes it is leader after a partition has no special read authority. This is
useful for Phase 9 catch-up inspection but is explicitly an eventual/local read,
not a linearizable read. Phase 10 replaces it with a committed read barrier.
The limitation is explicit rather than hidden behind a false consistency claim.

## Failure behavior

Peer connection failures act as dropped Raft messages. Elections and later
heartbeats retry through normal Raft behavior. A killed node restarts from its
term, vote, and log, then catches up from AppendEntries. An old leader cannot
answer successful mutations after stepping down. Process integration tests
start three binaries, elect, write/read, kill the leader, continue through a
new leader, restart the old process, and verify follower catch-up.
