# Real Raft Cluster

Status: Phase 9 implementation

ForgeKV runs each Raft member as an independent `forgekv-server` process. Each
process has a client listener and a peer listener on separate TCP ports. A
single owner thread is the only thread allowed to mutate the Raft core and KV
state machine. Socket workers submit bounded work to that owner; outbound Raft
RPCs use a separate bounded worker pool so a slow peer cannot block elections,
heartbeats, or local persistence.

## Write contract

A client `PUT` or `DELETE` receives `OK` only after all of the following have
happened:

1. the receiving process is the current Raft leader;
2. the command is validated and appended to the leader's durable Raft log;
3. AppendEntries has placed the entry durably on a majority of voters;
4. Raft advances the commit index; and
5. the owner thread applies that exact committed log entry to the KV state
   machine.

Local append, sending an RPC, or receiving one follower response is not success.
If leadership is lost before application, the request returns `BUSY`; the
client must discover the leader and retry the exact same logical mutation.
Followers return `REDIRECT` with the known leader's client endpoint, or `BUSY`
while no leader is known. Replicated request deduplication gives conforming
clients an at-most-once state-machine effect across response loss, failover,
restart, and snapshots; its limits are documented in
[`request-deduplication.md`](request-deduplication.md).

`GET` is leader-only and uses a committed Raft barrier before reading the state
machine. Followers never return local values. This deliberately favors clearly
linearizable semantics over the lower cost of a future ReadIndex optimization.

## Peer transport

Every Raft frame carries `cluster_id`, source node ID, destination node ID, and
one RequestVote or AppendEntries message. The receiver rejects wrong clusters,
wrong destinations, responses on the request path, malformed reserved bytes,
impossible entry counts, and frames outside the shared hard size bound. The
deterministic Raft core additionally verifies voter membership, candidate and
leader identity, log continuity, term monotonicity, and RPC correlation.

Transport failures are modeled as dropped messages. Timers and later
heartbeats retry; no network callback runs the consensus core directly.
AppendEntries suffixes are split by exact entry and encoded-byte limits. The
outbound queue keeps only the newest waiting RPC per peer and enforces both item
and byte caps. Successful delayed acknowledgements advance match indexes
monotonically; stale rejections cannot move progress backward.

Peer hosts are numeric IPv4 or IPv6 addresses in Phase 9. This keeps connection
setup and shutdown bounded without relying on potentially stalled synchronous
DNS. A future resolver can add hostnames with asynchronous resolution and
explicit deadlines.

IPv6 command-line peer hosts use brackets, for example
`--peer 1=[::1]:7201:7101`. Redirects use the same unambiguous
`[IPv6]:CLIENT_PORT` form; IPv4 endpoints remain `IPv4:CLIENT_PORT`.

The canonical fixed voter-ID set is fingerprinted into the durable identity and
log headers. Restart with a different voter set fails closed; endpoint changes
do not affect the fingerprint. Dynamic membership requires a future joint
consensus protocol and cannot be emulated by changing command-line arguments.

## Running three local members

Build with `./scripts/build.sh`, then start three processes with the same peer
map and distinct data directories and ports:

```bash
./build/debug/src/forgekv-server --cluster-id 42 --node-id 1 \
  --data-dir /tmp/forgekv-1 --client-port 7101 --peer-port 7201 \
  --peer 1=127.0.0.1:7201:7101 --peer 2=127.0.0.1:7202:7102 \
  --peer 3=127.0.0.1:7203:7103
```

Repeat for node IDs 2 and 3 with their matching local ports and directories.
The process prints `READY` only after storage recovery, the Raft owner, and both
listeners are available.

For deterministic partition testing, `SIGUSR1` toggles all peer traffic for that
process while leaving its client listener live. This is a fault-injection switch,
not an operator mechanism for normal traffic management. Toggling it again heals
the process; Raft then reconciles its term and log with the current leader.

The real-process integration test starts all three members, elects a leader,
checks follower redirects, commits writes, and verifies a restarted follower can
rejoin after a large bounded-batch catch-up. It then keeps the old leader's
client port live while partitioning only peer traffic, elects a replacement,
commits a new value, and proves stale GETs cannot return data. The test also
checks pending-read admission remains bounded after client timeout, drops a
committed mutation response and retries it after snapshot installation and
failover, rejects changed and stale retry IDs, heals the partition, and
inspects the former leader's durable log for the new value.
