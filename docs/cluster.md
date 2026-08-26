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
client must discover the leader and retry. Followers return `REDIRECT` with the
known leader's client endpoint, or `BUSY` while no leader is known. Phase 12
adds deterministic retry deduplication; until then, retrying a timed-out
mutation can append a second equivalent command.

Phase 9 `GET` is deliberately a local, eventual read on any node. This makes
follower catch-up observable but is not a linearizable-read claim. Phase 10
replaces this with a quorum-backed linearizable read barrier.

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

The real-process integration test starts all three members, elects a leader,
checks follower redirects, commits a write, kills the leader with `SIGKILL`,
elects a replacement, continues writing, restarts the old member from its
original disk, verifies catch-up on every node, and proves an isolated leader
cannot acknowledge a mutation.
