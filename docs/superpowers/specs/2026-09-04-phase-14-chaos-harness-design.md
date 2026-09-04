# Phase 14 Chaos Harness Design

## Objective

Build `forgekv-chaos`, a Linux multi-process test harness that exercises real
ForgeKV servers, sockets, persistence, signals, elections, snapshots, and
clients under controlled faults. A successful run must demonstrate post-fault
convergence and state correctness; process survival alone is not success.

The command accepts at least:

```text
forgekv-chaos --nodes 5 --clients 32 --duration 120 --seed 12345
```

Three- and five-node fixed-membership clusters are supported initially. Seeds
make generated topology, workloads, and fault choices reproducible. Exact
wall-clock interleavings are not promised because the kernel, filesystems, and
separate processes remain nondeterministic.

## Approaches considered

### Native multi-process harness with directed fault proxies — selected

A C++ executable launches real server processes and routes every directed peer
connection through an in-process TCP proxy. The proxy can independently block,
delay, jitter, or drop traffic for each source/destination pair without root
access. The harness owns clients, history, lifecycle, validation, and artifacts.

This has the highest implementation cost, but it exercises the actual system
and provides precise, portable test controls for the showcase feature.

### Shell or Python orchestration with operating-system networking

This is quick to prototype, but introduces another runtime, has weaker process
ownership, and generally requires privileged networking configuration for
useful link-level faults. Linux `tc netem` remains valuable and is deferred to
Phase 15 as a separate environment-dependent campaign.

### Extend the in-memory Raft simulator

The existing simulator provides deterministic consensus exploration, but it
does not exercise real processes, sockets, files, signals, admin endpoints, or
crash recovery. It remains complementary rather than serving as Phase 14.

## Architecture

`forgekv-chaos` contains five bounded components:

1. `ProcessCluster` reserves loopback ports, creates isolated node data
   directories, starts servers, sends signals, reaps children, and guarantees
   cleanup. No child is detached.
2. `FaultProxy` owns one directed source-to-destination peer route. It forwards
   byte streams with a bounded queue and applies a current link policy:
   healthy, partitioned, fixed latency, seeded jitter, or seeded loss. Closing
   an existing proxied connection makes policy changes prompt and prevents old
   buffered traffic from bypassing a partition.
3. `ChaosScheduler` uses a documented PRNG and generates a bounded sequence of
   actions: kill leader, kill follower, restart, isolate a node, isolate the
   leader from a majority, add latency/jitter/loss, pause/resume, heal, and
   rapid leader churn. A scripted timeline can replace generation for replay.
4. `ClientWorker` owns one key and one stable client ID. Workers issue
   PUT/DELETE/GET operations with monotonic request IDs, follow redirects, and
   retry ambiguous attempts with the same request ID. A worker does not start
   its next logical operation until the current operation is definitive or the
   chaos measurement interval ends.
5. `Verifier` heals every link, resumes and restarts all nodes, finds a leader,
   waits for matching commit/applied indexes and zero leader-reported lag, and
   checks every owned key against the last acknowledged mutation.

The harness limits node count, client count, duration, queued proxy bytes,
history records, artifact bytes, and per-request retry time. Limit exhaustion
is a controlled failed run with artifacts, never unbounded growth.

## Network fault semantics

A partition drops all traffic on selected directed routes and closes their
current connections. A node partition blocks both directions for every route
touching that node. A leader-majority partition selects enough bidirectional
routes to remove the leader's quorum while preserving connectivity among the
majority side.

Latency delays forwarding by a fixed duration. Jitter adds a seeded bounded
offset per forwarded chunk. Loss makes a seeded per-chunk drop decision and
closes the stream after a drop so the byte-oriented framing cannot silently
splice a corrupt message. Phase 14 therefore tests message transport failure,
not realistic kernel packet reordering. Phase 15 owns `tc netem` packet loss,
jitter, delay, and reordering experiments and clearly distinguishes those from
partitions.

## Client history and correctness

Each JSONL attempt records run sequence, client ID, logical request ID,
operation, key, value hash or bounded value, monotonic start and completion
microseconds, contacted endpoint, response kind, and diagnostic. Timeouts and
connection failures are recorded as ambiguous attempts. Retries retain the
same logical request ID, exercising Phase 12 deduplication.

Each client serializes operations on its own key. A definitive successful PUT
or DELETE updates that client's expected state. Successful GET results must
equal the current acknowledged state because no later operation on that key is
admitted concurrently. Ambiguous mutation attempts are retried until a
definitive duplicate result is obtained or the measurement interval ends; an
unresolved final mutation marks the key unknown for online checks and is
resolved before final verification without admitting another operation.

After all faults stop, the verifier requires:

- every node is running and healthy;
- exactly one serving leader is observed;
- the leader reports zero lag for all voters;
- all nodes expose matching commit and last-applied indexes;
- every resolved client key matches its last acknowledged mutation;
- every successful online GET matched the acknowledged per-key state;
- no server log contains a critical persistence or invariant failure.

Acknowledged mutations use ForgeKV's documented quorum-plus-durable-WAL
guarantee. The final check occurs after killing and restarting every node once,
so an acknowledged value that existed only in volatile memory cannot pass.
This demonstrates the tested history and failure schedule; it is not a proof of
correctness for all executions.

## Scheduling and replay

Generated actions use a stable integer PRNG and explicit distributions. The
timeline records the selected action, target nodes or links, parameters,
planned offset, observed start, and observed finish. Safety preconditions avoid
meaningless actions, such as restarting a live node or resuming an unpaused
node. When no selected action is applicable, the scheduler records a no-op.

Every run writes a replay command. Failure artifacts also include the realized
timeline. `--replay <timeline.jsonl>` uses that timeline instead of PRNG action
selection. Replay reproduces actions and relative offsets, but documentation
states that process and network scheduling can still change responses.

## Artifacts

Runs use an isolated directory containing:

```text
config.json
seed.txt
operations.jsonl
timeline.jsonl
summary.json
replay.txt
logs/node-<id>.log
metrics/node-<id>.prom
data/node-<id>/...
```

Failure artifacts are retained automatically. Successful runs retain a compact
summary by default and may retain the full directory with `--keep-success`.
Writes use temporary files followed by rename for final summary/config files.
History and timeline are append-only during the run so a harness crash leaves
useful partial evidence.

## Failure handling and cleanup

The parent process owns all children, proxy threads, client threads, and file
descriptors. Shutdown first stops new actions and client operations, heals the
network for verification when possible, performs bounded verification, stops
children with TERM then KILL fallback, joins threads, and closes descriptors.
Signals received by the harness trigger the same bounded cleanup path.

Harness failures are categorized as configuration, launch, client protocol,
invariant, convergence timeout, child crash, artifact I/O, or internal. The
summary records the category and first evidence. Cleanup failures are appended
without hiding the original failure.

## Test strategy

Unit tests cover stable seeded action generation, applicability rules, link
policy decisions, bounded queues, JSON escaping/serialization, response
history transitions, final expected-state calculation, and replay parsing.

Integration tests use short three-node runs to cover:

- concurrent writes with follower kill/restart;
- leader-majority partition and failover;
- pause/resume plus link delay and seeded loss;
- full heal, all-node restart, convergence, and exact final values;
- artifact creation and replay command on injected invariant failure;
- repeated harness shutdown without leaked children or threads.

The Phase 14 gate runs focused tests, the short end-to-end chaos campaign with
multiple seeds, the existing complete debug and release suites, ASan, UBSan,
and a source/spec review. TSan is attempted and any platform limitation is
reported rather than silently treated as a pass.

## Explicit non-goals

- production traffic shaping or a production proxy;
- packet-level reordering and kernel queue behavior, which belong to Phase 15;
- exhaustive linearizability checking across concurrent operations on one key;
- dynamic Raft membership;
- claiming production readiness from finite chaos runs.
