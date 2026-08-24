# ADR 0001: Ownership-Based Threading Model

- Status: Accepted
- Date: 2026-08-24

## Context

ForgeKV must keep Raft timers responsive while sockets are slow, disks flush,
snapshots serialize, and clients overload the service. Shared mutable consensus
state would make ordering and lifetime bugs difficult to test. A thread per
connection would consume stacks and scheduler time and would still require
synchronization around Raft and storage.

The initial objective is a correct, understandable three-to-seven-node store,
not maximum per-core throughput before measurement.

## Decision

Use an ownership-based pipeline with bounded message queues:

```text
network reactor -> request executor pool -> Raft owner -> apply owner
       ^                    |                   |
       |                    |                   v
       +------ completions -+              WAL owner
                                                 |
                                         snapshot worker
```

The exact ownership is:

| Thread or pool | Mutable state it owns |
|---|---|
| One network reactor | sockets, connection generations, parser buffers, output queues, readiness interest |
| Fixed request-executor pool | transient request objects only; no shared protocol state |
| One Raft thread | role, term, vote, peer replication progress, log metadata, commit index, ReadIndex contexts |
| One WAL thread | WAL and hard-state file descriptors, segment lifecycle, local durable index |
| One apply thread | KV state, deduplication state, last-applied index |
| At most one snapshot worker | one immutable snapshot image and its temporary output file |
| Metrics/admin thread | exported metrics snapshot and administrative listener state |

Owners expose typed asynchronous commands. Replies contain immutable values and
an internal correlation ID. No caller receives a mutable reference owned by
another thread.

All queues have item and byte capacity. Each queue defines high and low water
marks, rejection behavior, and shutdown behavior. The reactor pauses reads when
downstream work is saturated. Raft control messages receive reserved capacity so
client overload cannot prevent elections or heartbeats.

The apply thread serializes mutations and public reads. A read is enqueued only
after Raft produces a valid ReadIndex, and it executes only when
`lastApplied >= readIndex`.

## Why this decision

- Raft transitions have one total program order and need few locks.
- Disk stalls do not block network readiness or election timers.
- Network callbacks cannot accidentally perform disk work.
- Shutdown can follow producer-to-owner dependencies rather than race shared
  object destruction.
- Queue occupancy exposes overload instead of hiding it in thread stacks or
  allocator growth.
- The design can be model-tested by replacing queues and workers with
  deterministic schedulers.

## Alternatives considered

### One thread for the entire node

This has the smallest synchronization surface, but `fdatasync`, snapshot work,
large value copying, or a slow metrics scrape could delay heartbeats and all
clients. Rejected because blocking disk I/O is fundamental to the durability
contract.

### One thread per connection

This is easy to demonstrate but scales poorly because of thread stacks,
scheduler overhead, context switches, and lock contention around the single
Raft state. It also produces unpredictable tail latency under connection churn.
Rejected.

### Reactor per core with sharded execution

This can increase network throughput, but connection assignment, cross-reactor
responses, shared global limits, and safe shutdown add complexity. The single
Raft and apply owners would remain serialization points. Deferred until profiles
show the reactor is the limiting resource.

### General actor runtime

Actors match the ownership concept, but a new runtime adds scheduling and
debugging behavior not required for the initial system. Explicit threads and
typed queues make operational behavior easier to inspect. Rejected initially.

## Consequences

Positive consequences:

- synchronization policies are explicit;
- blocking operations are isolated;
- queue bounds provide measurable backpressure;
- fault injection can target every asynchronous completion boundary.

Costs and constraints:

- requests cross several queues and require correlation bookkeeping;
- copying or reference-counting immutable key/value buffers must be controlled;
- one Raft and one apply thread impose throughput ceilings;
- executor completion can arrive out of order, so per-request identity must be
  preserved end to end.

## Scheduling rules

1. Raft peer traffic and timer events have reserved queue capacity and priority
   over new client work.
2. No queue wait on the reactor or Raft thread is unbounded.
3. A producer that cannot enqueue follows the boundary's explicit policy:
   pause, return `BUSY`, coalesce an idempotent notification, or fail the
   connection.
4. Disk completions are processed even while client admission is paused.
5. Snapshot work is lower priority than WAL work and can be cancelled before
   publication.

## Shutdown implications

Shutdown first stops producers, then drains or cancels their messages, then joins
owner threads, and only then destroys owned objects. Connection completions use
both connection ID and generation, so a late result cannot target a reused
socket object. A forced stop remains recoverable as a crash.

## Revisit criteria

Create a new ADR before changing this model. Consider reactor sharding or
parallel state-machine reads only if production-like benchmarks show a sustained
CPU bottleneck in that owner, queue latency dominates end-to-end latency, and
the proposed design preserves the invariants in `docs/invariants.md`.
