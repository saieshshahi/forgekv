# Deterministic Raft Simulator Design

## Scope

Phase 7 wraps the Phase 6 pure Raft node in a seeded, single-threaded cluster
scheduler. It controls logical time, delivery order, delay, loss, partitions,
crashes, and restarts without sockets, disk files, sleeps, or wall-clock reads.
The same seed and operation sequence must produce byte-for-byte identical trace
and state dumps.

## Abstract durability

Phase 8 has not implemented files yet, but restart safety cannot treat term,
vote, and log as volatile. The simulator therefore maintains an abstract disk
per node:

- `PersistHardState` atomically replaces durable term and vote;
- `PersistLog` atomically replaces the named suffix;
- a crash destroys only the volatile `RaftNode`;
- restart reconstructs a node from its abstract durable term, vote, and log at
  the simulator's current logical time.

Commit index is volatile under the basic Raft model. A restarted node begins
with commit/applied index zero and learns commitment again from a leader. This
abstract model validates consensus logic and action ordering; Phase 8 will
replace each abstract operation with crash-point-tested storage.

## Scheduler

The simulator owns nodes in ascending ID order, global logical time, a monotonic
message ID, a pending-message collection, a bidirectional blocked-link matrix,
a seeded PRNG, a bounded trace, and invariant history.

An emitted `SendMessage` becomes a scheduled envelope containing source,
destination, typed message, creation time, earliest delivery time, and sequence
ID. Public controls can:

- advance every active node to an exact monotonic time;
- deliver any selected eligible message, enabling arbitrary reordering;
- delay a message to a later logical time;
- drop a selected message;
- block or heal one link or all links around a node;
- crash or restart any node;
- submit a proposal to any active node.

Blocked, future-dated, or crash-targeted messages remain pending until explicitly
dropped or made eligible. This permits delivery of stale traffic after heal or
restart. No control method waits on real time.

## Invariants

Checks run after every completed state-changing simulator operation. Historical
facts are captured while each returned Raft action stream is processed:

1. historical election safety: one elected node per term;
2. committed index content is immutable globally;
3. a node never loses a log entry it previously committed;
4. nodes that mark the same index committed contain the same entry;
5. no apply occurs above the node's commit index;
6. applied prefixes agree for nodes caught up to the same index;
7. same index/term entries satisfy log matching, including their prefix;
8. per-node persisted log remains gap-free with nondecreasing terms.

On failure the simulator throws a report containing seed, operation count,
logical time, trace, pending messages, active/crashed states, roles, terms, logs,
commit indexes, applied indexes, and abstract durable state.

## Randomized histories

`run_random(steps)` chooses from time advancement, eligible delivery, loss,
delay, link partition/heal, node crash/restart, and proposal submission. Choices
and payloads come only from the configured PRNG. The test suite runs at least
50,000 scheduler operations across many seeds; the CLI supports much longer
stress runs and prints the seed before execution.

The CLI form is:

```text
forgekv_raft_sim --seed 912831 --steps 100000
```

It exits nonzero and prints the complete replay report on an invariant failure.

## Bounds

Pending messages and trace records have configured hard caps. When the pending
cap is reached, randomized execution must deliver or drop work before producing
more. Trace retention is a rolling tail, while the failure report also includes
the deterministic seed and operation count needed to reproduce the full run.
