# Deterministic Raft Simulator

Phase 7 provides an in-process, single-threaded cluster harness around the pure
Raft core. It uses logical time and typed messages, so a run never depends on
sockets, files, threads, sleeps, or wall-clock timing. A seed and operation
count reproduce the same choices, trace, and final state.

## Run a history

Build the project, then run:

```bash
./build/debug/src/forgekv_raft_sim --seed 912831 --steps 100000
```

The seed is printed before execution. Success reports the number of scheduler
operations and final logical time. An invariant failure exits nonzero and
includes the seed, bounded trace tail, pending messages, node roles and terms,
commit/apply positions, votes, and complete abstract durable logs.

`--seed` defaults to `1` and `--steps` defaults to `100000`. Use `--help` for
the compact command synopsis.

## Failure controls

`RaftSimulator` can advance to an exact logical time; deliver a selected
eligible message; reorder, delay, or drop messages; block or heal links;
isolate a node; crash or restart a node; and submit a proposal. Blocked,
future-dated, and crash-targeted messages remain queued, allowing stale traffic
to be delivered after a heal or restart.

Each node has an abstract disk containing `currentTerm`, `votedFor`, and its
log. Persistence actions update that disk atomically. A crash discards the
volatile Raft object. A restart reconstructs it from the abstract disk at the
current logical time, while volatile commit and apply positions restart at
zero. Phase 8 replaces this abstract boundary with real crash-consistent files.

## Checked safety properties

Every completed scheduler operation validates:

- at most one elected leader per term, including historical leaders;
- immutable committed entries and retention by every node that observed them;
- agreement of committed and applied prefixes;
- no apply beyond `commitIndex` and no commit beyond the local log;
- Raft's log-matching property; and
- gap-free durable logs with nondecreasing, valid terms.

The unit suite runs 50,000 randomized scheduler operations over 50 three-node
seeds plus 10,000 operations covering five- and seven-node clusters, in
addition to focused election, partition, delayed-message, queue-bound, crash,
restart, and deterministic-replay cases. The CLI is intended for longer local
or CI stress runs. Random execution automatically drains or drops messages near
the configured pending-message cap, while retained traces use a bounded rolling
window.

This simulator proves properties of the modeled Raft transition system. It does
not yet prove file persistence ordering, process-level crash recovery, socket
behavior, or production thread integration; those enter in later phases.
