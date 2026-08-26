# Phase 10: Linearizable Reads Design

## Decision

ForgeKV will initially implement leader reads with a distinct Raft no-op barrier.
Each admitted GET appends a no-op entry in the leader's current term. The leader
waits until that exact entry is committed and applied, then reads local state.
Followers redirect GETs to the known leader.

This deliberately spends one durable Raft quorum round trip per read. It is the
simplest strategy whose safety follows directly from the existing log agreement,
commit, and apply-ordering rules.

## Safety argument

A process that merely believes it is leader may be stale after a partition. Its
barrier cannot commit without a current majority. A barrier that does commit is
ordered after every entry before it and is applied only after those entries.
The state-machine lookup therefore observes every completed operation ordered
before the barrier.

If a higher-term message causes leadership loss, every pending read fails with a
retry response. Delayed acknowledgements are accepted only for the exact peer
and exact RPC range already tracked by the Raft core. A delayed acknowledgement
for a barrier sent before a concurrent leadership change cannot make a read that
started after the new leader's completed write appear earlier: followers reject
new lower-term traffic after voting in the higher term. Overlapping operations
may legally be ordered at the committed barrier.

## Alternatives

### Per-read Raft barrier (selected)

- Strongest reuse of existing correctness machinery.
- No clocks, leases, or new quorum protocol.
- Costs a log entry, durable append, replication, and apply per read.

### ReadIndex quorum confirmation

The leader confirms authority with a heartbeat quorum in its current term and
waits for its state machine to apply at least the captured commit index. It avoids
log growth and can batch concurrent reads, but requires separate correlation,
current-term leadership proof, delayed-response handling, and apply-index waits.
This is the planned optimization after measurements justify the complexity.

### Leader lease

A lease can make reads local, but safety depends on bounded clock drift, bounded
message delay assumptions, careful renewal rules, and restart behavior. ForgeKV
does not currently establish those assumptions, so leases are not safe here.

## Runtime integration

The single Raft owner thread predicts the next log index, records pending keys
and completions under that index, and invokes `read_barrier()`. When the matching
no-op `ApplyEntry` arrives, it performs the lookups. A different committed no-op
cannot complete those reads. Leadership loss or shutdown fails all waiters.
Client deadlines use cancellation tokens and expired waiters are detached. The
empty barrier tracking remains until the entry commits or leadership is lost,
and `max_pending_reads` caps outstanding uncommitted barriers. Once the cap is
reached, an isolated leader rejects later reads without growing its log. ForgeKV
does not attach a later read to an earlier barrier: that barrier might already be
logically committed through delayed acknowledgements, placing its linearization
point before the later read's invocation.

## Verification and measurement

Tests cover the core barrier shape, follower redirects, an isolated stale leader
that cannot answer a GET, normal reads, reads after leader failover, and delayed
replication behavior already exercised by the Raft response-correlation suite.
A microbenchmark compares an in-memory local lookup baseline with one simulated
barrier quorum round trip. The real-process load generator remains the end-to-end
tool for measuring TCP, persistence, and scheduling cost.
