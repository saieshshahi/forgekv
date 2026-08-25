# Deterministic Raft Core

Phase 6 provides a deterministic Raft state machine in `src/raft`. It has no
networking, filesystem, storage-engine, thread, sleep, or wall-clock dependency.
Callers advance logical time, deliver typed messages, and submit opaque command
bytes. Each call returns an ordered vector of actions for an outer runtime to
execute.

## Configuration

`RaftConfig` fixes the node ID, voter IDs, election timeout range, heartbeat
interval, and PRNG seed. Membership must contain 3, 5, or 7 unique nonzero IDs
including this node. The heartbeat interval is positive and strictly below the
minimum election timeout.

The node mixes its ID into the configured seed, so every node gets a distinct
but reproducible election-timeout stream. Logical time starts at zero and cannot
move backward. Advancing across an election or heartbeat deadline performs the
corresponding transition immediately; there are no real sleeps.

## State and log

The core owns:

- `current_term`, optional `voted_for`, and follower/candidate/leader role;
- optional known leader ID;
- a gap-free log with an internal index-0, term-0 sentinel;
- `commit_index` and `last_applied`;
- candidate vote identities;
- leader `next_index`, `match_index`, newest RPC ID, and the last index covered
  by that RPC for every follower.

Real entries are command or no-op entries with an index, term, and immutable
byte vector. Log terms and indexes are nondecreasing and gap-free. Term 0 cannot
grant a vote or establish a leader.

## Messages

The core accepts four typed RPC messages:

- `RequestVote {term, candidate_id, last_log_index, last_log_term}`
- `RequestVoteResponse {term, vote_granted}`
- `AppendEntries {term, leader_id, previous index/term, entries,
  leader_commit, rpc_id}`
- `AppendEntriesResponse {term, success, match_index, reject_hint, rpc_id}`

Messages from self or IDs outside fixed membership are ignored. Any higher-term
message first updates term, clears the vote, and moves a candidate or leader to
follower. Stale requests receive a current-term rejection; stale responses are
ignored.

Vote freshness compares last log term first and index second. Granted votes are
identity-counted, so duplicate responses never create a quorum. A leader appends
a current-term no-op on election and initializes follower progress from the end
of its inherited log.

## Actions

Each input emits zero or more variants:

| Action | Meaning |
| --- | --- |
| `SendMessage` | transport one typed RPC to a configured peer |
| `PersistHardState` | replace persistent term/vote state |
| `PersistLog` | replace the log suffix beginning at `from_index` |
| `RoleChanged` | publish a role transition and leader hint |
| `CommitAdvanced` | publish a monotonic commit-index range |
| `ApplyEntry` | apply one committed entry in exact index order |
| `ProposalRejected` | reject a proposal with an optional leader hint |

Persistence actions precede every dependent outbound response or replication
message. This is an ordering specification, not durability: Phase 6 emits the
intent and continues synchronously. Phase 8 must wait for the appropriate WAL or
hard-state flush completion before releasing the dependent network action.
Until that gate exists, Phase 6 must not be wired directly to a production peer
transport.

## Replication and repair

A follower accepts AppendEntries only when the previous index exists and its
term matches. Missing indexes return `last_log_index + 1`; a term mismatch
returns the first local index with that conflicting term. Entry indexes must be
contiguous, entry terms must not decrease, and no entry term may exceed the
leader term.

The follower retains every identical prefix, rejects same-index/same-term entries
whose bytes differ, and replaces the first conflicting uncommitted suffix. It
never replaces an entry at or below its commit index. Repeating an identical
AppendEntries changes no log bytes but returns success again.

Follower commitment is bounded by all three values:

```text
new commit <= leader_commit
new commit <= local last_log_index
new commit <= last index matched by this exact AppendEntries RPC
```

The final bound is important: a delayed heartbeat that matches an earlier prefix
cannot commit a divergent local suffix that the leader did not prove.

Leaders correlate each response with the newest RPC sent to that follower.
Delayed or reordered older responses cannot move `match_index` or `next_index`.
A valid success advances progress monotonically and a rejection uses its bounded
hint to send the missing suffix immediately.

## Commitment and application

The leader counts itself plus follower match indexes. It may advance commit by
quorum only to an entry from its current term. When that entry commits, preceding
entries from older terms commit transitively. A minority can replicate but
cannot commit.

Commitment emits one `CommitAdvanced` action followed by gap-free `ApplyEntry`
actions and advances `last_applied` synchronously. The leader then broadcasts
its new commit index. A follower similarly applies only through the commit index
proven by its accepted AppendEntries.

The runtime validates internal invariants after construction and every public
input: sentinel integrity, gap-free indexes, nondecreasing terms, vote/leader
membership, commit/apply bounds, candidate self-vote, and leader progress bounds.

## Phase boundary

Phase 6 deliberately does not include:

- durable hard state or log recovery (Phase 8);
- a deterministic cluster scheduler, partitions, crashes, or randomized model
  histories (Phase 7);
- peer encoding or TCP transport (Phase 9);
- ReadIndex, snapshot installation, or deduplicated client commands (later
  phases).

Its tests cover simultaneous split votes, re-election, higher-term failover,
leader loss, stale terms, vote freshness, conflict repair, duplicate append,
minority and majority replication, previous-term commitment, old-leader rejoin,
heartbeats, malformed logs, and delayed/reordered responses.
