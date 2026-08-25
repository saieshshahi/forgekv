# Deterministic Raft Core Design

## Scope

Phase 6 implements Raft as a pure, single-owner state machine. It contains no
socket, thread, wall-clock, sleep, filesystem, or storage-engine calls. The core
consumes logical time, incoming messages, and client proposals and emits typed
actions. Phase 7 will schedule these actions in a simulator; Phase 8 will make
the persistence actions durable; Phase 9 will transport messages over TCP.

## Types and state

Node IDs, terms, log indexes, logical times, and RPC IDs are unsigned 64-bit
integers. Index 0 is an internal sentinel with term 0. Real log entries contain
an index, term, entry kind (`command` or `no-op`), and opaque command bytes.

Every node owns:

- role: follower, candidate, or leader;
- `current_term` and optional `voted_for`;
- a gap-free log;
- `commit_index` and `last_applied`;
- a seeded PRNG and logical election/heartbeat deadlines;
- candidate vote identities;
- for each follower while leader: `next_index`, `match_index`, and the newest
  sent AppendEntries RPC ID.

Configuration is immutable: self ID, unique fixed voter IDs, inclusive election
timeout range, heartbeat interval, and PRNG seed. Heartbeat interval must be
strictly below the minimum election timeout.

## Inputs and outputs

Inputs are explicit method calls:

- advance to a monotonic logical time;
- receive one typed message from a configured peer;
- propose opaque command bytes.

Each input returns an ordered list of actions:

- send a typed message to a peer;
- persist changed hard state;
- replace a log suffix in persistence;
- announce a role change;
- announce a commit-index advance;
- apply a committed entry in increasing index order;
- reject a client proposal because this node is not leader.

Persistence actions are ordering intents in Phase 6. A persist intent is emitted
before a response that depends on it. Phase 8 must gate that response on durable
completion; Phase 6 does not claim that emitting both actions is a flush.

## Elections

An election deadline is sampled from the configured inclusive range whenever a
follower receives valid leader traffic, grants a vote, or becomes follower, and
whenever a candidate starts an election. On expiration a follower or candidate:

1. increments its term;
2. becomes candidate;
3. votes for itself and emits hard-state persistence;
4. sends RequestVote containing its last log index and term.

Votes are identity sets so duplicates cannot count twice. A vote is granted only
when the request term is current, this node has not voted for another candidate,
and the candidate log is at least as up to date. Any higher-term message updates
hard state and causes step-down before that message is handled.

On quorum the candidate becomes leader, initializes follower progress, appends a
current-term no-op, and starts replication. The no-op establishes a current-term
commit point without waiting for a client mutation.

## Replication and repair

AppendEntries contains term, leader ID, previous index/term, zero or more
entries, leader commit index, and RPC ID. A follower rejects stale terms and any
request whose previous entry is absent or has a different term. Rejections carry
a next-index hint. On a match it removes only the first conflicting uncommitted
suffix, appends the replacement suffix, advances commit to
`min(leader_commit,last_log_index)`, and emits gap-free apply actions.

Identical repeated AppendEntries is idempotent. A leader accepts only a response
for the newest RPC sent to that follower, preventing delayed failures or successes
from corrupting progress. Success monotonically advances match/next indexes;
failure reduces next index using the bounded hint and immediately retries.

The leader advances commitment only when a quorum has an index and that index's
term equals the leader's current term. Older entries become committed
transitively. A minority can replicate entries but cannot commit them.

## Safety and bounds

- membership size and message source are validated;
- terms, commit index, applied index, and match indexes never decrease;
- log indexes remain gap-free and bounded by available memory in this phase;
- commands are immutable byte vectors and are never interpreted by Raft;
- no callback can re-enter the node; callers explicitly deliver emitted work;
- application is synchronous and deterministic in Phase 6, so every commit
  produces ordered apply actions and advances `last_applied` in the same step.

## Verification

Focused deterministic tests cover elections, split votes, re-election, leader
loss and rejoin, stale/higher-term traffic, log conflicts and repair, duplicate
AppendEntries, delayed and reordered responses, majority commitment, and
minority non-commitment. Tests inspect action ordering as well as state. Full
debug/release and sanitizer suites remain phase exit gates.
