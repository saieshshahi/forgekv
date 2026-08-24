# ADR 0004: Strict Quorum-Fsync Durability Before Acknowledgment

- Status: Accepted
- Date: 2026-08-24

## Context

Raft's safety rules require persistent term, vote, and log state, but a product
must separately define what a successful client response means. A replication
acknowledgment issued before disk flush can reduce latency while allowing a
cluster-wide power interruption to erase a write the client believed durable.
That ambiguity is unacceptable for ForgeKV's default contract.

## Decision

ForgeKV has one initial durability mode: **strict quorum fsync**.

A leader may send a successful `PUT` or `DELETE` response only after all of the
following are true:

1. the complete command is appended to the leader WAL;
2. the leader WAL flush covering that entry has succeeded;
3. enough followers have appended and flushed the entry that the durable copies,
   including the leader, form a voting quorum;
4. Raft's current-term commitment rule permits `commitIndex` to cover the entry;
5. the leader apply thread has applied the entry in order, including its
   deduplication record; and
6. the response outcome has been produced from that application.

```text
                         leader append
                              |
                         leader fdatasync
                              |
             +----------------+----------------+
             |                                 |
       follower append                   follower append
             |                                 |
       follower fdatasync                 follower fdatasync
             |                                 |
       durable ACK                         durable ACK
             +----------------+----------------+
                              |
                       quorum including leader
                              |
                    commit -> apply -> respond
```

The leader can pipeline replication and local I/O and can group multiple entries
in one flush. It cannot count its own entry before its flush completes. A
follower's successful AppendEntries response means the acknowledged prefix is
durable locally; receipt or buffered write is insufficient.

## Meaning of durable

Given the failure assumptions in `docs/failure-model.md`, an acknowledged
mutation survives:

- crash and restart of any process;
- simultaneous power loss and later restart of all nodes with intact disks;
- permanent loss of fewer than a quorum of node disks; and
- loss of the acknowledging leader followed by election of another node.

The quorum intersection property ensures at least one durable copy of every
committed entry participates in any later successful election, and Raft's voting
rules preserve leader completeness.

This contract does not cover loss/corruption of quorum disks, Byzantine nodes,
or devices/filesystems that falsely report flush success.

## Uncommitted writes

An entry that has not committed:

- is never applied to user state;
- is never reported successful;
- may remain in one or more WALs after a crash;
- may later be committed if a leader containing it is elected; or
- may be overwritten if a different valid log wins.

Therefore a timed-out request can still commit. The client must treat timeout as
unknown and retry the same logical request ID. A new request ID is a distinct
operation.

## Disk errors

Any append, flush, manifest, or hard-state error prevents the dependent protocol
or client acknowledgment. A leader that cannot make its own persistent state
safe stops accepting writes and steps down or transitions to unavailable. A
follower with a persistent storage error stops sending successful replication or
vote responses. Neither substitutes an in-memory acknowledgment.

## Group commit

The WAL worker may group adjacent pending entries into one flush. Completion for
index `i` means every required byte through `i` was included in a successful
flush. Grouping has a configured maximum delay so low-volume traffic is not
deferred indefinitely. Metrics distinguish queue time, write time, and flush
time.

Group commit changes latency and throughput, not the acknowledgment guarantee.

## Reads

`GET` does not require a new WAL record or disk flush. Its durability/consistency
contract comes from a quorum-confirmed ReadIndex and waiting for application
through that index. If current leadership cannot be confirmed, no successful
value is returned.

## Alternatives considered

### Leader fsync only

Rejected because leader disk loss after acknowledgment can lose the only durable
copy even though followers had the entry in volatile buffers.

### Quorum receive/write without fsync

Rejected as the default because correlated power loss can erase every volatile
copy.

### Acknowledge at Raft commit, apply later

Raft commitment is enough to preserve ordering, but responding before leader
application complicates read-your-write expectations, duplicate results, and
error reporting. Rejected initially; ForgeKV waits through application.

### Configurable relaxed durability

Deferred. Multiple modes make benchmarks and operator expectations easy to
misread. If later added, a relaxed response must use a visibly distinct
configuration and documented response guarantee; it must never be called durable
under the strict definition.

## Consequences

Every mutation pays quorum replication and stable-storage latency. Group commit
can amortize flush cost, but tail latency will depend on the slowest node needed
for the current quorum and the leader's disk. In return, the client success
contract is unambiguous and recovery does not rely on shutdown having run.

## Required evidence

Tests must crash leader and followers at every acknowledgment boundary and verify
that no successful history loses a write. Tests must include full-cluster process
restart, delayed and duplicated replication responses, a follower that responds
only after flush, disk-full and flush errors, leadership change before response,
and response loss followed by same-ID retry.
