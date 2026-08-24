# ForgeKV Failure Model

Status: normative Phase 0 assumptions and responses

## 1. Scope

ForgeKV is designed for crash-stop and crash-recovery faults in a fixed
three-to-seven-node Raft cluster. It prefers safety over availability. This
document distinguishes faults the design must tolerate from failures that are
detected but require operator repair and from conditions outside the guarantee.

Recommended deployments use three or five nodes in separate failure domains.
Seven nodes are supported when an operator accepts higher quorum and replication
cost. Even node counts add failure domains but do not increase the number of
simultaneous failures tolerated by the preceding odd size.

For `N` voters, quorum size is `floor(N / 2) + 1`. The cluster can make progress
only while a quorum can communicate and persist required state.

## 2. Faults the protocol must tolerate

### 2.1 Process and host faults

- A process can terminate at any instruction without running destructors or its
  shutdown sequence.
- A host can lose power and later restart.
- Multiple nodes can crash concurrently or repeatedly.
- A process can pause for an unbounded interval because of scheduling, swapping,
  stop-the-world diagnostics, or resource starvation. Other nodes cannot
  distinguish a pause from network loss.
- Volatile memory is lost on process or host restart.

Safety must survive any number of stopped nodes. Availability requires a quorum
of operational nodes. Acknowledged writes remain recoverable after permanent
loss of fewer than a quorum of disks.

### 2.2 Network faults

- Connections can fail during connect, read, or write.
- Bytes already handed to the kernel may or may not reach the peer when a
  connection fails.
- Messages can be delayed indefinitely, duplicated by retries, or observed on a
  replacement connection after newer traffic.
- TCP can fragment or combine application frames arbitrarily.
- Partitions can isolate any topology, including an old leader in a minority.
- Bandwidth can collapse and buffers can fill while connections remain open.

TCP preserves byte order only within one live connection. ForgeKV derives no
cross-connection ordering or delivery guarantee from TCP. Raft term, index, peer
identity, and request correlation make stale or duplicated RPCs harmless.

### 2.3 Disk and filesystem faults

- A process can crash before, during, or after a write, `fdatasync`, rename, or
  directory `fsync`.
- An unflushed write can be absent, partial, or visible after restart.
- The final WAL append can be shorter than its declared record length.
- A temporary snapshot or new manifest generation can remain after a crash.
- Disk-full, quota, permission, and I/O errors can occur and must be reported.

The implementation assumes:

- a successful `fdatasync` makes preceding file data and required file metadata
  persistent according to the supported Linux filesystem contract;
- a successful directory `fsync` makes a preceding rename or file creation
  persistent;
- atomic rename has standard same-filesystem Linux semantics;
- storage does not acknowledge flush while silently discarding the data;
- memory, CPU, kernel, and cryptographic/checksum calculations are not malicious.

Checksums detect accidental corruption but do not correct it. An incomplete
final WAL record may be truncated. Any other checksum or structural failure
quarantines the node until trustworthy repair.

## 3. Faults outside the safety guarantee

- Byzantine nodes, forged peer identities, malicious operators, or a compromised
  process deliberately emitting protocol-valid lies.
- Simultaneous permanent loss or unrecoverable corruption of quorum storage.
- Filesystems or devices that violate successfully reported flush semantics.
- Undetected RAM, CPU, kernel, or DMA corruption.
- Misconfiguration that starts multiple processes with the same writable data
  directory after bypassing the exclusive lock.
- Dynamic membership changes; initial membership is fixed.
- Disaster recovery from an externally copied, inconsistent set of files.
- Confidentiality, authentication, and denial-of-service resistance beyond hard
  resource limits; transport security requires a later security design.

These exclusions must not become silent behavior. Identity mismatch, corruption,
and unsupported formats cause startup or service failure with diagnostics.

## 4. Failure response by subsystem

| Event | Required response | Client-visible result |
|---|---|---|
| Follower process crash | quorum continues if available; follower recovers and catches up | normally none |
| Leader process crash | remaining quorum elects a new leader | in-flight requests time out or receive retryable errors |
| Minority partition with old leader | old leader cannot confirm ReadIndex or commit | no successful read/write; retry or redirect when known |
| Majority partition | majority side can elect/retain leader and progress | minority unavailable |
| No quorum | preserve state; continue retrying peer links | no successful linearizable operations |
| Client disconnect after submission | operation continues; response route is discarded | outcome unknown; retry same ID |
| Peer connection replacement | reject stale responses using term and correlation | none |
| Queue high-water mark | stop reads and propagate backpressure | `BUSY`, delay, or disconnect |
| Disk full or WAL I/O error | node stops acknowledging storage-dependent RPCs; leader steps down or fails service | retryable/unavailable; never success |
| Incomplete final WAL record | truncate to last complete boundary during recovery | node rejoins after validation |
| WAL/snapshot checksum failure | quarantine and fail closed | node unavailable pending repair |
| Crash during snapshot creation | retain old published snapshot; ignore temporary file | recovery uses old snapshot + WAL |
| Crash during shutdown | use normal crash recovery | same as unexpected crash |

## 5. Critical crash points

Every boundary below requires a failure-injection test. “Crash” means immediate
termination without cleanup.

### 5.1 Vote persistence

```text
receive RequestVote
    |
update term/vote in memory
    |  CRASH: old durable state remains; no vote response was allowed
write alternate hard-state generation
    |  CRASH: checksum selects old or complete new generation
fdatasync
    |  CRASH: recovered new vote; response may have been lost
send granted response
```

The response is never sent before the flush completion.

### 5.2 Follower append

```text
validate prevLogIndex/prevLogTerm
    |
append framed entry bytes
    |  CRASH: incomplete tail ignored; no success response
fdatasync
    |  CRASH: entry is durable; success response may have been lost
notify Raft owner
    |
send AppendEntries success
```

A repeated RPC is matched by index and term and is safe.

### 5.3 Leader acknowledgment

```text
leader append durable + durable follower acknowledgments
    |
quorum including leader reached
    |
advance commit index under current-term rule
    |  CRASH: new leader recovers commitment through Raft quorum intersection
apply command and dedup record
    |  CRASH: replay is deterministic
send success
    |  CRASH: client may retry; dedup returns same result
```

### 5.4 Snapshot publication

```text
write unique temporary snapshot
    |  CRASH: temp ignored
fdatasync + verify
    |  CRASH: temp ignored
rename to final snapshot name + directory fsync
    |  CRASH: old manifest remains authoritative
publish checksummed manifest generation + directory fsync
    |  CRASH: newest valid generation selects old or new snapshot
delete covered WAL segments
```

No WAL segment is deleted before the new snapshot is authoritative.

## 6. Partition and leadership behavior

An isolated leader can retain its local role temporarily, but it cannot:

- advance commitment without durable acknowledgments from a quorum;
- complete ReadIndex without current-term quorum confirmation;
- return a successful mutation before commitment and application; or
- return a successful `GET` from local state alone.

When it observes a higher term, it immediately becomes a follower after
persisting required hard state. Responses completed before that observation are
still linearizable because their quorum evidence intersects any later election
quorum.

Election timeouts and heartbeat intervals affect availability only. Safety does
not depend on bounded delay, synchronized clocks, or clock monotonicity across
hosts. Each process uses a monotonic local clock for timeouts.

## 7. Client timeouts and retries

A timeout partitions the timeline into possibilities the client cannot
distinguish:

1. the server never accepted the request;
2. the entry is uncommitted and may later commit or be overwritten;
3. the entry committed and applied but the response was lost; or
4. the response is delayed in the network.

Therefore a timeout returns an unknown outcome. The client discovers the leader
and retries the exact command with the same `(client_id, request_id)`. It does
not issue a higher mutation ID until the current mutation has a definitive
result. ForgeKV's replicated apply-time deduplication makes such retries safe.

`GET` has no side effect and may be retried with a new correlation ID. A timed
out read yields no statement about the value at any later time.

## 8. Recovery states

```text
STARTING
   |
   v
VALIDATING_STORAGE --corrupt/mismatch--> QUARANTINED
   |
   v
RESTORING_SNAPSHOT_AND_LOG
   |
   v
RAFT_REJOINING <-------------------------+
   |                                     |
   v                                     | lost connectivity
FOLLOWER / CANDIDATE / LEADER -----------+
   |
   v
DRAINING -> STOPPED
```

`QUARANTINED` exposes diagnostics and metrics only on a local administrative
surface. It does not participate as a voter or serve client data.

Recovery uses a snapshot plus the continuous valid WAL suffix. A node never
uses the mere presence of an entry as proof of commitment. A durable commit
watermark may safely accelerate replay; otherwise the node waits for a leader's
commit index. It does not serve requests until state validation and Raft rejoin
are complete.

## 9. Availability summary

| Cluster size | Quorum | Simultaneous node/disk losses while retaining progress |
|---:|---:|---:|
| 3 | 2 | 1 |
| 4 | 3 | 1 |
| 5 | 3 | 2 |
| 6 | 4 | 2 |
| 7 | 4 | 3 |

This table describes fault count, not correlated failure safety. Nodes should be
placed across independent power, host, and storage failure domains. A cluster
may remain safe with more failures than shown but will not remain available.
