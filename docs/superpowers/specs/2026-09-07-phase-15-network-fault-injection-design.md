# Phase 15: Network Fault Injection Design

Status: approved (standing user approval for the recommended design)

## Objective

Measure ForgeKV under realistic kernel-level latency, jitter, packet loss, and
packet reordering without weakening the deterministic Phase 14 chaos harness or
modifying the host's normal network configuration.

The first required experiment matrix is:

- latency: 0, 10, 50, and 100 milliseconds;
- packet loss: 0%, 0.1%, 1%, and 5%;
- explicit jitter and reordering smoke profiles.

Every experiment runs the real server processes, clients, Raft transport,
storage, and recovery checks. Results are evidence, not a production-readiness
claim.

## Fault semantics

ForgeKV exposes three deliberately separate concepts:

1. **Partition**: the Phase 14 directed proxy closes existing connections and
   refuses traffic until healed. This models persistent loss of reachability.
2. **Connection reset**: the Phase 14 seeded proxy may discard an application
   chunk and close that TCP stream. This is deterministic and replayable, but it
   is not packet loss. Existing serialized `set_loss` actions remain readable;
   documentation and new user-facing output call this a connection reset fault.
3. **Packet impairment**: Linux `tc netem` drops, delays, or reorders IP packets.
   TCP may retransmit them, so a dropped packet need not fail the request. This
   mode is realistic but scheduling and retransmission timing are not bit-for-bit
   replayable.

The distinction is part of the public contract. Reports must never label proxy
connection resets as kernel packet loss or label finite packet loss as a
partition.

## Architecture

### Stable workload mode

`forgekv-chaos --no-chaos` runs the existing concurrent client workload and all
end-of-run consistency, convergence, log, and durable-restart checks, but does
not schedule Phase 14 process or proxy faults. This provides the stable workload
used by the netem experiment runner. It remains seed-controlled and emits the
normal bounded artifacts.

### Disposable namespace runner

`scripts/run-netem-matrix.sh` is the privileged experiment boundary. A normal
invocation re-executes through a documented root mechanism; CI or operators may
invoke the privileged form directly. For each profile it:

1. creates a uniquely named Linux network namespace;
2. enables only that namespace's loopback device;
3. applies a root `netem` qdisc to that isolated loopback device;
4. runs `forgekv-chaos --no-chaos` inside the namespace;
5. captures the installed qdisc statistics and campaign result;
6. removes the qdisc and namespace through an unconditional cleanup trap.

The entire cluster is inside one namespace. Impairment therefore covers client,
admin, proxy, and Raft traffic. This makes the measured effect an end-to-end
service result. Per-directed-link fault composition remains the job of the
Phase 14 proxy; kernel netem is intentionally node-agnostic in this first safe
implementation.

### Profiles and precision

Profiles use integer microseconds for delay/jitter and integer basis points for
loss/reordering. One basis point is 0.01%, so 0.1% is represented exactly as 10.
Bounds are validated before any privileged command runs:

- delay and jitter: 0 through 60 seconds;
- loss and reorder: 0 through 10000 basis points;
- correlation: 0 through 10000 basis points;
- duration, clients, nodes, executable, and artifact paths use the chaos CLI's
  existing limits plus canonical-path validation.

The command emitted to `tc` is assembled from validated numeric fields and
fixed tokens. User text is never evaluated as shell syntax.

## Safety and privilege boundary

The runner must fail closed unless all of these are true:

- the effective user is root for the privileged portion;
- `ip` and `tc` are available;
- the namespace name has the fixed `fkv-netem-` prefix and a conservative
  character set;
- the target device is exactly `lo` inside the newly created namespace;
- the namespace did not exist before this run;
- the server binary is a regular executable file;
- the output directory is not `/`, a home directory, or the repository root.

The runner never executes `tc` against a device in the default network
namespace. It never mutates `eth0`, a bridge, a physical device, or host
loopback. Cleanup targets only the exact namespace created by the current
process. A signal triggers the same cleanup trap.

If privilege or kernel support is unavailable, the runner reports an explicit
unsupported result. Unit tests still validate parsing, profile generation, and
semantic labeling; the privileged integration test skips rather than silently
substituting proxy behavior.

## Result format

The output root contains:

- `environment.txt`: kernel, `tc`, `ip`, CPU, build, and exact invocation;
- `results.jsonl`: one bounded record per profile;
- `<profile>/`: the retained chaos artifacts and qdisc statistics;
- `summary.md`: a compact human-readable table.

Each result records profile name, fault kind, numeric parameters, seed, nodes,
clients, intended duration, wall duration, exit status, attempts,
acknowledged mutations, actions, convergence, durable-restart verification,
qdisc packet/drop counters, and diagnostic. An unsuccessful ForgeKV campaign is
recorded before the matrix runner returns nonzero; it is not erased by cleanup.

The runner executes profiles serially. Each profile gets a fresh namespace and
fresh data directory, preventing qdisc state, TCP state, or persisted cluster
state from leaking between samples.

## Experiment matrix

The default quick matrix contains seven unique profiles:

| Profile | Kernel impairment |
| --- | --- |
| baseline | none |
| latency-10ms | 10 ms delay |
| latency-50ms | 50 ms delay |
| latency-100ms | 100 ms delay |
| loss-0.1pct | 0.1% packet loss |
| loss-1pct | 1% packet loss |
| loss-5pct | 5% packet loss |

Optional smoke profiles cover delay with jitter and packet reordering. Phase 16
will add warm-up, repeated trials, variance, workload mixes, and fuller resource
measurement; Phase 15 establishes correct fault semantics and a safe real-kernel
test path.

## Correctness and test strategy

Test-driven implementation starts with failing tests for:

- stable workload mode schedules zero chaos actions but still verifies
  convergence and durable restart;
- basis-point formatting, bounds, and unambiguous semantic labels;
- refusal to target the default namespace or a non-loopback interface;
- cleanup after success, ForgeKV failure, and TERM/INT;
- exact result parsing and bounded artifact records;
- baseline, latency, loss, jitter, and reorder qdisc installation in a disposable
  namespace when the test host supports it;
- the required end-to-end latency/loss matrix against the real 3-node cluster.

The final gate includes focused unit/integration tests, the full debug and
release suites, ASan, UBSan, TSan, `git diff --check`, a source review, and real
matrix evidence from this environment. Expensive matrix runs may be explicitly
gated in ordinary CI, but Phase 15 completion requires one recorded local run.

## Known limits and future work

- Netem timing is realistic but not deterministic replay. The profile and seed
  reproduce configuration, not exact packet decisions or scheduling.
- Loopback impairment affects every traffic class in the isolated cluster.
  Selective client-versus-Raft and per-directed-peer kernel shaping would require
  multiple namespaces/veth devices or classified qdiscs and is deferred until a
  measured need justifies that complexity.
- Netem does not model bandwidth caps, MTU faults, duplication, bit corruption,
  asymmetric routing, or NIC queue behavior in this phase.
- The quick matrix is a correctness/behavior campaign, not statistically sound
  performance benchmarking. Phase 16 owns controlled trials and variance.

