# Network fault injection

Phase 15 adds real Linux kernel packet impairment around ForgeKV's multi-process
cluster. `forgekv-netem` creates a fresh network namespace for each profile,
enables only its loopback device, applies `tc netem` there, runs the stable chaos
workload, captures results, and deletes the namespace. It never attaches a qdisc
to the default namespace or a host interface.

## Three different failure models

These terms are not interchangeable:

| Model | Implementation | What happens |
| --- | --- | --- |
| Network partition | Phase 14 directed proxy | Existing streams close and traffic is refused until an explicit heal. |
| Connection reset | Phase 14 seeded proxy (`set_loss` in old timelines) | An application chunk is discarded and that TCP stream closes. It is deterministic and replayable. |
| Packet impairment | Phase 15 kernel `tc netem` | IP packets are delayed, reordered, or dropped. TCP may retransmit, so a dropped packet need not fail a request. |

Netem configuration and the workload seed are reproducible. Exact packet drops,
TCP retransmission timing, process scheduling, and election timing are not
bit-for-bit deterministic.

## Build and run

Install the Linux networking tools in addition to the normal prerequisites:

```bash
sudo apt-get install -y iproute2
./scripts/build.sh release
```

On native Linux, run the complete required matrix as root:

```bash
sudo ./build/release/chaos/forgekv-netem \
  --chaos ./build/release/chaos/forgekv-chaos \
  --server ./build/release/src/forgekv-server \
  --artifacts /root/forgekv-phase15-netem \
  --nodes 3 --clients 8 --duration 10 --seed 150015
```

From Windows PowerShell with WSL, use WSL's explicit root user. Convert the
repository path to its `/mnt/c/...` form:

```powershell
wsl -d Ubuntu -u root -- /mnt/c/path/to/forgekv/build/release/chaos/forgekv-netem `
  --chaos /mnt/c/path/to/forgekv/build/release/chaos/forgekv-chaos `
  --server /mnt/c/path/to/forgekv/build/release/src/forgekv-server `
  --artifacts /root/forgekv-phase15-netem `
  --nodes 3 --clients 8 --duration 10 --seed 150015
```

The output directory must be absent. The runner creates every missing component
with private permissions, and every existing ancestor must be root-owned and
not group/other-writable (a root-owned sticky directory such as `/tmp` is the
only writable-parent exception). Do not point the root runner into a
user-writable checkout or mounted Windows directory. `/`, the current repository
root, a home directory, any symlinked component, a missing executable, an
unknown profile, duplicate options, and non-root execution are rejected before
any namespace is created.

`forgekv-netem` is a privileged test runner, not a security sandbox. A network
namespace isolates network state; it does not isolate the filesystem or remove
root privileges from the supplied server and workload executables. Run only
trusted binaries on a dedicated development host or disposable VM/WSL instance.

The default matrix contains `baseline`, `latency-10ms`, `latency-50ms`,
`latency-100ms`, `loss-0.1pct`, `loss-1pct`, and `loss-5pct`. Select one or more
profiles with repeatable `--profile NAME`. `jitter-smoke` and `reorder-smoke`
exercise the additional kernel behaviors.

## Safety and cleanup

Namespace names are generated internally as `fkv-netem-<pid>-<index>` and
validated before use. Commands use direct argument arrays, not shell evaluation;
privileged helper paths are resolved from a fixed allowlist and never from the
ambient `PATH`.
The only accepted qdisc device is `lo`, always reached through
`ip netns exec <owned-name>`. Namespace creation and deletion are bounded but do
not honor the stop flag mid-operation; this deliberate exception closes the race
where SIGTERM could otherwise prevent cleanup. The workload does honor INT/TERM,
terminates its process group, and then runs the bounded namespace deletion.

After an interrupted or failed run, verify cleanup with:

```bash
ip netns list | grep fkv-netem-
```

No output is expected. A deletion failure is reported as an experiment failure
and retained in the evidence.

## Evidence

The output root contains:

- `environment.txt`: kernel, iproute2, CPU and compiler details, exact argv,
  executable paths, SHA-256 identities for the runner/server/workload, and the
  semantic label;
- `results.jsonl`: one bounded machine-readable record per completed profile;
- `summary.md`: attempts/second, acknowledged mutations/second, observed qdisc
  drops, convergence, and restart verification;
- `<profile>/`: full chaos artifacts, workload output, and raw qdisc statistics.

Every profile gets fresh storage and a fresh namespace. A passing profile means
the real cluster accepted concurrent traffic, reached one consistent state,
survived a complete process restart, and returned the exact acknowledged client
state under that configured impairment. This quick matrix is behavioral
evidence, not a statistically controlled performance benchmark; Phase 16 owns
warm-up, repeated trials, variance, and saturation methodology.

Executable identities are sampled before and after the matrix. A persistent
path replacement or unreadable binary invalidates every profile record. This is
an evidence-integrity check for the documented trusted-host workflow, not inode
pinning: it does not defend against a privileged actor temporarily replacing an
executable and restoring it between the two samples.

## Limits

The first implementation shapes all loopback traffic used by the stable run:
client, admin, and direct Raft connections. Stable mode deliberately bypasses
the Phase 14 application proxy so proxy queuing cannot multiply kernel delay or
turn packet impairment into a second fault model. It does not yet separate
traffic classes or apply asymmetric per-peer kernel rules. It also does not
model bandwidth limits, duplication, corruption, MTU faults, or physical NIC
queues. Those additions require evidence that their complexity answers a real
question.

## Phase 15 measured evidence

The development WSL2 host ran the optimized 3-node, 8-client matrix for three
seconds of traffic per profile with seed 150015. Every profile scheduled zero
Phase 14 actions, converged, matched the exact acknowledged client state after a
complete restart, removed its namespace, and left default-namespace loopback at
`noqueue`.

| Profile | Attempts/s | Ack mutations/s | Netem packets | Netem drops |
| --- | ---: | ---: | ---: | ---: |
| baseline | 119 | 87 | 0 | 0 |
| latency-10ms | 46 | 33 | 4,376 | 0 |
| latency-50ms | 19 | 3 | 3,362 | 0 |
| latency-100ms | 15 | 2 | 4,249 | 0 |
| loss-0.1pct | 119 | 87 | 12,586 | 12 |
| loss-1pct | 78 | 56 | 7,144 | 71 |
| loss-5pct | 34 | 23 | 4,266 | 205 |

The single short trials vary sharply with profile and include no warm-up or
variance estimate, so they are not a benchmark.
Phase 16 will correct that methodology. The meaningful Phase 15 evidence is that
the kernel installed each profile, observed the expected order of loss, and
ForgeKV preserved the checked guarantees. This final matrix was regenerated
from hardened commit `f904d94` into the private root-owned directory
`/root/forgekv-phase15-netem-f904d94`. `environment.txt` binds it to the exact
runner, chaos workload, and server binaries with SHA-256 identities.

Separate two-second, 3-node, 4-client smoke runs with seed 150016 verified the
additional behaviors. The kernel reported `delay 10ms 5ms` for `jitter-smoke`
and `delay 10ms reorder 1% 25%` for `reorder-smoke`; both converged and verified
durable restart. The permanent privileged integration test covers both the
100 ms and 5% endpoints, and a second test sends SIGTERM only after live server
children exist and verifies the owned namespace is removed.
