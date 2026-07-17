# JVM performance verification

The benchmark module separates repeatable JMH measurements from bounded
end-to-end scenarios. It covers:

- fanout to 1, 10, 100, and 1,000 connected routes;
- 1 KiB, 64 KiB, and 512 KiB updates;
- zero, 5 ms, and 25 ms persistence latency;
- outbound rejection and slow-consumer disconnect;
- weak-reference and heap checks after 1,000 document unloads;
- a duration-configurable soak that defaults to 30 minutes and recreates the
  server every 30 updates so CRDT history growth does not turn the soak into an
  artificial out-of-memory test.

Run the bounded suite:

```sh
./gradlew -Pyks.localPath=/path/to/yks \
  :hocuspocus-benchmark:performanceSmoke
```

Run the full soak:

```sh
./gradlew -Pyks.localPath=/path/to/yks \
  :hocuspocus-benchmark:soak -PsoakDuration=30m
```

Run the full JMH matrix:

```sh
./gradlew -Pyks.localPath=/path/to/yks \
  :hocuspocus-benchmark:jmh
```

Run the cross-runtime WebSocket comparison:

```sh
pnpm benchmark:jvm:ab -- --check
```

This starts the upstream Node Hocuspocus server and the Ktor/YKS server as
separate processes, alternates their order, and drives both with the same built
Provider v4 and Y.Doc workload. Each scenario verifies document convergence
and records connection time, fanout p50/p95/p99, burst throughput, server CPU,
and process RSS. Use `-- --quick` only for diagnostics.

Run the separate infrastructure comparison:

```sh
pnpm benchmark:jvm:infra-ab -- --check
```

It uses real SQLite files for write/unload/reconnect/read and launches a real
Redis 7.4 container for two-node pub/sub convergence. It compares the upstream
Node SQLite/Redis extensions with the JVM SQLite/Redis implementations. The
compatibility phase additionally runs Node→JVM→Node against one SQLite file and
Node+JVM simultaneously against one Redis instance, covering initial/live
sync, awareness, stateless messages, persistence, and reconnect. The core and
infrastructure suites are separate so a fast in-memory path cannot hide
storage, migration, or multi-node behavior.

Both suites use this bounded benchmark JVM profile by default:

```text
-Xms32m -Xmx256m -XX:MaxDirectMemorySize=128m -XX:ActiveProcessorCount=4
```

Override it only through `HOCUSPOCUS_BENCHMARK_JVM_OPTS`. The JSON report
records the effective options, workload scale, raw repetitions, medians, and
gate policy.

For a bounded JFR/JMH investigation:

```sh
./gradlew -Pyks.localPath=/path/to/yks \
  :hocuspocus-benchmark:jmh \
  -PjmhQuick \
  -PjmhJfr \
  -PjmhInclude=FrameEncodingBenchmark \
  -PjmhRecipients=1000 \
  -PjmhUpdateBytes=524288
```

The recording is written to
`hocuspocus-benchmark/build/reports/jmh/profile.jfr`.

## Allocation result and optimization

An exploratory run on an Apple M4 Pro, macOS 26.5.2, and OpenJDK 21.0.11 used
one 250 ms warmup and two 250 ms measurement iterations. These short numbers
are diagnostic evidence, not capacity promises:

| 1,000 recipients, 512 KiB | Average | Allocated per operation |
| --- | ---: | ---: |
| Encode sync and frame per recipient | 51,460.326 µs | 2,097,399,246.000 B |
| Encode nested sync/frame once per routing key | 59.463 µs | 2,105,811.640 B |
| Encode one fused sync frame per routing key | 21.345 µs | 532,712.590 B |

A follow-up quick run of the transport ownership boundary used the same
1,000-recipient/512 KiB parameters:

| Shared-frame delivery model | Average | Allocated per operation |
| --- | ---: | ---: |
| Copy frame for every recipient | 11,935.887 µs | 524,844,662.667 B |
| Retain one immutable shared frame | 11.710 µs | 524,344.321 B |

JFR showed the old path dominated by `byte[]` allocation and copying. The
server now groups recipients by routing key, creates one immutable frame for
each distinct key, and the Ktor sender queues retain that immutable frame
without a per-recipient copy. A custom transport must copy only if its
downstream boundary cannot preserve immutability. The protocol codec also has
an exact-size fused sync-frame encoder, removing the intermediate nested
payload. In this diagnostic case that reduced encoding time by about 2,411x
and allocation by about 3,937x before accounting for the additional Ktor-copy
removal. Removing that transport copy reduced the isolated delivery benchmark
by about 1,019x in time and 1,001x in allocation.

The end-to-end bounded suite after routing-key sharing measured the
1,000-recipient/512 KiB case at 1.174 ms on the same machine, down from roughly
142 ms before the change. Use the full JMH settings and a real production
network path before turning these short local figures into an SLO or sizing
decision.

## Upstream Node versus Ktor/YKS

A three-repetition loopback run on the same Apple M4 Pro used Node 24.11.1,
OpenJDK 21.0.11, the repository's built Provider v4, and the local YKS
composite. Values are medians across alternating target order with the default
10x workload scale:

| Scenario | Node p95 / p99 | JVM p95 / p99 | Node / JVM burst ops/s | Node / JVM workload CPU |
| --- | ---: | ---: | ---: | ---: |
| 10 clients, 128 B | 0.204 / 0.659 ms | 0.327 / 2.346 ms | 11,762 / 12,301 | 260 / 1,200 ms |
| 100 clients, 128 B | 3.230 / 3.808 ms | 3.460 / 4.000 ms | 1,495 / 1,514 | 970 / 1,400 ms |
| 25 clients, 16 KiB | 2.402 / 3.637 ms | 2.706 / 3.635 ms | 1,230 / 1,291 | 340 / 920 ms |

JVM/Node p95 ratios were 1.603x, 1.071x, and 1.127x; throughput
ratios were 1.046x, 1.013x, and 1.050x. Median peak RSS was 320.891
MiB for the JVM and 353.797 MiB for Node. Latency, throughput, convergence,
and RSS pass. CPU remains red at 4.615x, 1.443x, and 2.706x, so the
core `--check` correctly exits nonzero instead of claiming complete efficiency
parity.

Two bounded transport experiments were measured and rejected. Replacing Netty
with Ktor CIO increased quick-run CPU and latency, while bypassing the
per-route actor moved CRDT work onto the Netty event loop and reduced the small
and large workload throughput. Neither experiment is present in the final
source; the isolated route actor and Netty engine remain the safer measured
choice.

The optimized path now:

- parses the outer frame as a bounded view and decodes each nested payload
  once;
- transfers Ktor's freshly materialized inbound `ByteArray` without the
  additional `Frame.readBytes()` copy;
- copies raw bytes only when an installed message hook can observe them;
- indexes actual hook overrides once, avoiding no-op default-interface calls
  for every extension and event;
- serializes YKS access with a JVM lock that permits safe sequential
  dispatcher handoff without coroutine-mutex continuation overhead;
- shares immutable fanout frames and coalesces persistence deadlines.

The original YKS `mergeNewItemsUnobserved` hotspot is gone after the sibling
engine optimization. A follow-up JFR no longer attributes the dominant
application allocation to that method. Its largest server/runtime sites are
Netty promise creation and coroutine scheduling; the remaining YKS sample is
standard string decoding while applying updates. The removed Ktor
`Frame.readBytes()` copy no longer appears. This evidence assigns the remaining
CPU gap to the complete Ktor/Netty/coroutine process path, not to a known YKS
algorithmic failure.

## Real SQLite and Redis A/B

The final three-repetition infrastructure run passed its executable gate:

| Workload | Node | JVM | JVM/Node |
| --- | ---: | ---: | ---: |
| SQLite writes | 1,097 docs/s | 1,464 docs/s | 1.335x |
| SQLite reconnect reads | 2,335 docs/s | 1,871 docs/s | 0.802x |
| Redis p95 / p99 | 1.421 / 1.539 ms | 1.730 / 2.483 ms | 1.217x / 1.613x |
| Redis updates | 6,193/s | 17,350/s | 2.802x |
| Redis peak RSS | 579.719 MiB | 366.000 MiB | 0.631x |

SQLite CPU was 6.769x and Redis CPU was 2.357x Node in these short
process samples. They are reported but deliberately not gated until the
infrastructure intervals are long enough for stable CPU accounting. Functional
coverage uses actual file persistence, unload/reconnect, two independent JVM
or Node server processes, Redis pub/sub, and exact final Y.Doc convergence.
The mixed-runtime phase also passed Node→JVM and JVM→Node SQLite reads, both
directions of Redis initial/live sync, awareness add/removal, server stateless
messages, reconnect, and simultaneous Node+JVM Redis/SQLite store locking with
one durable converged state.

## 2026-07-17 validation record

The final bounded suite used the current Hocuspocus source and the local
YKS `0.2.0` commit `37703a1269ead28e38632b73093953621262cb6d` on the same
Apple M4 Pro and OpenJDK 21.0.11 environment:

- YKS's strict cross-runtime gate passed all 35 Yjs comparison scenarios,
  including the exact 1,000 sequential-update workload that originally exposed
  the cleanup hotspot;
- fanout covered 1, 10, 100, and 1,000 recipients with 1 KiB, 64 KiB, and
  512 KiB updates;
- the 1,000-recipient/512 KiB case completed in 1.174 ms and represented
  524,326,000 wire bytes;
- persistence latency cases completed in 2.106 ms, 8.015 ms, and 29.090 ms
  for zero, 5 ms, and 25 ms injected latency;
- all 10 slow consumers were disconnected;
- 1,000 unloaded documents retained zero weak references in the bounded heap
  check;
- the five-second smoke completed 10,407 operations and 347 server lifecycle
  cycles with 100 clients.

The final 30-minute soak completed successfully:

```text
duration=30m operations=3827295 cycles=127577 clients=100
BUILD SUCCESSFUL in 30m 10s
```

These results found no server-side regression or retained-document signal in
the covered local paths. They do not measure TLS, kernel socket buffers,
internet latency, Redis cluster failure behavior, or production storage
latency. At 1,000 recipients and 512 KiB, unavoidable network fanout is about
500 MiB for one update, so network bandwidth and slow-consumer policy remain
the practical production limits even though server-side frame copying is
removed.
