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
pnpm benchmark:jvm:ab
```

This starts the upstream Node Hocuspocus server and the Ktor/YKS server as
separate processes, alternates their order, and drives both with the same built
Provider v4 and Y.Doc workload. Each scenario verifies document convergence
and records connection time, fanout p50/p95/p99, burst throughput, server CPU,
and process RSS. Use `-- --quick` only for diagnostics and `-- --check` to make
the declared comparison bands executable.

The default A/B target is anonymous and installs no application hooks,
persistence, or Redis extension on either server. It isolates WebSocket,
protocol, CRDT-apply, and fanout cost; it is not evidence that Node and JVM
storage or Redis deployments have equal capacity.

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
OpenJDK 21.0.11, the repository's built Provider v4, and the clean local YKS
revision recorded below. Values are medians across alternating target order:

| Scenario | Node p95 / p99 | JVM p95 / p99 | Node / JVM burst ops/s | Node / JVM workload CPU |
| --- | ---: | ---: | ---: | ---: |
| 10 clients, 128 B | 0.282 / 0.534 ms | 0.737 / 2.652 ms | 11,809 / 11,094 | 30 / 350 ms |
| 100 clients, 128 B | 3.159 / 3.614 ms | 3.615 / 4.905 ms | 1,422 / 1,605 | 110 / 570 ms |
| 25 clients, 16 KiB | 3.485 / 4.401 ms | 3.233 / 3.872 ms | 1,143 / 1,105 | 40 / 250 ms |

The JVM peak RSS was 220.625 MiB versus Node's 153.344 MiB, a 1.439x ratio.
All scenarios pass the executable latency, throughput, and RSS bands. CPU does
not: the JVM used 5.18x to 11.67x the measured Node workload CPU in these
medians. The benchmark therefore exits nonzero with `--check`; this is a
visible performance gap, not a claim of full server-performance parity.

The Hocuspocus hot path was tightened before the final run:

- an inbound frame is decoded once instead of once before and once after queue
  admission;
- raw message bytes are copied only when an installed hook can observe them;
- servers without extensions no longer launch empty change/awareness hook
  coroutines;
- persistence debounce uses one coalescing deadline job instead of cancelling
  and recreating a coroutine job for every update.

A longer JFR workload still identified
`dev.yks.YDoc.mergeNewItemsUnobserved(Map, Map)` as the largest application
allocation site, at 9.43% of sampled allocation pressure. That cleanup occurs
inside standard-update application, so bypassing it, batching Provider updates
artificially, or relaying unvalidated bytes in Hocuspocus would violate the
engine boundary. The remaining work and its completion gate are recorded in
[`../yks.todo.md`](../yks.todo.md).

## 2026-07-17 validation record

The final bounded suite used the current Hocuspocus source and the clean local
YKS worktree at `f0c33ecb73e2a1327378b5893f0e8044ba4e2559` on the same
Apple M4 Pro and OpenJDK 21.0.11 environment:

- YKS's strict cross-runtime gate passed all 28 Yjs comparison scenarios;
  applying 5,000 structs measured 0.93x the Yjs median, applying into open
  roots measured 1.41x, and full-state encoding measured 0.04x;
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
