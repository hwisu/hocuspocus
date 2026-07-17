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
