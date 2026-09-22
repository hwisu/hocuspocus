# JVM compatibility target

This project targets behavioral and wire compatibility with
`@hocuspocus/server` and `@hocuspocus/provider` `4.7.0`. It is a Kotlin/Ktor
implementation, not a TypeScript API translation.

## Contract

| Surface | JVM behavior |
| --- | --- |
| Framing | Hocuspocus opcodes, document routing, optional session IDs |
| Authentication | v4 token flow, provider version, read-only mode, token refresh |
| CRDT | Yjs update V1 sync steps, updates, replies, and status |
| Awareness | y-protocols clocks, tombstones, ownership, query, and broadcast |
| Lifecycle | Single-flight load, ordered hooks, serialized store, safe unload |
| Limits | Bounded frames, updates, queues, documents, routes, and awareness |
| Ktor | Bounded channels, request context, route and shutdown ownership |
| Multi-node | Redis sync, loop prevention, bounded queues, and store locking |

Node HTTP server APIs are intentionally replaced by Ktor. The browser provider
and wire protocol remain the interoperability boundary. Compatible CRDT states
may have different byte encodings.

## Verification

The pinned JavaScript oracle connects independent Provider/Y.Doc clients to the
Ktor server and checks authentication, session routing, sync, awareness,
stateless messages, persistence, and reconnect behavior. Nested-value coverage includes
514 container levels sent by the real Provider and 10,000-level update inspection,
persistence, reload, and adjacent text formatting through the JVM adapter. No depth
cutoff is enabled.

`upstream-server-test-matrix.json` maps the pinned `4.7.0` server and selected
Redis/S3/throttle extension scenarios to named JVM contract tests. The verifier
checks npm version and source commit, scenario totals, target files, minimum
test counts, and JUnit discovery. Several upstream assertions intentionally map
to one lifecycle contract; the matrix is not a one-test-per-assertion claim.

The `0.1.9-SNAPSHOT` source build targets YKS `0.2.15` at
`c6d53147a7a056c08289a28e9b267c1968b5f619`. Both repositories build with Kotlin
2.4.20 and Gradle 9.7.1; standalone consumers also verify Kotlin 2.3.21. YKS
`0.2.15` must be published before Hocuspocus `0.1.9`. Published Hocuspocus `0.1.8`
continues to use YKS `0.2.14`.

Hocuspocus uses only standard Yjs updates. YKS-specific lossless envelopes and
experimental Yjs 14 facades are outside this server contract.

Upstream `main` was reviewed through `0d9a7ff7` (2026-09-23). Its post-4.7.0
load-failure cleanup fix is already implemented by `loadDocument`: an unpublished
document is destroyed if either load hook throws. A regression test verifies
engine closure and a fresh retry. The upstream self-apply optimization does not
apply to the JVM hook, which returns update bytes rather than a document object.

## Non-goals

- Reimplementing the browser provider or Node HTTP APIs.
- Sending private YKS data to JavaScript clients.
- Claiming source, API, or bug-for-bug equivalence with the Node server.
- Requiring byte-identical updates when state and state vectors converge.
