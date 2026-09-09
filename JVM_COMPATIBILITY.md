# JVM compatibility target

This project targets behavioral and wire compatibility with
`@hocuspocus/server` and `@hocuspocus/provider` `4.6.0`. It is a Kotlin/Ktor
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

`upstream-server-test-matrix.json` maps the pinned `4.6.0` server and selected
Redis/S3/throttle extension scenarios to named JVM contract tests. The verifier
checks npm version and source commit, scenario totals, target files, minimum
test counts, and JUnit discovery. Several upstream assertions intentionally map
to one lifecycle contract; the matrix is not a one-test-per-assertion claim.

Release `0.1.8` and the `0.1.8-SNAPSHOT` source build use the published YKS
`0.2.14` engine at `fcfa849ad8a2de6ffbffd52613a8212ed7bbf2fd`, which contains
the stack-safe value implementation. Hocuspocus uses only standard Yjs updates.
YKS-specific lossless envelopes and experimental Yjs 14 facades are outside this
server contract.

## Non-goals

- Reimplementing the browser provider or Node HTTP APIs.
- Sending private YKS data to JavaScript clients.
- Claiming source, API, or bug-for-bug equivalence with the Node server.
- Requiring byte-identical updates when state and state vectors converge.
