# JVM compatibility target

The JVM implementation targets behavioral compatibility with
`@hocuspocus/server` 4.4.0 and the existing `@hocuspocus/provider` v4 wire
protocol. It is a Kotlin/Ktor API, not a source-level translation of the
TypeScript classes.

## Compatibility contract

| Surface | JVM target |
| --- | --- |
| Outer framing | Document routing key, optional `NUL + sessionId`, and message opcodes 0–10 |
| Authentication | v4 token subtype, optional provider version, permission denial, readonly/read-write acknowledgement, token refresh |
| Yjs sync | Sync step 1/2, updates, sync replies, sync status, genuine update V1 bytes |
| Awareness | y-protocols clocks and tombstones, JSON state, removal, connection ownership, query and broadcast |
| Stateless | Client stateless messages and server-only broadcast opcode separation |
| Ordering | Sequential processing per physical socket/document route |
| Resource limits | Frame/update bytes, document name/load/socket routes, queues, awareness entries/clients, auth and idle timeout |
| Documents | Single-flight load, shared in-memory document, update broadcast, save serialization, debounce/max-debounce, safe unload, merge/emptiness/awareness queries |
| Y.Doc options | Garbage collection plus an engine-neutral `gcFilter` metadata contract |
| Hooks | Ordered suspending Kotlin extension chain; store-only short-circuit semantics matching v4 |
| Ktor | Application plugin, configurable WebSocket route, bounded application-stop flush/close, structured request context |
| Direct access | Typed server-side YKS transactions using the same change/store lifecycle; no managed native-document getter |
| Multi-node | Redis pub/sub initial/live sync, awareness/stateless propagation, loop prevention, and store lock |
| Operational extensions | Bounded throttle, low-cardinality metrics, signed webhook, S3 and SQLite persistence |

`Document.isEmpty(fieldName)` is exact for missing and concretely opened roots.
YKS cannot yet distinguish an unopened remote root from an empty root through
its public API, so the adapter throws instead of guessing; that engine-owned
gap is tracked in `yks.todo.md`.

## Non-goals

- Rewriting the browser provider in Kotlin.
- Emitting private YKS envelopes to JavaScript Yjs clients.
- Reproducing Node HTTP server APIs when Ktor already owns that boundary.
- Reproducing the console-oriented extension logger when Ktor `CallLogging`,
  `System.Logger`, and structured metrics own that operational boundary.
- Pretending byte-for-byte update identity is required: Yjs-compatible updates
  may differ in encoding while converging to the same state and state vector.

## Verification

The executable interoperability oracle builds this repository's real
`@hocuspocus/provider` v4 and connects two independent Y.Doc clients to the Ktor
server. It verifies session-aware routing, provider-version authentication,
token refresh, sync acknowledgement, cross-provider Yjs updates (including
non-BMP text), awareness, and stateless broadcast.

JVM protocol and integration tests cover the remaining server-only contracts:
malformed and oversized codec inputs, read-only rejection, authentication and
queue limits, single-flight document loading, save/reload, shutdown flushing,
extension failure isolation, and coroutine ownership. The compatibility target
is wire behavior and lifecycle semantics; Node HTTP server ownership and
extension-specific infrastructure are deliberately supplied by Ktor and the
host application.

`jvm/upstream-server-test-matrix.json` inventories every upstream server test
file and records the expected scenario count for each file. The verifier fails
on file additions/removals, scenario-count drift, disabled/focused tests,
unclassified ownership, missing targets, or a reduced JVM contract-test floor.
Node `address`, `listen`, `onRequest`, `onUpgrade`, and `onListen` behavior is
an intentional Ktor-native adaptation, not a missing wire feature. JVM
contract tests separately exercise multiplexed session IDs, hook failure
order, stateless opcode isolation, concurrent load failure, store priority,
provider route close, and token-refresh failure.

The matrix is deliberately not presented as 213 one-to-one JVM tests. Several
upstream tests repeat configuration-level assertions around one lifecycle
invariant, so the JVM suite groups them into contract tests. The matrix proves
that every source scenario has an owner and that its contract-test target
cannot silently disappear; semantic equivalence remains backed by those JVM
tests plus the real Provider/Yjs interoperability oracle.

`pnpm benchmark:jvm:ab` adds a separate performance contract. It alternates
upstream Node and Ktor/YKS processes, drives both with the same built Provider
v4 and Y.Doc workload over real loopback WebSockets, verifies convergence, and
records connection time, p50/p95/p99 fanout completion, burst throughput,
server CPU, and RSS. Latency/throughput/RSS currently pass the declared local
band; CPU efficiency does not, and the identified engine-owned allocation
hotspot is recorded in `yks.todo.md`.

The wire boundary is compatible. The pinned YKS engine packs standard text
content, maintains indexed sequence access, and rejects non-standard local
transactions atomically. The JVM server still applies a conservative
`maxCrdtUpdateSize` admission limit as an independent untrusted-input boundary;
it is not a workaround for a private YKS representation. Cross-runtime
performance evidence and its currently open incremental-update cleanup issue
are tracked in `yks.todo.md`.
