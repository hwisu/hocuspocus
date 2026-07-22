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
| Ktor | Application plugin, configurable WebSocket route, end-to-end bounded frame/transport queues, bounded application-stop flush/close, structured request context |
| Direct access | Typed server-side YKS transactions using the same change/store lifecycle; no managed native-document getter |
| Multi-node | Redis pub/sub initial/live sync, awareness/stateless propagation, bounded queues, retry/fail-closed unload, loop prevention, and store lock |
| Operational extensions | Bounded throttle, low-cardinality metrics, signed webhook with optional Node JSON mode, S3 safe/default and Node-legacy keys, and SQLite persistence |

`Document.isEmpty(fieldName)` is exact for missing, concretely opened, remotely
unopened, deleted-only, and map-only roots. The adapter delegates to YKS's
type-neutral structural query, which matches Yjs `_start`/`_map` semantics
without guessing the root type.

## Non-goals

- Rewriting the browser provider in Kotlin.
- Emitting private YKS envelopes to JavaScript Yjs clients.
- Reproducing Node HTTP server APIs when Ktor already owns that boundary.
- Reproducing the console-oriented extension logger when Ktor `CallLogging`,
  `System.Logger`, and structured metrics own that operational boundary.
- Pretending byte-for-byte update identity is required: Yjs-compatible updates
  may differ in encoding while converging to the same state and state vector.

## Verification

The executable interoperability oracle uses the pinned official
`@hocuspocus/provider` 4.4.0 package and connects two independent Y.Doc clients to the Ktor
server. It verifies session-aware routing, provider-version authentication,
token refresh, sync acknowledgement, cross-provider Yjs updates (including
non-BMP text), awareness, and stateless broadcast.

JVM protocol and integration tests cover the remaining server-only contracts:
malformed and oversized codec inputs, read-only rejection, authentication and
queue limits, single-flight document loading, save/reload, shutdown flushing,
unload-veto retry, extension failure isolation, and coroutine ownership.
Committed Kotlin ABI dumps cover every published JVM library module. The
compatibility target is wire behavior and lifecycle semantics; Node HTTP
server ownership is deliberately supplied by Ktor and the host application.

`upstream-server-test-matrix.json` records every server test file and scenario
count from the pinned 4.4.0 source release. The verifier binds that snapshot to
the exact npm server version and fails on duplicate or invalid classifications,
missing targets, total scenario drift, or a reduced JVM contract-test floor.
Updating the upstream version requires updating this explicit snapshot.
Node `address`, `listen`, `onRequest`, `onUpgrade`, and `onListen` behavior is
an intentional Ktor-native adaptation, not a missing wire feature. JVM
contract tests separately exercise multiplexed session IDs, hook failure
order, stateless opcode isolation, concurrent load failure, store priority,
provider route close, and token-refresh failure.

The matrix is deliberately not presented as 214 one-to-one JVM tests. Several
upstream tests repeat configuration-level assertions around one lifecycle
invariant, so the JVM suite groups them into contract tests. The matrix proves
that every source scenario has an owner and that its contract-test target
cannot silently disappear; semantic equivalence remains backed by those JVM
tests plus the real Provider/Yjs interoperability oracle.

`pnpm benchmark:jvm:ab` adds a separate core performance contract. It alternates
upstream Node and Ktor/YKS processes, drives both with the same pinned Provider
v4 and Y.Doc workload over real loopback WebSockets, verifies convergence, and
records connection time, p50/p95/p99 fanout completion, burst throughput,
server CPU, and RSS. `pnpm benchmark:jvm:infra-ab` separately runs real SQLite
files, homogeneous Redis pairs, Node→JVM→Node SQLite migration, and a
simultaneous Node+JVM Redis topology. The mixed topology verifies initial/live
sync, awareness, stateless messages, persistence, and reconnect. The required
compatibility phase is a separate CI gate; infrastructure performance ratios
remain platform-sensitive. With native KQueue, core CPU efficiency remains
1.458x to 4.231x Node depending on workload and is kept as an explicit failing
gate.

The published Node 4.4.0 server does not consistently apply awareness removal
tombstones received from a JVM peer through Redis. The mixed-runtime gate
therefore enforces removal in the Node-to-JVM direction; explicit JVM client
tombstones and JVM-to-JVM removal remain covered by the Provider oracle and
Redis integration tests. A rolling migration must drain Node WebSocket routes
before shifting their documents to JVM instances so stale Node-side awareness
cannot outlive the old sockets.

The wire boundary is compatible. The pinned YKS engine packs standard text
content, maintains indexed sequence access, and rejects non-standard local
transactions atomically. The JVM server still applies a conservative
`maxCrdtUpdateSize` admission limit as an independent untrusted-input boundary;
it is not a workaround for a private YKS representation. Cross-runtime
performance evidence is tracked in `PERFORMANCE.md`. The former
incremental-update, unopened-root, relative-position, and XML performance gaps
are resolved. YKS `0.2.1` also closes the former UndoManager CPU gap; there are
currently no known engine-owned blockers in `yks.todo.md`.
