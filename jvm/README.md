# Hocuspocus JVM for Ktor

This directory is a Kotlin/JVM implementation of the Hocuspocus v4 server
contract. It is designed to run inside an existing Ktor application, while the
official JavaScript `@hocuspocus/provider` remains the browser client.

The runtime targets JDK 21, Kotlin 2.2.20, and Ktor 3.5.1. Kotlin is pinned to
2.2.20 to match the current YKS binary contract; changing it independently can
break JVM string handling at the CRDT boundary.

The audited engine baseline is YKS commit
`0658cd1c125b31907fe7f12932872e153e4b3d96`. It supplies externally serialized
coroutine access, atomic standard-update enforcement, indexed structural hot
paths, type-neutral root emptiness, and Node 26 scalar-read parity. Its full
suite and strict 33-scenario Yjs performance gate pass locally. Publish that
same source as a new Maven version before consuming the adapter without the
composite checkout. The exact engine-owned distribution prerequisite is
tracked in `../yks.todo.md`.

## Modules

- `hocuspocus-protocol`: bounded lib0, Hocuspocus, sync, authentication,
  routing-key, and awareness codecs.
- `hocuspocus-core`: coroutine-safe server engine, documents, connections,
  hooks, persistence lifecycle, and direct connections.
- `hocuspocus-yks`: standard Yjs update V1 document engine backed by YKS.
- `hocuspocus-ktor`: Ktor plugin and lower-level WebSocket integration.
- `hocuspocus-redis`: Lettuce-backed live CRDT, awareness, and stateless
  synchronization with distributed store locking.
- `hocuspocus-throttle`: bounded sliding-window connection throttling.
- `hocuspocus-metrics`: low-cardinality structured lifecycle metrics.
- `hocuspocus-webhook`: signed, bounded HTTPS webhook integration.
- `hocuspocus-storage-s3`: AWS SDK v2 standard-update object storage.
- `hocuspocus-storage-sqlite`: serialized, parameterized SQLite persistence.
- `hocuspocus-benchmark`: JMH, JFR, fanout, persistence, slow-consumer, heap,
  and soak verification.
- `hocuspocus-ktor-example`: executable Netty application used by the real
  provider interoperability test.

Provider-facing CRDT traffic is always genuine Yjs update V1. The server never
sends a YKS-private serialization envelope to a JavaScript client.

## Add it to a Ktor application

Until the artifacts are published to a repository, publish this build locally:

```sh
JAVA_HOME=/path/to/jdk-21 ./gradlew \
  -Pyks.localPath=/path/to/yks \
  publishToMavenLocal
```

Then add the Ktor adapter. It exposes the core, protocol, and YKS APIs
transitively:

```kotlin
dependencies {
    implementation("ai.hocuspocus:hocuspocus-ktor:0.1.0-SNAPSHOT")
}
```

The published YKS dependency is resolved from GitHub Packages. Configure its
repository with credentials that can read `hwisu/yks`:

```kotlin
repositories {
    mavenCentral()
    mavenLocal()
    maven {
        url = uri("https://maven.pkg.github.com/hwisu/yks")
        credentials {
            username = providers.gradleProperty("gpr.user").orNull
                ?: System.getenv("GITHUB_ACTOR")
            password = providers.gradleProperty("gpr.key").orNull
                ?: System.getenv("GITHUB_TOKEN")
        }
    }
}
```

## Minimal Ktor setup

```kotlin
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.ktor.HocuspocusKtor
import ai.hocuspocus.yks.YksDocumentFactory
import io.ktor.server.application.install

val hocuspocus = HocuspocusServer(
    HocuspocusConfiguration<Unit>(
        documentFactory = YksDocumentFactory(),
        authenticator = HocuspocusAuthenticator { payload ->
            if (!verifyToken(payload.token)) throw HocuspocusAuthenticationException()
        },
        onError = { error -> logger.error("Hocuspocus failure", error) },
    ),
)

fun Application.module() {
    install(HocuspocusKtor) {
        path = "/collab"
        use(hocuspocus)
    }
}
```

The plugin installs Ktor WebSockets when needed, configures frame and heartbeat
limits from the server configuration, creates `/collab`, and calls
`hocuspocus.shutdown()` during `ApplicationStopping`, bounded by the finite
`shutdownTimeout` setting (30 seconds by default). Set
`installWebSockets = false` if the application installs and configures
`WebSockets` itself. In that case, its `maxFrameSize`, ping, and timeout values
remain the application's responsibility; keep the transport frame limit at or
below `HocuspocusConfiguration.maxFrameSize` so oversized frames are rejected
before Ktor allocates them.

For a route controlled by your own routing tree, install `WebSockets` and call
`serveHocuspocus(server, context)` from a Ktor `webSocket` block instead.

## Authentication and typed request context

The context is created once per physical WebSocket and is available to every
document routed through that socket. The dedicated authenticator runs for each
routed document. There is no permissive default: without an `authenticator`,
WebSocket authentication is rejected. Set `allowAnonymous = true` only for an
intentionally public collaboration endpoint.

```kotlin
data class RequestContext(
    val tenantId: String? = null,
    val userId: String? = null,
)

val authenticator = HocuspocusAuthenticator<RequestContext> { payload ->
    val principal = verifyToken(payload.token)
        ?: throw HocuspocusAuthenticationException()

    // The provider controls documentName, so authorization must bind it to
    // the authenticated tenant before loading or sharing any document.
    val tenantPrefix = "${principal.tenantId}:"
    if (!payload.attempt.routingKey.documentName.startsWith(tenantPrefix)) {
        throw HocuspocusAuthenticationException()
    }

    payload.attempt.context.value = RequestContext(principal.tenantId, principal.userId)
    payload.attempt.connectionConfiguration.readOnly = !principal.canEdit
}

val hocuspocus = HocuspocusServer(
    HocuspocusConfiguration(
        documentFactory = YksDocumentFactory(),
        authenticator = authenticator,
    ),
)

install(HocuspocusKtor) {
    use(hocuspocus) { RequestContext() }
}
```

Throw `HocuspocusAuthenticationException` for an expected authentication
denial. The authenticator must validate both the credential and access to
`payload.attempt.routingKey.documentName`; never authorize from a tenant header
supplied by the same unauthenticated caller. `onAuthenticate` remains available
to ordered extensions after the mandatory authenticator. Successful completion
produces the v4 `read-write`/`read-only` acknowledgement automatically.

## Persistence

Implement `DocumentStorage` with a database, object store, or key/value store,
then register `DatabaseExtension`. Stored values are complete standard Yjs V1
updates, so they can be loaded directly by YJS-compatible implementations.

```kotlin
class PostgresDocuments(private val dataSource: DataSource) : DocumentStorage {
    override suspend fun load(documentName: String): ByteArray? =
        withContext(Dispatchers.IO) { /* SELECT state ... */ }

    override suspend fun store(documentName: String, state: ByteArray) {
        withContext(Dispatchers.IO) { /* UPSERT state ... */ }
    }
}

val hocuspocus = HocuspocusServer(
    HocuspocusConfiguration<Unit>(
        documentFactory = YksDocumentFactory(),
        extensions = listOf(DatabaseExtension(PostgresDocuments(dataSource))),
        debounce = 2.seconds,
        maxDebounce = 10.seconds,
    ),
)
```

Use `ContextualDocumentStorage<C>` when persistence needs the authenticated
request context. Its load request includes the request headers/parameters,
socket id, and connection configuration; its store request includes the last
transaction context/origin and active connection count. `DocumentStorage`
remains the small document-name-only contract.

Loads are single-flight per document. Saves are serialized, debounced, bounded
by `maxDebounce`, retried on a later dirty generation, and flushed before an
unload or application shutdown. Do not perform blocking JDBC calls directly in
a hook; move them to an IO dispatcher or use a suspending database driver.
Use a tenant-qualified document name (and enforce that prefix in the
authenticator) so the raw `DocumentStorage` key cannot collide across tenants.
If shutdown persistence fails, `shutdown()` throws
`HocuspocusShutdownException`, keeps failed dirty documents in memory, rejects
new work, and may be called again after storage recovers.

## Server-side document transactions

Use a direct connection for jobs, HTTP handlers, or administrative mutations.
It shares the same document, broadcast, change hooks, debounce, and persistence
lifecycle as WebSocket updates.

```kotlin
val direct = hocuspocus.openDirectConnection("tenant-a:document-42", Unit)
try {
    direct.transactYks { ydoc ->
        ydoc.getText("body").insert(0, "server-generated text")
    }
} finally {
    direct.disconnect() // waits for the final store/unload by default
}
```

Prefer suspending `disconnect()` in coroutine code. `close()` is a blocking
`AutoCloseable` bridge and also waits for final persistence, so it is suitable
for `use` at blocking boundaries. The server-managed native YKS document is not
exposed; mutate it through the typed `transactYks` adapter so broadcast, change,
and storage hooks always run.

## Administration and shutdown

The server exposes detached, concurrency-safe management reads instead of its
mutable document map:

```kotlin
val healthy = hocuspocus.isStarted && !hocuspocus.isClosed
val loadedDocuments = hocuspocus.documentsCount
val physicalConnections = hocuspocus.connectionsCount
val documentNames = hocuspocus.documentNames()
val document = hocuspocus.document("tenant-a:document-42")
```

The document wrapper also provides the v4 convenience semantics without
exposing mutable engine internals:

```kotlin
val empty = document?.isEmpty("body")
val hasPresence = document?.hasAwarenessStates()
val clientIds = document?.connections()?.firstOrNull()?.let {
    document.getClients(it)
}
targetDocument.merge(sourceDocument)
```

`isEmpty` is exact for missing, concretely opened, remotely unopened,
deleted-only, and map-only roots. It uses YKS's type-neutral structural query
and does not guess a root type.

Configure the Y.Doc garbage collector through engine-neutral struct metadata:

```kotlin
val options = CrdtDocumentOptions(
    garbageCollection = true,
    garbageCollectionFilter = { struct ->
        struct.kind != CrdtStructKind.Item || struct.length < 1_000
    },
)
```

`closeConnections(documentName)` gracefully closes only WebSocket routes for
that document; omit the name to close all WebSocket routes. It intentionally
does not close direct server connections. `flushPendingStores()` waits for
in-flight mutations and attempts every dirty document. `shutdown()` rejects new
work, closes sessions and direct connections, flushes persistence, unloads
documents, then destroys extensions. Both `flushPendingStores()` and
`shutdown()` aggregate persistence failures instead of silently dropping them.

## Extension migration

`HocuspocusExtension<C>` is the suspending Kotlin equivalent of the v4 server
hook chain. Extensions run from highest `priority` to lowest.

| TypeScript v4 boundary | Kotlin/Ktor boundary |
| --- | --- |
| `onConfigure` | `HocuspocusExtension.onConfigure` |
| `onConnect`, `onAuthenticate`, `connected`, `onTokenSync` | Same named suspending hooks |
| `onCreateDocument`, `onLoadDocument`, `afterLoadDocument` | Same document lifecycle hooks |
| message, sync, awareness, stateless hooks | Same semantic hooks with typed payloads |
| `onChange`, store, disconnect, unload, destroy | Same semantic lifecycle hooks |
| Node `onRequest`, `onUpgrade`, `onListen` | Ktor plugins, routing, authentication, and engine lifecycle |
| extension database types | `DocumentStorage` plus `DatabaseExtension`, or a custom extension |
| console `extension-logger` | Ktor `CallLogging`, `System.Logger`, and `StructuredMetricsExtension` |

Throw `SkipFurtherHooksException` only from `onStoreDocument` or
`afterStoreDocument`, after a higher-priority extension has durably handled the
current store generation. In every other hook it is an operation failure and
propagates normally.

## Production limits

`HocuspocusConfiguration` provides explicit bounds for frame and CRDT update
size, document-name length, loaded documents, documents per physical socket,
unauthenticated queued bytes/messages/documents, established per-document
queues, awareness update bytes/entries/client identities, authentication/idle
timeout, and awareness expiry. The Ktor transport also bounds its outbound queue with
`outboundQueueCapacity` and `outboundQueueByteCapacity`. Keep these finite and
size them for your largest legitimate document update. The CRDT update limit is
an untrusted-input and capacity boundary independent of the YKS internal
representation. Keep it at or below the Ktor WebSocket frame limit.

Awareness tombstone clocks are retained indefinitely by default, matching
`y-protocols` and preventing stale presence from being resurrected. Tombstones
count toward `maxAwarenessClientsPerDocument`, which bounds forged metadata.
Setting a finite `awarenessMetadataRetention` trades exact long-lived clock
compatibility for a smaller metadata footprint and should be done only with a
known upper bound on offline client lifetime.

Apply origin allowlists, per-IP/per-tenant connection and message rate limits,
TLS, and proxy request-size limits at the Ktor or ingress boundary. The core
server validates protocol shape and per-document/socket resource limits; it
does not infer a trusted browser origin or deployment-specific client identity.
Unhandled background failures use `System.Logger` by default. Production
applications should supply `onError` to their structured logger/telemetry; an
exception thrown by that callback is isolated and cannot interrupt protocol
cleanup or connection termination.

One `HocuspocusServer` instance is an in-process collaboration node. A
multi-node deployment can add `RedisExtension(redisUri)`. It subscribes per
loaded document, performs a blocking initial peer sync when another node is
already subscribed, propagates standard CRDT/awareness/stateless messages
without loops, and coordinates shared persistence with a TTL-bounded Redis
lock. `DocumentStorage` remains necessary for durable state.

```kotlin
val sqlite = SQLiteDocumentStorage(
    SQLiteStorageConfiguration(database = "/var/lib/app/collaboration.sqlite"),
)

val server = HocuspocusServer(
    HocuspocusConfiguration<Unit>(
        documentFactory = YksDocumentFactory(),
        authenticator = authenticator,
        extensions = listOf(
            ThrottleExtension(),
            RedisExtension("redis://redis:6379"),
            StructuredMetricsExtension(metricsSink),
            DatabaseExtension(sqlite, closeOnDestroy = true),
        ),
    ),
)
```

Use `S3DocumentStorage` in the same `DatabaseExtension` position for object
storage. Its default key encoder base64url-encodes document names, its reads and
writes are size-bounded, and AWS credentials come from the SDK provider chain
instead of source configuration. SQLite uses prepared statements, serializes
one JDBC connection, and enables WAL for file databases.

`ThrottleExtension` trusts the Ktor socket address by default, not
`X-Forwarded-For`; inject an address resolver only after configuring a trusted
proxy chain. `StructuredMetricsExtension` omits document-name labels by
default. `WebhookExtension` requires HTTPS, a nontrivial HMAC secret, bounded
request/response sizes, bounded pending debounce count and bytes, no redirects,
and explicit allowlists before forwarding request headers or parameters.

## Build and verification

Use the sibling YKS checkout as a Gradle composite during development:

```sh
JAVA_HOME=/path/to/jdk-21 ./gradlew \
  -Pyks.localPath=/path/to/yks \
  check :hocuspocus-ktor-example:installDist
```

Run the real JavaScript Provider v4 oracle against the Ktor server:

```sh
JAVA_HOME=/path/to/jdk-21 \
YKS_LOCAL_PATH=/path/to/yks \
pnpm test:jvm:interop
```

Compare upstream Node Hocuspocus and Ktor/YKS with the same built Provider v4
workload over real loopback WebSockets:

```sh
JAVA_HOME=/path/to/jdk-21 \
YKS_LOCAL_PATH=/path/to/yks \
pnpm benchmark:jvm:ab
```

Pass `-- --quick` for a one-repetition diagnostic run, or `-- --check` to
enforce the documented latency, throughput, CPU, and RSS bands. The current
core check intentionally remains red only on server CPU; the former
engine-owned incremental-update hotspot is resolved, and current JFR evidence
places the remaining gap in the complete Ktor/Netty/coroutine path.

Compare real SQLite persistence and two-node Redis pub/sub separately:

```sh
JAVA_HOME=/path/to/jdk-21 \
YKS_LOCAL_PATH=/path/to/yks \
pnpm benchmark:jvm:infra-ab -- --check
```

This infrastructure gate currently passes. CPU is still reported but not
gated for these shorter process intervals.

Check every locked production runtime coordinate against OSV:

```sh
pnpm test:jvm:security
```

Build-only Kotlin plugin configurations are excluded from this runtime gate;
they do not ship in the published modules.

The oracle uses the repository's built `@hocuspocus/provider`, two independent
Y.Doc instances, session-aware multiplexing, provider-version authentication,
token refresh, non-BMP text, sync acknowledgement, awareness, and stateless
broadcast. It then closes every client, waits for store/unload, reconnects a new
Provider, and verifies the persisted Yjs state. Unit and integration tests
additionally cover codec rejection, read-only updates, queue limits,
authentication timeout, single-flight loading, persistence/reload, shutdown
flushing, and coroutine ownership.

See [`../JVM_COMPATIBILITY.md`](../JVM_COMPATIBILITY.md) for the exact
compatibility boundary and [`PERFORMANCE.md`](PERFORMANCE.md) for the JMH/JFR
method and soak commands.
