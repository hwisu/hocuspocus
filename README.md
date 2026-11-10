# Hocuspocus JVM for Ktor

A Kotlin/JVM implementation of the Hocuspocus v4 server contract for existing
Ktor applications. Browser clients continue to use the official JavaScript
`@hocuspocus/provider`.

## Baseline

| Component | Version |
| --- | --- |
| JVM release | `0.1.12` |
| Source build | `0.1.12-SNAPSHOT` |
| Hocuspocus / Provider | `4.7.1` |
| Source CRDT engine | YKS `0.2.17` |
| Toolchain | JDK 21, Kotlin 2.4.21, Gradle 9.8.2, Ktor 3.5.2 |

Release `0.1.12` adopts the published YKS `0.2.17` engine, including its update
merge and malformed-update fixes, and aligns lifecycle and awareness behavior
with Hocuspocus `4.7.1`. It also updates Netty, Logback, Kotlin, and Gradle security
baselines. Hocuspocus and YKS use Kotlin 2.4.21 and Gradle 9.8.2.
CI, soak, and publication workflows pin the YKS `v0.2.17` source commit.
Standalone consumers also check Norric's Kotlin 2.3.21 compiler baseline through
the Build Tools API while keeping the patched Kotlin 2.4.21 Gradle plugin.
Dependency checks cover runtime, test, build, consumer, and Gradle wrapper versions.
CI checks both standalone consumers with empty Gradle caches, and publication
checks a fresh consumer before uploading artifacts.

Published release `0.1.8` pins the published YKS `0.2.14` engine, bringing the stack-safe
value implementation already used by the source build into released artifacts.
It reduces temporary allocations in awareness cleanup, change-hook bookkeeping,
disconnect handling, and throttling, and reuses the existing lib0 byte-array
writer for strings. These cleanups preserve public APIs and runtime behavior.

Deeply nested standard Yjs values remain accepted without a new depth cutoff. Provider
interoperability includes synchronization and persisted reconnect of nested values
and adjacent text formatting with deeply nested attributes.

JavaScript packages in `package.json` are test oracles only and are not shipped
in JVM artifacts.

## Modules

- `hocuspocus-core`, `hocuspocus-protocol`, `hocuspocus-yks`: server, wire
  protocol, and Yjs-compatible CRDT engine.
- `hocuspocus-ktor`: Ktor plugin and WebSocket adapter.
- `hocuspocus-redis`, `hocuspocus-throttle`, `hocuspocus-metrics`,
  `hocuspocus-webhook`: optional operational extensions.
- `hocuspocus-storage-s3`, `hocuspocus-storage-sqlite`: optional persistence.

All library modules are published separately. Add only what the application
uses.

## Installation

```kotlin
dependencies {
    implementation("ai.hocuspocus:hocuspocus-ktor:0.1.12")
    // implementation("ai.hocuspocus:hocuspocus-redis:0.1.12")
}
```

Artifacts are in GitHub Packages. Configure credentials with `gpr.user` and
`gpr.key`, then add repositories for both `hwisu/hocuspocus` and `hwisu/yks`.
Use `mavenLocal()` with `0.1.12-SNAPSHOT` for source development.

## Minimal Ktor setup

```kotlin
val server = HocuspocusServer(
    HocuspocusConfiguration<Unit>(
        documentFactory = YksDocumentFactory(),
        authenticator = HocuspocusAuthenticator { payload ->
            if (!verifyToken(payload.token)) {
                throw HocuspocusAuthenticationException()
            }
        },
        onError = { error -> logger.error("Hocuspocus failure", error) },
    ),
)

fun Application.module() {
    install(HocuspocusKtor) {
        path = "/collab"
        use(server)
    }
}
```

The plugin installs bounded WebSocket channels, applies frame and heartbeat
limits, and shuts the server down with the Ktor application. If Ktor
`WebSockets` is installed separately, keep both channels bounded and the frame
limit no larger than `HocuspocusConfiguration.maxFrameSize`.

## Runtime contract

- Provider traffic is genuine Yjs update V1; private YKS envelopes never cross
  the browser boundary.
- Authentication, decoding, queues, documents, awareness, and fanout have
  explicit limits.
- CRDT fanout is batched by `flushDelay` and capped by `flushMaxBytes`.
  `document.flush()` enqueues broadcasts; it does not force storage or wait for
  client acknowledgement.
- Change hooks identify affected document roots. Disconnect stores before
  unload, cleanup hooks are failure-isolated, and failed setup removes its
  partial route before sending a denial so immediate retries can authenticate.
- A CRDT callback can fail after committing changes. Engines report those
  deltas in `CrdtMutationException`; the server broadcasts them and registers
  change/store hooks before rethrowing the original cause. This preserves
  cancellation and connection-reset behavior without implying a rollback or
  a successful sync acknowledgement. Failed initial loads are discarded.
- Redis synchronizes CRDT, awareness, and server stateless messages and uses a
  renewable ownership-checked store lock.
- Node-compatible webhook create failures are reported and treated as an empty
  load; standard-update mode remains fail-closed.

## Build and verification

```sh
./gradlew -Pyks.localPath=/path/to/yks check
pnpm test:jvm:matrix
pnpm test:jvm:interop
```

The matrix verifies the pinned Hocuspocus `4.7.1` source map and JUnit test
discovery. Redis and S3 integration cases require `REDIS_URL`, `S3_ENDPOINT`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`; otherwise those
service-backed cases are skipped.

See [JVM_COMPATIBILITY.md](JVM_COMPATIBILITY.md) for the compatibility boundary
and [PERFORMANCE.md](PERFORMANCE.md) for benchmark and soak commands.

### WebSocket transport diagnostics

`HocuspocusKtorConfiguration.onTransportDiagnostic` is an optional synchronous,
best-effort observer. It receives the Ktor call and a metadata-only snapshot at
`opened`, `ending` (before cleanup), and `closed` (after cleanup, including failure).
It does not alter the existing `serveHocuspocus` binary interface or timeout policy.
Observer exceptions are isolated from message delivery and cleanup.

Snapshots include the server socket ID, application binary-message counts/ages,
outbound queued bytes (including an in-flight send), send timing/rejections,
requested/observed close codes, exception class/fixed failure category, and cleanup
time. They contain no message bytes, document names, headers, URL, token, or raw
close reason. The call is supplied to let applications select and validate their
own correlation field; do not log the call or request wholesale.

The five-second session-coroutine timer reports maximum scheduler lateness, not
Netty event-loop latency. Application-message ages are not native ping/pong
observations. A missing close code remains null rather than waiting on another
coroutine while handling a cancellation. The observer must not block or perform
network I/O; applications should emit snapshots through their normal log sink.
