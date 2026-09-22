# Hocuspocus JVM for Ktor

A Kotlin/JVM implementation of the Hocuspocus v4 server contract for existing
Ktor applications. Browser clients continue to use the official JavaScript
`@hocuspocus/provider`.

## Baseline

| Component | Version |
| --- | --- |
| Published JVM release | `0.1.8` |
| Source build | `0.1.9-SNAPSHOT` |
| Hocuspocus / Provider | `4.7.0` |
| Source CRDT engine | YKS `0.2.15` (planned release) — `c6d53147a7a0` |
| Toolchain | JDK 21, Kotlin 2.4.20, Gradle 9.7.1, Ktor 3.5.2 |

The next `0.1.9` release aligns both JVM repositories on Kotlin 2.4.20 and
Gradle 9.7.1 and includes the Hocuspocus 4.7.0 Redis reply-routing fix. Publish
YKS `0.2.15` before Hocuspocus `0.1.9`; CI, soak, and publication workflows pin
the same YKS source commit. Standalone consumers cover Kotlin 2.4.20 and 2.3.21.

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
    implementation("ai.hocuspocus:hocuspocus-ktor:0.1.8")
    // implementation("ai.hocuspocus:hocuspocus-redis:0.1.8")
}
```

Artifacts are in GitHub Packages. Configure credentials with `gpr.user` and
`gpr.key`, then add repositories for both `hwisu/hocuspocus` and `hwisu/yks`.
Use `mavenLocal()` with `0.1.9-SNAPSHOT` for source development.

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

The matrix verifies the pinned Hocuspocus `4.7.0` source map and JUnit test
discovery. Redis and S3 integration cases require `REDIS_URL`, `S3_ENDPOINT`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`; otherwise those
service-backed cases are skipped.

See [JVM_COMPATIBILITY.md](JVM_COMPATIBILITY.md) for the compatibility boundary
and [PERFORMANCE.md](PERFORMANCE.md) for benchmark and soak commands.
