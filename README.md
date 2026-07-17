# Hocuspocus JVM

A Kotlin/JVM implementation of the Hocuspocus v4 collaboration server for
Ktor. Browser clients continue to use the JavaScript `@hocuspocus/provider`,
while authentication, Yjs synchronization, awareness, persistence, and
application lifecycle run on the JVM.

## Modules

- `hocuspocus-protocol`: bounded Hocuspocus, lib0, sync, auth, and awareness codecs.
- `hocuspocus-core`: document, connection, hook, persistence, and shutdown engine.
- `hocuspocus-yks`: standard Yjs update V1 support backed by YKS.
- `hocuspocus-ktor`: Ktor WebSocket plugin and request-context integration.
- `hocuspocus-redis`: Redis multi-node synchronization and store locking.
- `hocuspocus-throttle`, `hocuspocus-metrics`, `hocuspocus-webhook`: production
  connection and observability extensions.
- `hocuspocus-storage-s3`, `hocuspocus-storage-sqlite`: durable standard-update
  adapters.
- `hocuspocus-benchmark`: JMH/JFR and bounded load/soak verification.
- `hocuspocus-ktor-example`: executable interoperability fixture.

The TypeScript provider sources are retained only as a compatibility oracle;
this repository does not publish the original Node packages.

## Use with Ktor

```kotlin
val hocuspocus = HocuspocusServer(
    HocuspocusConfiguration<Unit>(
        documentFactory = YksDocumentFactory(),
        authenticator = HocuspocusAuthenticator { payload ->
            if (!verifyToken(payload.token)) throw HocuspocusAuthenticationException()
        },
    ),
)

fun Application.module() {
    install(HocuspocusKtor) {
        path = "/collab"
        use(hocuspocus)
    }
}
```

See [jvm/README.md](jvm/README.md) for dependencies, authentication,
persistence, direct transactions, and production limits. The exact wire and
lifecycle compatibility boundary is documented in
[JVM_COMPATIBILITY.md](JVM_COMPATIBILITY.md).

## Verify

```sh
JAVA_HOME=/path/to/jdk-21 ./jvm/gradlew \
  -p jvm \
  -Pyks.localPath=/path/to/yks \
  check

JAVA_HOME=/path/to/jdk-21 \
YKS_LOCAL_PATH=/path/to/yks \
pnpm test:jvm:interop
```

## License

MIT. See [LICENSE.md](LICENSE.md) and [jvm/THIRD_PARTY_NOTICES](jvm/THIRD_PARTY_NOTICES).
