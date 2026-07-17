package ai.hocuspocus.benchmark

import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.ktor.HocuspocusKtor
import ai.hocuspocus.yks.YksDocumentFactory
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing

/**
 * Minimal Ktor process used only by the cross-runtime WebSocket benchmark.
 *
 * It deliberately has no persistence or application hooks, matching the
 * upstream benchmark server. Both targets therefore measure protocol decode,
 * CRDT application, fanout, and their HTTP/WebSocket runtimes.
 */
public object WebSocketBenchmarkServer {
    @JvmStatic
    public fun main(args: Array<String>) {
        val port = System.getenv("HOCUSPOCUS_BENCHMARK_PORT")?.toIntOrNull() ?: 19877
        val server: HocuspocusServer<Unit> = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = YksDocumentFactory(),
                allowAnonymous = true,
                onError = { error ->
                    System.getLogger("ai.hocuspocus.benchmark")
                        .log(System.Logger.Level.ERROR, "Benchmark server failure", error)
                },
            ),
        )

        embeddedServer(Netty, host = "127.0.0.1", port = port) {
            install(HocuspocusKtor) {
                path = "/collab"
                use(server)
            }
            routing {
                get("/health") {
                    call.respondText("ok", ContentType.Text.Plain)
                }
            }
        }.start(wait = true)
    }
}
