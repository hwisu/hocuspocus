package ai.hocuspocus.benchmark

import ai.hocuspocus.core.DatabaseExtension
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.ktor.HocuspocusKtor
import ai.hocuspocus.redis.RedisExtension
import ai.hocuspocus.redis.RedisExtensionConfiguration
import ai.hocuspocus.storage.sqlite.SQLiteDocumentStorage
import ai.hocuspocus.storage.sqlite.SQLiteStorageConfiguration
import ai.hocuspocus.yks.YksDocumentFactory
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

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
        val mode = System.getenv("HOCUSPOCUS_BENCHMARK_MODE") ?: "core"
        val benchmarkOutboundQueueCapacity =
            System.getenv("HOCUSPOCUS_BENCHMARK_OUTBOUND_QUEUE_MESSAGES")?.toIntOrNull() ?: 8_192
        val benchmarkOutboundQueueBytes =
            System.getenv("HOCUSPOCUS_BENCHMARK_OUTBOUND_QUEUE_BYTES")?.toIntOrNull()
                ?: 256 * 1024 * 1024
        val storedDocuments = AtomicLong()
        val extensions = benchmarkExtensions(mode, storedDocuments)
        val server: HocuspocusServer<Unit> = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = YksDocumentFactory(),
                allowAnonymous = true,
                extensions = extensions,
                debounce = if (mode == "sqlite") Duration.ZERO else 2.seconds,
                maxDebounce = if (mode == "sqlite") Duration.ZERO else 10.seconds,
                maxEstablishedQueueMessages = 8_192,
                maxEstablishedQueueSize = 256 * 1024 * 1024,
                onError = { error ->
                    System.getLogger("ai.hocuspocus.benchmark")
                        .log(System.Logger.Level.ERROR, "Benchmark server failure", error)
                },
            ),
        )

        val engine = embeddedServer(Netty, host = "127.0.0.1", port = port) {
            install(HocuspocusKtor) {
                path = "/collab"
                outboundQueueCapacity = benchmarkOutboundQueueCapacity
                outboundQueueByteCapacity = benchmarkOutboundQueueBytes
                use(server)
            }
            routing {
                get("/health") {
                    call.respondText("ok", ContentType.Text.Plain)
                }
                get("/benchmark/stats") {
                    call.respondText(
                        """{"mode":"$mode","stores":${storedDocuments.get()},"documents":${server.documentsCount}}""",
                        ContentType.Application.Json,
                    )
                }
            }
        }
        engine.engineConfig.apply {
            connectionGroupSize = 1
            workerGroupSize = 2
            callGroupSize = 2
            shareWorkGroup = true
        }
        engine.start(wait = true)
    }

    private fun benchmarkExtensions(
        mode: String,
        storedDocuments: AtomicLong,
    ): List<HocuspocusExtension<Unit>> = when (mode) {
        "core" -> emptyList()
        "sqlite" -> {
            val database = requireNotNull(System.getenv("HOCUSPOCUS_BENCHMARK_SQLITE_PATH")) {
                "HOCUSPOCUS_BENCHMARK_SQLITE_PATH is required in sqlite mode"
            }
            listOf(
                DatabaseExtension(
                    SQLiteDocumentStorage(SQLiteStorageConfiguration(database)),
                    closeOnDestroy = true,
                ),
                object : HocuspocusExtension<Unit> {
                    override val priority: Int = 0
                    override val name: String = "benchmark-stats"

                    override suspend fun afterStoreDocument(payload: StorePayload<Unit>) {
                        storedDocuments.incrementAndGet()
                    }
                },
            )
        }
        "redis" -> {
            val redisUri = requireNotNull(System.getenv("HOCUSPOCUS_BENCHMARK_REDIS_URI")) {
                "HOCUSPOCUS_BENCHMARK_REDIS_URI is required in redis mode"
            }
            listOf(
                RedisExtension(
                    redisUri = redisUri,
                    configuration = RedisExtensionConfiguration(
                        prefix = System.getenv("HOCUSPOCUS_BENCHMARK_REDIS_PREFIX") ?: "hocuspocus-benchmark",
                        identifier = requireNotNull(System.getenv("HOCUSPOCUS_BENCHMARK_IDENTIFIER")) {
                            "HOCUSPOCUS_BENCHMARK_IDENTIFIER is required in redis mode"
                        },
                        disconnectDelay = Duration.ZERO,
                        initialSyncTimeout = 2.seconds,
                    ),
                ),
            )
        }
        else -> error("Unsupported HOCUSPOCUS_BENCHMARK_MODE: $mode")
    }
}
