package ai.hocuspocus.metrics

import ai.hocuspocus.core.AwarenessUpdatePayload
import ai.hocuspocus.core.ChangePayload
import ai.hocuspocus.core.ConfigurePayload
import ai.hocuspocus.core.ConnectionAttempt
import ai.hocuspocus.core.ConnectedPayload
import ai.hocuspocus.core.DisconnectPayload
import ai.hocuspocus.core.DocumentHookPayload
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.core.UnloadDocumentPayload
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

public enum class MetricKind {
    Counter,
    Gauge,
    Histogram,
}

public data class MetricPoint(
    val name: String,
    val kind: MetricKind,
    val value: Double,
    val attributes: Map<String, String> = emptyMap(),
    val timestampMillis: Long = System.currentTimeMillis(),
)

public fun interface MetricsSink {
    public fun record(point: MetricPoint)
}

public data class StructuredMetricsConfiguration(
    val includeDocumentName: Boolean = false,
    val staticAttributes: Map<String, String> = emptyMap(),
    val maxTrackedOperations: Int = 10_000,
    val operationTrackingTtl: Duration = 5.minutes,
) {
    init {
        require(maxTrackedOperations > 0) { "maxTrackedOperations must be positive" }
        require(operationTrackingTtl.isPositive() && operationTrackingTtl.isFinite()) {
            "operationTrackingTtl must be positive and finite"
        }
    }
}

/**
 * Low-cardinality structured metrics emitted directly from the server hooks.
 *
 * The default does not use document names as labels. Enable that only for
 * bounded document sets, otherwise it creates an unbounded metrics cardinality.
 */
public class StructuredMetricsExtension<C : Any>(
    private val sink: MetricsSink,
    public val configuration: StructuredMetricsConfiguration = StructuredMetricsConfiguration(),
    private val nanoTime: () -> Long = System::nanoTime,
) : HocuspocusExtension<C> {
    override val priority: Int = Int.MAX_VALUE
    override val name: String = "structured-metrics"

    private val loadStarted: ConcurrentHashMap<String, Long> = ConcurrentHashMap()
    private val storeStarted: ConcurrentHashMap<String, Long> = ConcurrentHashMap()

    override suspend fun onConfigure(payload: ConfigurePayload<C>) {
        emit("hocuspocus.server.start", MetricKind.Counter, 1.0)
    }

    override suspend fun onConnect(payload: ConnectionAttempt<C>) {
        emit(
            "hocuspocus.connection.attempt",
            MetricKind.Counter,
            1.0,
            attributes(payload.routingKey.documentName),
        )
    }

    override suspend fun connected(payload: ConnectedPayload<C>) {
        emit(
            "hocuspocus.connection.accepted",
            MetricKind.Counter,
            1.0,
            attributes(payload.connection.document.name),
        )
        connectionGauge(payload.attempt.server)
    }

    override suspend fun onLoadDocument(payload: DocumentHookPayload<C>): ByteArray? {
        trackStart(loadStarted, payload.document.name)
        return null
    }

    override suspend fun afterLoadDocument(payload: DocumentHookPayload<C>) {
        loadStarted.remove(payload.document.name)?.let { started ->
            emitDuration("hocuspocus.document.load.duration", started, payload.document.name)
        }
        emit("hocuspocus.document.loaded", MetricKind.Counter, 1.0, attributes(payload.document.name))
        emit(
            "hocuspocus.documents",
            MetricKind.Gauge,
            payload.server.documentsCount.toDouble() + 1.0,
        )
    }

    override suspend fun onChange(payload: ChangePayload<C>) {
        val labels = attributes(payload.document.name) +
            ("origin" to payload.transactionOrigin.metricName())
        emit("hocuspocus.change", MetricKind.Counter, 1.0, labels)
        emit("hocuspocus.change.bytes", MetricKind.Histogram, payload.update.size.toDouble(), labels)
    }

    override suspend fun onAwarenessUpdate(payload: AwarenessUpdatePayload<C>) {
        val labels = attributes(payload.document.name)
        emit("hocuspocus.awareness.change", MetricKind.Counter, 1.0, labels)
        emit(
            "hocuspocus.awareness.clients",
            MetricKind.Gauge,
            payload.states.size.toDouble(),
            labels,
        )
    }

    override suspend fun onStoreDocument(payload: StorePayload<C>) {
        trackStart(storeStarted, payload.document.name)
    }

    override suspend fun afterStoreDocument(payload: StorePayload<C>) {
        storeStarted.remove(payload.document.name)?.let { started ->
            emitDuration("hocuspocus.document.store.duration", started, payload.document.name)
        }
        emit("hocuspocus.document.stored", MetricKind.Counter, 1.0, attributes(payload.document.name))
    }

    override suspend fun onDisconnect(payload: DisconnectPayload<C>) {
        emit(
            "hocuspocus.connection.disconnected",
            MetricKind.Counter,
            1.0,
            attributes(payload.document.name),
        )
        connectionGauge(payload.server)
    }

    override suspend fun afterUnloadDocument(payload: UnloadDocumentPayload<C>) {
        loadStarted.remove(payload.document.name)
        storeStarted.remove(payload.document.name)
        emit("hocuspocus.document.unloaded", MetricKind.Counter, 1.0, attributes(payload.document.name))
        emit(
            "hocuspocus.documents",
            MetricKind.Gauge,
            payload.server.documentsCount.toDouble(),
        )
    }

    override suspend fun onDestroy(server: HocuspocusServer<C>) {
        emit("hocuspocus.server.stop", MetricKind.Counter, 1.0)
        loadStarted.clear()
        storeStarted.clear()
    }

    private fun connectionGauge(server: HocuspocusServer<C>) {
        emit("hocuspocus.connections", MetricKind.Gauge, server.connectionsCount.toDouble())
    }

    private fun trackStart(target: ConcurrentHashMap<String, Long>, key: String) {
        val now = nanoTime()
        if (target.size >= configuration.maxTrackedOperations) {
            val ttl = configuration.operationTrackingTtl.inWholeNanoseconds
            target.entries.removeIf { (_, started) -> now - started >= ttl }
        }
        if (target.size < configuration.maxTrackedOperations || target.containsKey(key)) {
            target[key] = now
        }
    }

    private fun emitDuration(name: String, startedNanos: Long, documentName: String) {
        val elapsed = (nanoTime() - startedNanos).coerceAtLeast(0)
        emit(name, MetricKind.Histogram, elapsed / 1_000_000.0, attributes(documentName))
    }

    private fun attributes(documentName: String): Map<String, String> =
        if (configuration.includeDocumentName) {
            configuration.staticAttributes + ("document" to documentName)
        } else {
            configuration.staticAttributes
        }

    private fun emit(
        name: String,
        kind: MetricKind,
        value: Double,
        attributes: Map<String, String> = configuration.staticAttributes,
    ) {
        runCatching {
            sink.record(MetricPoint(name, kind, value, attributes))
        }
    }
}

public class InMemoryMetricsSink : MetricsSink {
    private val points: CopyOnWriteArrayList<MetricPoint> = CopyOnWriteArrayList()

    override fun record(point: MetricPoint) {
        points += point
    }

    public fun snapshot(): List<MetricPoint> = points.toList()

    public fun clear() {
        points.clear()
    }
}

public class SystemLoggerMetricsSink(
    private val logger: System.Logger = System.getLogger("ai.hocuspocus.metrics"),
) : MetricsSink {
    override fun record(point: MetricPoint) {
        val json = buildJsonObject {
            put("name", point.name)
            put("kind", point.kind.name.lowercase())
            put("value", point.value)
            put("timestampMillis", point.timestampMillis)
            put(
                "attributes",
                buildJsonObject {
                    point.attributes.toSortedMap().forEach { (key, value) -> put(key, value) }
                },
            )
        }
        logger.log(System.Logger.Level.INFO, Json.encodeToString(json))
    }
}

private fun ai.hocuspocus.core.TransactionOrigin.metricName(): String = when (this) {
    is ai.hocuspocus.core.TransactionOrigin.Connection -> "connection"
    is ai.hocuspocus.core.TransactionOrigin.Local -> "local"
    ai.hocuspocus.core.TransactionOrigin.Redis -> "redis"
}
