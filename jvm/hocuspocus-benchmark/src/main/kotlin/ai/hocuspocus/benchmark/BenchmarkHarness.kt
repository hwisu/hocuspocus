package ai.hocuspocus.benchmark

import ai.hocuspocus.core.ClientSession
import ai.hocuspocus.core.CloseEvent
import ai.hocuspocus.core.DatabaseExtension
import ai.hocuspocus.core.DocumentStorage
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.SocketTransport
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.yks.YksDocumentFactory
import ai.hocuspocus.yks.transactYks
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

public class BenchmarkHarness(
    clientCount: Int,
    private val documentName: String = "benchmark",
    private val rejectAfterFrames: Long = Long.MAX_VALUE,
) : AutoCloseable {
    init {
        require(clientCount > 0) { "clientCount must be positive" }
        require(rejectAfterFrames >= 0) { "rejectAfterFrames must not be negative" }
    }

    private val errors: MutableList<Throwable> = java.util.Collections.synchronizedList(mutableListOf())
    private val server: HocuspocusServer<Unit> = HocuspocusServer(
        HocuspocusConfiguration(
            documentFactory = YksDocumentFactory(),
            allowAnonymous = true,
            debounce = 1.hours,
            maxDebounce = 1.hours,
            maxFrameSize = 2 * 1024 * 1024,
            maxCrdtUpdateSize = 1024 * 1024,
            onError = errors::add,
        ),
    )
    private val transports: List<CountingTransport> =
        List(clientCount) { CountingTransport(rejectAfterFrames) }
    private val sessions: MutableList<ClientSession<Unit>> = mutableListOf()
    private val direct = runBlocking {
        transports.forEachIndexed { index, transport ->
            val session = server.openSession(
                transport,
                HocuspocusRequest("ws://benchmark/collab"),
                Unit,
                socketId = "benchmark-$index",
            )
            sessions += session
            session.handleBinary(authenticationFrame())
        }
        eventually { server.connectionsCount == clientCount }
        transports.forEach(CountingTransport::reset)
        server.openDirectConnection(documentName, Unit)
    }
    private var length: Int = 0

    public val clientCount: Int
        get() = transports.size

    public fun broadcast(payloadSize: Int): FanoutResult {
        require(payloadSize > 0) { "payloadSize must be positive" }
        val payload = "x".repeat(payloadSize)
        val beforeFrames = transports.sumOf(CountingTransport::frames)
        val beforeBytes = transports.sumOf(CountingTransport::bytes)
        val started = System.nanoTime()
        runBlocking {
            direct.transactYks {
                it.getText("body").insert(length, payload)
                length += payload.length
            }
        }
        val elapsed = System.nanoTime() - started
        return FanoutResult(
            elapsedNanos = elapsed,
            frames = transports.sumOf(CountingTransport::frames) - beforeFrames,
            bytes = transports.sumOf(CountingTransport::bytes) - beforeBytes,
        )
    }

    public fun activeClients(): Int = server.connectionsCount - if (direct.isOpen) 1 else 0

    public fun failures(): List<Throwable> = synchronized(errors) { errors.toList() }

    override fun close() {
        runBlocking {
            for (session in sessions) {
                session.close()
            }
            direct.disconnect()
            server.shutdown()
        }
    }

    private suspend fun eventually(assertion: () -> Boolean) {
        withTimeout(5.seconds) {
            while (!assertion()) delay(1.milliseconds)
        }
    }

    private fun authenticationFrame(): ByteArray = FrameCodec.encode(
        RoutingKey(documentName),
        MessageType.Auth,
        AuthenticationCodec.encodeClient(ClientAuthentication("", "4.4.0")),
    )
}

public data class FanoutResult(
    val elapsedNanos: Long,
    val frames: Long,
    val bytes: Long,
)

public object PerformanceScenarios {
    public fun persistenceLatency(storageLatency: Duration): Duration {
        val storage = LatencyStorage(storageLatency)
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        val started = System.nanoTime()
        runBlocking {
            val connection = server.openDirectConnection("persistence", Unit)
            connection.transactYks { it.getText("body").insert(0, "persist") }
            connection.disconnect()
            server.shutdown()
        }
        check(storage.stores == 1L) { "expected exactly one persistence call" }
        return (System.nanoTime() - started).nanoseconds
    }

    public fun retainedDocumentProbe(documentCount: Int): RetainedHeapResult {
        require(documentCount > 0) { "documentCount must be positive" }
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        val references = mutableListOf<WeakReference<Any>>()
        val before = usedHeap()
        runBlocking {
            repeat(documentCount) { index ->
                val connection = server.openDirectConnection("heap-$index", Unit)
                references += WeakReference(connection.document)
                connection.disconnect()
            }
        }
        repeat(4) {
            System.gc()
            Thread.sleep(25)
        }
        val retained = references.count { it.get() != null }
        val after = usedHeap()
        check(server.documentsCount == 0) { "unloaded documents remain registered" }
        runBlocking { server.shutdown() }
        return RetainedHeapResult(documentCount, retained, after - before)
    }

    private fun usedHeap(): Long {
        val runtime = Runtime.getRuntime()
        return runtime.totalMemory() - runtime.freeMemory()
    }
}

public data class RetainedHeapResult(
    val documents: Int,
    val retainedReferences: Int,
    val heapDeltaBytes: Long,
)

private class CountingTransport(
    private val rejectAfterFrames: Long,
) : SocketTransport {
    private val open: AtomicBoolean = AtomicBoolean(true)
    private val rejectionEnabled: AtomicBoolean = AtomicBoolean()
    private val frameCount: AtomicLong = AtomicLong()
    private val byteCount: AtomicLong = AtomicLong()

    override val isOpen: Boolean
        get() = open.get()

    val frames: Long
        get() = frameCount.get()

    val bytes: Long
        get() = byteCount.get()

    override fun send(bytes: ByteArray): Boolean {
        if (!open.get()) return false
        val next = frameCount.incrementAndGet()
        if (rejectionEnabled.get() && next > rejectAfterFrames) return false
        byteCount.addAndGet(bytes.size.toLong())
        return true
    }

    override fun close(code: Int, reason: String) {
        open.set(false)
    }

    fun reset() {
        frameCount.set(0)
        byteCount.set(0)
        rejectionEnabled.set(true)
    }
}

private class LatencyStorage(
    private val latency: Duration,
) : DocumentStorage {
    var stores: Long = 0
        private set

    override suspend fun load(documentName: String): ByteArray? = null

    override suspend fun store(documentName: String, state: ByteArray) {
        delay(latency)
        stores += 1
    }
}
