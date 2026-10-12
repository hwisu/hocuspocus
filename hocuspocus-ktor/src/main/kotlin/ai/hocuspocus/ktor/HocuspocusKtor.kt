package ai.hocuspocus.ktor

import ai.hocuspocus.core.ClientSession
import ai.hocuspocus.core.CloseEvents
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.SocketTransport
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationPlugin
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.application.pluginOrNull
import io.ktor.server.request.uri
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.ChannelOverflow
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.send
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

public class HocuspocusKtorConfiguration {
    public var path: String = "/collab"
    public var installWebSockets: Boolean = true
    public var outboundQueueCapacity: Int = 256
    public var outboundQueueByteCapacity: Int = 16 * 1024 * 1024
    public var webSocketIncomingQueueCapacity: Int = 256
    public var webSocketOutgoingQueueCapacity: Int = 256
    public var requireBoundedWebSocketChannels: Boolean = true
    public var shutdownTimeout: Duration = 30.seconds
    /** Optional, non-suspending metadata observer. Exceptions never affect the session. */
    public var onTransportDiagnostic: ((ApplicationCall, HocuspocusTransportDiagnostic) -> Unit)? = null

    internal var binding: HocuspocusKtorBinding? = null

    public fun use(server: HocuspocusServer<Unit>) {
        use(server) { Unit }
    }

    public fun <C : Any> use(
        server: HocuspocusServer<C>,
        contextFactory: suspend ApplicationCall.() -> C,
    ) {
        binding = TypedHocuspocusKtorBinding(server, contextFactory)
    }

    internal fun validate() {
        require(binding != null) { "HocuspocusKtor requires use(server)" }
        require(path.startsWith('/')) { "path must start with /" }
        require(outboundQueueCapacity > 0) { "outboundQueueCapacity must be positive" }
        require(outboundQueueByteCapacity > 0) { "outboundQueueByteCapacity must be positive" }
        require(webSocketIncomingQueueCapacity > 0) {
            "webSocketIncomingQueueCapacity must be positive"
        }
        require(webSocketOutgoingQueueCapacity > 0) {
            "webSocketOutgoingQueueCapacity must be positive"
        }
        require(shutdownTimeout.isPositive() && shutdownTimeout.isFinite()) {
            "shutdownTimeout must be positive and finite"
        }
    }
}

internal interface HocuspocusKtorBinding {
    val timeout: Duration
    val maxFrameSize: Int

    suspend fun start()

    suspend fun serve(
        session: DefaultWebSocketServerSession,
        call: ApplicationCall,
        outboundQueueCapacity: Int,
        outboundQueueByteCapacity: Int,
        observer: ((HocuspocusTransportDiagnostic) -> Unit)?,
    )

    suspend fun shutdown()

    fun reportError(error: Throwable)
}

private class TypedHocuspocusKtorBinding<C : Any>(
    private val server: HocuspocusServer<C>,
    private val contextFactory: suspend ApplicationCall.() -> C,
) : HocuspocusKtorBinding {
    override val timeout: Duration
        get() = server.configuration.timeout

    override val maxFrameSize: Int
        get() = server.configuration.maxFrameSize

    override suspend fun start() {
        server.start()
    }

    override suspend fun serve(
        session: DefaultWebSocketServerSession,
        call: ApplicationCall,
        outboundQueueCapacity: Int,
        outboundQueueByteCapacity: Int,
        observer: ((HocuspocusTransportDiagnostic) -> Unit)?,
    ) {
        val context = contextFactory(call)
        session.serveObservedHocuspocus(server, context, outboundQueueCapacity, outboundQueueByteCapacity, observer)
    }

    override suspend fun shutdown() {
        server.shutdown()
    }

    override fun reportError(error: Throwable) {
        runCatching { server.configuration.onError(error) }
    }
}

/** Ktor application plugin that owns the Hocuspocus WebSocket route and lifecycle. */
public val HocuspocusKtor: ApplicationPlugin<HocuspocusKtorConfiguration> = createApplicationPlugin(
    name = "HocuspocusKtor",
    createConfiguration = ::HocuspocusKtorConfiguration,
) {
    pluginConfig.validate()
    val binding = checkNotNull(pluginConfig.binding)
    val routePath = pluginConfig.path
    val outboundQueueCapacity = pluginConfig.outboundQueueCapacity
    val outboundQueueByteCapacity = pluginConfig.outboundQueueByteCapacity
    val webSocketIncomingQueueCapacity = pluginConfig.webSocketIncomingQueueCapacity
    val webSocketOutgoingQueueCapacity = pluginConfig.webSocketOutgoingQueueCapacity
    val shutdownTimeout = pluginConfig.shutdownTimeout
    val observer = pluginConfig.onTransportDiagnostic

    if (pluginConfig.installWebSockets && application.pluginOrNull(WebSockets) == null) {
        application.install(WebSockets) {
            pingPeriodMillis = binding.timeout.inWholeMilliseconds / 2
            timeoutMillis = binding.timeout.inWholeMilliseconds
            maxFrameSize = binding.maxFrameSize.toLong()
            masking = false
            channels {
                incoming = bounded(
                    capacity = webSocketIncomingQueueCapacity,
                    onOverflow = ChannelOverflow.CLOSE,
                )
                outgoing = bounded(
                    capacity = webSocketOutgoingQueueCapacity,
                    onOverflow = ChannelOverflow.SUSPEND,
                )
            }
        }
    }
    val webSockets = checkNotNull(application.pluginOrNull(WebSockets)) {
        "Ktor WebSockets is not installed; enable installWebSockets or install it before HocuspocusKtor"
    }
    if (pluginConfig.requireBoundedWebSocketChannels) {
        check(webSockets.channelsConfig.incoming.capacity != Channel.UNLIMITED) {
            "Ktor WebSockets incoming channel must be bounded; configure WebSockets.channels or " +
                "set requireBoundedWebSocketChannels=false to accept the OOM risk"
        }
        check(webSockets.channelsConfig.outgoing.capacity != Channel.UNLIMITED) {
            "Ktor WebSockets outgoing channel must be bounded; configure WebSockets.channels or " +
                "set requireBoundedWebSocketChannels=false to accept the OOM risk"
        }
    }

    runBlocking { binding.start() }

    application.routing {
        webSocket(routePath) {
            binding.serve(
                this, call, outboundQueueCapacity, outboundQueueByteCapacity,
                observer?.let { callback -> { snapshot -> callback(call, snapshot) } },
            )
        }
    }
    application.monitor.subscribe(ApplicationStopping) {
        try {
            runBlocking {
                withTimeout(shutdownTimeout) { binding.shutdown() }
            }
        } catch (error: Throwable) {
            binding.reportError(error)
            throw error
        }
    }
}

public suspend fun <C : Any> DefaultWebSocketServerSession.serveHocuspocus(
    server: HocuspocusServer<C>,
    context: C,
    outboundQueueCapacity: Int = 256,
    outboundQueueByteCapacity: Int = 16 * 1024 * 1024,
) {
    serveObservedHocuspocus(server, context, outboundQueueCapacity, outboundQueueByteCapacity, null)
}

private suspend fun <C : Any> DefaultWebSocketServerSession.serveObservedHocuspocus(
    server: HocuspocusServer<C>,
    context: C,
    outboundQueueCapacity: Int,
    outboundQueueByteCapacity: Int,
    observer: ((HocuspocusTransportDiagnostic) -> Unit)?,
) {
    require(outboundQueueCapacity > 0) { "outboundQueueCapacity must be positive" }
    require(outboundQueueByteCapacity > 0) { "outboundQueueByteCapacity must be positive" }
    val transport = KtorSocketTransport(this, this, outboundQueueCapacity, outboundQueueByteCapacity)
    val coreSession: ClientSession<C> = server.openSession(
        transport = transport,
        request = call.toHocuspocusRequest(),
        initialContext = context,
    )
    val observation = observer?.let { TransportObservation() }
    val lagSampler = observation?.let { state ->
        launch {
            while (isActive) {
                val before = System.nanoTime()
                delay(5.seconds)
                state.schedulerLagMaxMs = maxOf(state.schedulerLagMaxMs, ((System.nanoTime() - before) / 1_000_000 - 5_000).coerceAtLeast(0))
            }
        }
    }
    fun record(phase: String, cleanupDurationMs: Long? = null, cleanupFailure: Throwable? = null) {
        if (observation == null) return
        try {
            observer(observation.snapshot(phase, coreSession.socketId, transport, completedCloseCode(), cleanupDurationMs, cleanupFailure))
        } catch (_: Exception) {
            // An unavailable observer must not change delivery, cancellation or cleanup.
        }
    }
    record("opened")
    try {
        for (frame in incoming) {
            when (frame) {
                // Ktor's WebSocketReader materializes each inbound frame into a
                // fresh ByteArray. Core retains that owned array until its
                // asynchronous per-document queue has consumed the frame.
                is Frame.Binary -> {
                    observation?.received()
                    coreSession.handleBinaryOwned(frame.data)
                }
                is Frame.Close -> break
                else -> {
                    transport.close(CloseEvents.ResetConnection.code, "Hocuspocus requires binary frames")
                    break
                }
            }
        }
    } catch (failure: Throwable) {
        observation?.failure = failure
        throw failure
    } finally {
        lagSampler?.cancel()
        record("ending")
        val cleanupStarted = System.nanoTime()
        var cleanupFailure: Throwable? = null
        try {
            coreSession.close()
            transport.finish(server.configuration.timeout)
        } catch (failure: Throwable) {
            cleanupFailure = failure
            throw failure
        } finally {
            record("closed", (System.nanoTime() - cleanupStarted) / 1_000_000, cleanupFailure)
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
private fun DefaultWebSocketServerSession.completedCloseCode(): Int? =
    if (closeReason.isCompleted) runCatching { closeReason.getCompleted()?.code?.toInt() }.getOrNull() else null

/** Application binary-message statistics; these are NOT native ping/pong observations. */
public data class HocuspocusTransportDiagnostic(
    val phase: String,
    val socketId: String,
    val durationMs: Long,
    val receivedMessages: Long,
    val lastReceivedAgeMs: Long?,
    val sentMessages: Long,
    val lastSentAgeMs: Long?,
    val queuedBytes: Long,
    val sendInFlightMs: Long?,
    val sendMaxDurationMs: Long,
    val sendRejectedCount: Long,
    val requestedCloseCode: Int?,
    val closeCode: Int?,
    val failureType: String?,
    val failureCategory: String?,
    val schedulerLagMaxMs: Long,
    val cleanupDurationMs: Long?,
    val cleanupFailureType: String?,
)

private class TransportObservation {
    private val started = System.nanoTime()
    private var receivedMessages = 0L
    private var lastReceived: Long? = null
    var failure: Throwable? = null
    @Volatile
    var schedulerLagMaxMs = 0L

    fun received() {
        receivedMessages++
        lastReceived = System.nanoTime()
    }

    fun snapshot(phase: String, socketId: String, transport: KtorSocketTransport, closeCode: Int?, cleanupDurationMs: Long?, cleanupFailure: Throwable?): HocuspocusTransportDiagnostic {
        val now = System.nanoTime()
        val causes = generateSequence(failure) { it.cause }.take(8).toList()
        return HocuspocusTransportDiagnostic(
            phase, socketId, (now - started) / 1_000_000, receivedMessages,
            lastReceived?.let { (now - it) / 1_000_000 }, transport.sentMessages.get(),
            transport.lastSent.get().takeIf { it != 0L }?.let { (now - it) / 1_000_000 },
            transport.queuedBytes.get(), transport.sendStarted.get().takeIf { it != 0L }?.let { (now - it) / 1_000_000 },
            transport.sendMaxDurationMs.get(), transport.sendRejectedCount.get(), transport.requestedCloseCode,
            closeCode, failure?.javaClass?.name,
            when {
                causes.any { it is java.io.IOException && it.message == "Ping timeout" } -> "ping_timeout"
                failure is java.util.concurrent.CancellationException -> "cancelled"
                failure != null -> "receive_failed"
                else -> null
            },
            schedulerLagMaxMs, cleanupDurationMs, cleanupFailure?.javaClass?.name,
        )
    }
}

private fun ApplicationCall.toHocuspocusRequest(): HocuspocusRequest = HocuspocusRequest(
    uri = request.uri,
    headers = TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER).apply {
        request.headers.names().forEach { name -> put(name, request.headers.getAll(name).orEmpty()) }
    },
    parameters = request.queryParameters.names().associateWith { name ->
        request.queryParameters.getAll(name).orEmpty()
    },
    remoteAddress = request.local.remoteAddress,
)

internal class KtorSocketTransport private constructor(
    private val scope: CoroutineScope,
    capacity: Int,
    private val byteCapacity: Int,
    private val sendBinary: suspend (ByteArray) -> Unit,
    private val closeSocket: suspend (Int, String) -> Unit,
) : SocketTransport {
    public constructor(
        session: DefaultWebSocketServerSession,
        scope: CoroutineScope,
        capacity: Int,
        byteCapacity: Int,
    ) : this(
        scope,
        capacity,
        byteCapacity,
        sendBinary = { bytes -> session.send(Frame.Binary(fin = true, data = bytes)) },
        closeSocket = { code, reason -> session.close(CloseReason(code.toShort(), reason.take(123))) },
    )

    internal constructor(
        scope: CoroutineScope,
        capacity: Int,
        byteCapacity: Int,
        sendBinary: suspend (ByteArray) -> Unit,
    ) : this(
        scope,
        capacity,
        byteCapacity,
        sendBinary,
        closeSocket = { _, _ -> },
    )

    private val open: AtomicBoolean = AtomicBoolean(true)
    private val outgoing: Channel<ByteArray> = Channel(capacity)
    internal val queuedBytes: AtomicLong = AtomicLong()
    internal val sentMessages = AtomicLong()
    internal val lastSent = AtomicLong()
    internal val sendStarted = AtomicLong()
    internal val sendMaxDurationMs = AtomicLong()
    internal val sendRejectedCount = AtomicLong()
    @Volatile
    internal var requestedCloseCode: Int? = null
    private val closeLock: Any = Any()
    private var closeJob: Job? = null
    private val sender: Job = scope.launch {
        try {
            for (bytes in outgoing) {
                val started = System.nanoTime()
                sendStarted.set(started)
                try {
                    sendBinary(bytes)
                    sentMessages.incrementAndGet()
                    lastSent.set(System.nanoTime())
                } finally {
                    sendMaxDurationMs.accumulateAndGet((System.nanoTime() - started) / 1_000_000, ::maxOf)
                    sendStarted.set(0)
                    queuedBytes.addAndGet(-bytes.size.toLong())
                }
            }
        } finally {
            open.set(false)
        }
    }

    override val isOpen: Boolean
        get() = open.get()

    override fun send(bytes: ByteArray): Boolean {
        if (!open.get()) {
            sendRejectedCount.incrementAndGet()
            return false
        }
        val nextQueuedBytes = queuedBytes.addAndGet(bytes.size.toLong())
        if (nextQueuedBytes > byteCapacity) {
            queuedBytes.addAndGet(-bytes.size.toLong())
            sendRejectedCount.incrementAndGet()
            return false
        }
        // Core frames are immutable and remain valid after send(), so the
        // bounded sender queue can retain the shared frame without a
        // per-recipient 1:1 ByteArray copy.
        val result = outgoing.trySend(bytes)
        if (result.isFailure) {
            queuedBytes.addAndGet(-bytes.size.toLong())
            sendRejectedCount.incrementAndGet()
        }
        return result.isSuccess
    }

    override fun close(code: Int, reason: String) {
        synchronized(closeLock) {
            if (!open.compareAndSet(true, false)) return
            requestedCloseCode = code
            outgoing.close()
            closeJob = scope.launch {
                closeSocket(code, reason)
            }
        }
    }

    internal suspend fun finish(timeout: Duration) {
        val pendingClose = synchronized(closeLock) {
            open.set(false)
            outgoing.close()
            closeJob
        }
        val drained = withTimeoutOrNull(timeout) {
            sender.join()
            pendingClose?.join()
            true
        } ?: false
        if (!drained) {
            sender.cancelAndJoin()
            pendingClose?.cancelAndJoin()
        }
    }
}
