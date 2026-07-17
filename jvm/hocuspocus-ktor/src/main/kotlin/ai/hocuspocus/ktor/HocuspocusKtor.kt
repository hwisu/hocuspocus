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
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.send
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

public class HocuspocusKtorConfiguration {
    public var path: String = "/collab"
    public var installWebSockets: Boolean = true
    public var outboundQueueCapacity: Int = 256
    public var outboundQueueByteCapacity: Int = 16 * 1024 * 1024
    public var shutdownTimeout: Duration = 30.seconds

    internal var configuredServer: HocuspocusServer<Any>? = null
    internal var createContext: suspend ApplicationCall.() -> Any = { Unit }

    public fun use(server: HocuspocusServer<Unit>) {
        use(server) { Unit }
    }

    @Suppress("UNCHECKED_CAST")
    public fun <C : Any> use(
        server: HocuspocusServer<C>,
        contextFactory: suspend ApplicationCall.() -> C,
    ) {
        configuredServer = server as HocuspocusServer<Any>
        createContext = contextFactory as suspend ApplicationCall.() -> Any
    }

    internal fun validate() {
        require(configuredServer != null) { "HocuspocusKtor requires use(server)" }
        require(path.startsWith('/')) { "path must start with /" }
        require(outboundQueueCapacity > 0) { "outboundQueueCapacity must be positive" }
        require(outboundQueueByteCapacity > 0) { "outboundQueueByteCapacity must be positive" }
        require(shutdownTimeout.isPositive() && shutdownTimeout.isFinite()) {
            "shutdownTimeout must be positive and finite"
        }
    }
}

/** Ktor application plugin that owns the Hocuspocus WebSocket route and lifecycle. */
public val HocuspocusKtor: ApplicationPlugin<HocuspocusKtorConfiguration> = createApplicationPlugin(
    name = "HocuspocusKtor",
    createConfiguration = ::HocuspocusKtorConfiguration,
) {
    pluginConfig.validate()
    val server = checkNotNull(pluginConfig.configuredServer)
    val routePath = pluginConfig.path
    val contextFactory = pluginConfig.createContext
    val outboundQueueCapacity = pluginConfig.outboundQueueCapacity
    val outboundQueueByteCapacity = pluginConfig.outboundQueueByteCapacity
    val shutdownTimeout = pluginConfig.shutdownTimeout

    if (pluginConfig.installWebSockets && application.pluginOrNull(WebSockets) == null) {
        application.install(WebSockets) {
            pingPeriodMillis = server.configuration.timeout.inWholeMilliseconds / 2
            timeoutMillis = server.configuration.timeout.inWholeMilliseconds
            maxFrameSize = server.configuration.maxFrameSize.toLong()
            masking = false
        }
    }
    check(application.pluginOrNull(WebSockets) != null) {
        "Ktor WebSockets is not installed; enable installWebSockets or install it before HocuspocusKtor"
    }

    application.routing {
        webSocket(routePath) {
            val context = contextFactory(call)
            serveHocuspocus(server, context, outboundQueueCapacity, outboundQueueByteCapacity)
        }
    }
    application.monitor.subscribe(ApplicationStopping) {
        try {
            runBlocking {
                withTimeout(shutdownTimeout) { server.shutdown() }
            }
        } catch (error: Throwable) {
            runCatching { server.configuration.onError(error) }
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
    require(outboundQueueCapacity > 0) { "outboundQueueCapacity must be positive" }
    require(outboundQueueByteCapacity > 0) { "outboundQueueByteCapacity must be positive" }
    val transport = KtorSocketTransport(this, this, outboundQueueCapacity, outboundQueueByteCapacity)
    val coreSession: ClientSession<C> = server.openSession(
        transport = transport,
        request = call.toHocuspocusRequest(),
        initialContext = context,
    )
    try {
        for (frame in incoming) {
            when (frame) {
                // Ktor's WebSocketReader materializes each inbound frame into a
                // fresh ByteArray. Core retains that owned array until its
                // asynchronous per-document queue has consumed the frame.
                is Frame.Binary -> coreSession.handleBinaryOwned(frame.data)
                is Frame.Close -> break
                else -> {
                    transport.close(CloseEvents.ResetConnection.code, "Hocuspocus requires binary frames")
                    break
                }
            }
        }
    } finally {
        coreSession.close()
        transport.finish()
    }
}

private fun ApplicationCall.toHocuspocusRequest(): HocuspocusRequest = HocuspocusRequest(
    uri = request.uri,
    headers = request.headers.names().associateWith { name -> request.headers.getAll(name).orEmpty() },
    parameters = request.queryParameters.names().associateWith { name ->
        request.queryParameters.getAll(name).orEmpty()
    },
    remoteAddress = request.local.remoteAddress,
)

private class KtorSocketTransport(
    private val session: DefaultWebSocketServerSession,
    scope: CoroutineScope,
    capacity: Int,
    private val byteCapacity: Int,
) : SocketTransport {
    private val open: AtomicBoolean = AtomicBoolean(true)
    private val outgoing: Channel<ByteArray> = Channel(capacity)
    private val queuedBytes: AtomicLong = AtomicLong()
    private val closeLock: Any = Any()
    private var closeJob: Job? = null
    private val sender: Job = scope.launch {
        try {
            for (bytes in outgoing) {
                queuedBytes.addAndGet(-bytes.size.toLong())
                session.send(Frame.Binary(fin = true, data = bytes))
            }
        } finally {
            open.set(false)
        }
    }

    override val isOpen: Boolean
        get() = open.get()

    override fun send(bytes: ByteArray): Boolean {
        if (!open.get()) return false
        val nextQueuedBytes = queuedBytes.addAndGet(bytes.size.toLong())
        if (nextQueuedBytes > byteCapacity) {
            queuedBytes.addAndGet(-bytes.size.toLong())
            return false
        }
        // Core frames are immutable and remain valid after send(), so the
        // bounded sender queue can retain the shared frame without a
        // per-recipient 1:1 ByteArray copy.
        val result = outgoing.trySend(bytes)
        if (result.isFailure) queuedBytes.addAndGet(-bytes.size.toLong())
        return result.isSuccess
    }

    override fun close(code: Int, reason: String) {
        synchronized(closeLock) {
            if (!open.compareAndSet(true, false)) return
            outgoing.close()
            closeJob = session.launch {
                session.close(CloseReason(code.toShort(), reason.take(123)))
            }
        }
    }

    suspend fun finish() {
        val pendingClose = synchronized(closeLock) {
            open.set(false)
            outgoing.close()
            closeJob
        }
        sender.join()
        pendingClose?.join()
    }
}
