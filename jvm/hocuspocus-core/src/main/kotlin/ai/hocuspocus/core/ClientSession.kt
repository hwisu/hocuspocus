package ai.hocuspocus.core

import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.DecodeLimits
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.HocuspocusFrame
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.ServerAuthentication
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

private sealed interface RouteState<C : Any>

internal data class InboundFrame(
    val rawMessage: ByteArray?,
    val frame: HocuspocusFrame,
    val size: Int,
)

private class PendingRoute<C : Any>(
    val attempt: ConnectionAttempt<C>,
) : RouteState<C> {
    val queue: ArrayDeque<InboundFrame> = ArrayDeque()
    var queuedBytes: Int = 0
    var authenticating: Boolean = false
    var authenticationJob: Job? = null
}

private class EstablishedRoute<C : Any>(
    val connection: HocuspocusConnection<C>,
) : RouteState<C>

public class ClientSession<C : Any> internal constructor(
    internal val server: HocuspocusServer<C>,
    private val transport: SocketTransport,
    public val request: HocuspocusRequest,
    public val socketId: String,
    private val initialContext: C,
) {
    private val stateMutex: Mutex = Mutex()
    private val routes: MutableMap<String, RouteState<C>> = linkedMapOf()
    private val closed: AtomicBoolean = AtomicBoolean()
    private val hasAuthenticated: AtomicBoolean = AtomicBoolean()
    private val connectionEstablishedAtNanos: Long = System.nanoTime()
    private val lastMessageReceivedAtNanos: AtomicLong = AtomicLong(connectionEstablishedAtNanos)
    private var totalQueuedBytes: Int = 0
    private var totalQueuedMessages: Int = 0

    private val connectionTimeoutJob: Job = server.scope.launch {
        while (isActive) {
            delay(server.configuration.timeout)
            val referenceTime = if (hasAuthenticated.get()) {
                lastMessageReceivedAtNanos.get()
            } else {
                connectionEstablishedAtNanos
            }
            if (System.nanoTime() - referenceTime >= server.configuration.timeout.inWholeNanoseconds) {
                terminate(CloseEvents.ConnectionTimeout)
                break
            }
        }
    }

    public suspend fun handleBinary(bytes: ByteArray) {
        handleBinary(bytes, ownsBytes = false)
    }

    /**
     * Handles a frame whose byte array is exclusively owned by this session.
     *
     * Transport integrations should prefer this method when their receive API
     * returns a fresh array. It preserves the public [handleBinary] defensive
     * ownership contract while avoiding a redundant inbound copy.
     */
    public suspend fun handleBinaryOwned(bytes: ByteArray) {
        handleBinary(bytes, ownsBytes = true)
    }

    private suspend fun handleBinary(bytes: ByteArray, ownsBytes: Boolean) {
        if (closed.get()) return
        lastMessageReceivedAtNanos.set(System.nanoTime())
        if (bytes.size > server.configuration.maxFrameSize) {
            terminate(CloseEvents.MessageTooBig)
            return
        }
        val frame = try {
            FrameCodec.decode(bytes, frameDecodeLimits())
        } catch (error: Throwable) {
            server.reportError(error)
            terminate(CloseEvents.Unauthorized)
            return
        }
        if (frame.routingKey.documentName.length > server.configuration.maxDocumentNameLength) {
            terminate(CloseEvents.ResetConnection)
            return
        }
        val rawKey = frame.routingKey.encode()
        val inbound = InboundFrame(
            rawMessage = if (server.hasExtensions) {
                if (ownsBytes) bytes else bytes.copyOf()
            } else {
                null
            },
            frame = frame,
            size = bytes.size,
        )
        var established: HocuspocusConnection<C>? = null
        var pendingToAuthenticate: PendingRoute<C>? = null
        var authentication: ClientAuthentication? = null
        var limitExceeded: CloseEvent? = null

        stateMutex.withLock {
            if (closed.get()) return
            val current = routes[rawKey]
                ?: routes[frame.routingKey.documentName].takeIf { it is EstablishedRoute }
            if (current is EstablishedRoute) {
                established = current.connection
                return@withLock
            }
            val pending = current as? PendingRoute ?: run {
                if (routes.size >= server.configuration.maxDocumentsPerSocket) {
                    limitExceeded = CloseEvents.ResetConnection
                    return@withLock
                }
                if (routes.values.count { it is PendingRoute } >= server.configuration.maxPendingDocuments) {
                    limitExceeded = CloseEvents.ResetConnection
                    return@withLock
                }
                PendingRoute(
                    ConnectionAttempt(
                        server = server,
                        request = request,
                        routingKey = frame.routingKey,
                        socketId = socketId,
                        context = MutableContext(initialContext),
                    ),
                ).also { routes[rawKey] = it }
            }

            if (frame.type == MessageType.Auth && !pending.authenticating) {
                authentication = try {
                    AuthenticationCodec.decodeClient(frame.payload, authenticationDecodeLimits())
                } catch (error: Throwable) {
                    server.reportError(error)
                    limitExceeded = CloseEvents.ResetConnection
                    return@withLock
                }
                pending.authenticating = true
                pendingToAuthenticate = pending
                return@withLock
            }

            val nextBytes = totalQueuedBytes + bytes.size
            val nextMessages = totalQueuedMessages + 1
            if (
                nextBytes > server.configuration.maxUnauthenticatedQueueSize ||
                nextMessages > server.configuration.maxUnauthenticatedQueueMessages
            ) {
                limitExceeded = CloseEvents.ResetConnection
                return@withLock
            }
            pending.queue.addLast(inbound)
            pending.queuedBytes += bytes.size
            totalQueuedBytes = nextBytes
            totalQueuedMessages = nextMessages
        }

        limitExceeded?.let {
            terminate(it)
            return
        }
        established?.enqueue(inbound)
        val pending = pendingToAuthenticate ?: return
        val auth = authentication ?: return
        val job = server.scope.launch { authenticateRoute(rawKey, pending, auth) }
        stateMutex.withLock {
            if (routes[rawKey] === pending) pending.authenticationJob = job else job.cancel()
        }
    }

    public suspend fun close() {
        closeInternal(closeTransport = false, event = CloseEvents.Normal)
    }

    public suspend fun terminate(event: CloseEvent) {
        closeInternal(closeTransport = true, event = event)
    }

    internal fun send(bytes: ByteArray) {
        if (closed.get() || !transport.isOpen) return
        if (!transport.send(bytes)) {
            server.scope.launch { terminate(CloseEvents.ResetConnection) }
        }
    }

    internal suspend fun removeConnection(connection: HocuspocusConnection<C>) {
        stateMutex.withLock {
            routes.entries.removeIf { (_, state) ->
                state is EstablishedRoute && state.connection === connection
            }
        }
    }

    private suspend fun authenticateRoute(
        rawKey: String,
        pending: PendingRoute<C>,
        authentication: ClientAuthentication,
    ) {
        try {
            pending.attempt.providerVersion = authentication.providerVersion
            server.connect(pending.attempt)
            server.authenticate(AuthenticatePayload(pending.attempt, authentication.token))
            pending.attempt.connectionConfiguration.isAuthenticated = true
            send(
                FrameCodec.encode(
                    pending.attempt.routingKey,
                    MessageType.Auth,
                    AuthenticationCodec.encodeServer(
                        ServerAuthentication.Authenticated(
                            if (pending.attempt.connectionConfiguration.readOnly) {
                                ai.hocuspocus.protocol.AuthorizedScope.ReadOnly
                            } else {
                                ai.hocuspocus.protocol.AuthorizedScope.ReadWrite
                            },
                        ),
                    ),
                ),
            )

            val document = server.getOrLoadDocument(pending.attempt)
            val connection = HocuspocusConnection(this, document, pending.attempt)
            connection.start()
            var queued: List<InboundFrame> = emptyList()
            var accepted = false
            stateMutex.withLock {
                if (!closed.get() && routes[rawKey] === pending) {
                    queued = pending.queue.toList()
                    totalQueuedBytes -= pending.queuedBytes
                    totalQueuedMessages -= pending.queue.size
                    routes[rawKey] = EstablishedRoute(connection)
                    hasAuthenticated.set(true)
                    accepted = true
                }
            }
            if (!accepted) {
                connection.close()
                return
            }
            queued.forEach(connection::enqueue)
            server.connected(ConnectedPayload(pending.attempt, connection))
        } catch (error: CancellationException) {
            stateMutex.withLock {
                if (routes[rawKey] === pending) {
                    routes.remove(rawKey)
                    totalQueuedBytes -= pending.queuedBytes
                    totalQueuedMessages -= pending.queue.size
                }
            }
            throw error
        } catch (error: Throwable) {
            val authError = error as? HocuspocusAuthenticationException
            val event = authError?.event ?: CloseEvents.Forbidden
            if (authError == null) server.reportError(error)
            send(
                FrameCodec.encode(
                    pending.attempt.routingKey,
                    MessageType.Auth,
                    AuthenticationCodec.encodeServer(
                        ServerAuthentication.PermissionDenied(event.reason.ifBlank { "permission-denied" }),
                    ),
                ),
            )
            stateMutex.withLock {
                if (routes[rawKey] === pending) {
                    routes.remove(rawKey)
                    totalQueuedBytes -= pending.queuedBytes
                    totalQueuedMessages -= pending.queue.size
                }
            }
        }
    }

    private suspend fun closeInternal(closeTransport: Boolean, event: CloseEvent) {
        if (!closed.compareAndSet(false, true)) return
        if (coroutineContext[Job] !== connectionTimeoutJob) connectionTimeoutJob.cancel()
        val connections = stateMutex.withLock {
            val established = routes.values.mapNotNull { (it as? EstablishedRoute)?.connection }
            routes.values.mapNotNull { (it as? PendingRoute)?.authenticationJob }.forEach(Job::cancel)
            routes.clear()
            totalQueuedBytes = 0
            totalQueuedMessages = 0
            established
        }
        connections.forEach { connection ->
            if (closeTransport) connection.abort(event) else connection.close(event)
        }
        if (closeTransport && transport.isOpen) transport.close(event.code, event.reason)
        server.sessionClosed(this)
    }

    private fun frameDecodeLimits(): DecodeLimits = DecodeLimits(
        maxByteArraySize = server.configuration.maxFrameSize,
        maxStringSize = server.configuration.maxRoutingKeyLength,
        maxAwarenessEntries = server.configuration.maxAwarenessEntriesPerMessage,
    )

    private fun authenticationDecodeLimits(): DecodeLimits = DecodeLimits(
        maxByteArraySize = server.configuration.maxFrameSize,
        maxStringSize = server.configuration.maxAuthenticationStringLength,
        maxAwarenessEntries = server.configuration.maxAwarenessEntriesPerMessage,
    )
}
