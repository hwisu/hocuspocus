package ai.hocuspocus.core

import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.AwarenessCodec
import ai.hocuspocus.protocol.DecodeLimits
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.HocuspocusFrameView
import ai.hocuspocus.protocol.Lib0Writer
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.ProtocolException
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.protocol.ServerAuthentication
import ai.hocuspocus.protocol.SyncCodec
import ai.hocuspocus.protocol.SyncMessage
import ai.hocuspocus.protocol.SyncMessageType
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

public class HocuspocusConnection<C : Any> internal constructor(
    internal val session: ClientSession<C>,
    public val document: HocuspocusDocument<C>,
    public val attempt: ConnectionAttempt<C>,
) {
    public val socketId: String
        get() = attempt.socketId

    public val routingKey: RoutingKey
        get() = attempt.routingKey

    public val context: C
        get() = attempt.context.value

    public val readOnly: Boolean
        get() = attempt.connectionConfiguration.readOnly

    public val providerVersion: String?
        get() = attempt.providerVersion

    private val encodedRoutingKey: String = routingKey.encode()

    public val id: String = "$socketId:$encodedRoutingKey"

    internal val ownedAwarenessClientIds: MutableSet<Long> = linkedSetOf()
    internal val transactionOrigin: TransactionOrigin.Connection =
        TransactionOrigin.Connection(socketId, encodedRoutingKey)
    private val syncStatusSavedFrame: ByteArray =
        FrameCodec.encode(routingKey, MessageType.SyncStatus, byteArrayOf(1))
    private val syncStatusUnsavedFrame: ByteArray =
        FrameCodec.encode(routingKey, MessageType.SyncStatus, byteArrayOf(0))
    private val payloadLimits: DecodeLimits = DecodeLimits(
        maxByteArraySize = session.server.configuration.maxFrameSize,
        maxStringSize = session.server.configuration.maxFrameSize,
        maxAwarenessEntries = session.server.configuration.maxAwarenessEntriesPerMessage,
    )
    private val authenticationLimits: DecodeLimits = DecodeLimits(
        maxByteArraySize = session.server.configuration.maxFrameSize,
        maxStringSize = session.server.configuration.maxAuthenticationStringLength,
        maxAwarenessEntries = session.server.configuration.maxAwarenessEntriesPerMessage,
    )
    private val statelessLimits: DecodeLimits = DecodeLimits(
        maxByteArraySize = session.server.configuration.maxStatelessPayloadSize,
        maxStringSize = session.server.configuration.maxStatelessPayloadSize,
        maxAwarenessEntries = session.server.configuration.maxAwarenessEntriesPerMessage,
    )
    private val awarenessLimits: DecodeLimits = DecodeLimits(
        maxByteArraySize = session.server.configuration.maxAwarenessUpdateSize,
        maxStringSize = session.server.configuration.maxAwarenessUpdateSize,
        maxAwarenessEntries = session.server.configuration.maxAwarenessEntriesPerMessage,
    )

    private val incoming: Channel<InboundFrame> = Channel(
        capacity = session.server.configuration.maxEstablishedQueueMessages,
    )
    private val closed: AtomicBoolean = AtomicBoolean()
    private val discardPendingMessages: AtomicBoolean = AtomicBoolean()
    private val queuedBytes: AtomicLong = AtomicLong()
    private lateinit var processingJob: Job

    internal suspend fun start() {
        document.addConnection(this)
        processingJob = session.server.scope.launch {
            for (message in incoming) {
                queuedBytes.addAndGet(-message.size.toLong())
                if (discardPendingMessages.get()) break
                try {
                    if (!processMessageWithoutSuspension(message)) {
                        processMessage(message)
                    }
                } catch (error: Throwable) {
                    session.server.reportError(error)
                    abort(CloseEvents.ResetConnection)
                }
            }
        }
    }

    internal fun enqueue(message: InboundFrame): Boolean {
        if (closed.get()) return false
        val nextQueuedBytes = queuedBytes.addAndGet(message.size.toLong())
        if (nextQueuedBytes > session.server.configuration.maxEstablishedQueueSize) {
            queuedBytes.addAndGet(-message.size.toLong())
            session.server.scope.launch { abort(CloseEvents.ResetConnection) }
            return false
        }
        val result: ChannelResult<Unit> = incoming.trySend(message)
        if (result.isFailure) {
            queuedBytes.addAndGet(-message.size.toLong())
            session.server.scope.launch { abort(CloseEvents.ResetConnection) }
        }
        return result.isSuccess
    }

    public fun sendStateless(payload: String) {
        sendFrame(
            MessageType.Stateless,
            Lib0Writer().writeVarString(payload).toByteArray(),
        )
    }

    public fun requestToken() {
        sendFrame(
            MessageType.Auth,
            AuthenticationCodec.encodeServer(ServerAuthentication.TokenRequest),
        )
    }

    public suspend fun close(event: CloseEvent = CloseEvents.Normal) {
        closeInternal(event, drainPendingMessages = true)
    }

    internal suspend fun abort(event: CloseEvent = CloseEvents.ResetConnection) {
        closeInternal(event, drainPendingMessages = false)
    }

    private suspend fun closeInternal(
        event: CloseEvent,
        drainPendingMessages: Boolean,
    ) {
        if (!drainPendingMessages) discardPendingMessages.set(true)
        if (!closed.compareAndSet(false, true)) {
            if (!drainPendingMessages) discardQueuedMessages()
            return
        }
        incoming.close()
        if (!drainPendingMessages) discardQueuedMessages()
        if (::processingJob.isInitialized && coroutineContext[Job] !== processingJob) {
            listOf(processingJob).joinAll()
        }
        val lastConnection = document.removeConnection(this)
        sendFrame(
            MessageType.Close,
            Lib0Writer().writeVarString(event.reason).toByteArray(),
            allowClosed = true,
        )
        session.removeConnection(this)
        session.server.disconnected(this, lastConnection)
    }

    internal fun sendSyncUpdate(update: ByteArray) {
        sendFrame(MessageType.Sync, SyncCodec.encode(SyncMessageType.Update, update))
    }

    internal fun sendAwarenessUpdate(update: ByteArray) {
        sendFrame(
            MessageType.Awareness,
            Lib0Writer().writeVarByteArray(update).toByteArray(),
        )
    }

    internal fun sendEncodedFrame(frame: ByteArray) {
        if (!closed.get()) session.send(frame)
    }

    private suspend fun processMessage(message: InboundFrame) {
        val frame = message.frame
        if (frame.routingKey.documentName != document.name) return
        if (routingKey.sessionId != null && frame.routingKey != routingKey) return

        val hookPayload = message.rawMessage?.let { MessageHookPayload(this, it) }
        if (hookPayload != null) session.server.beforeHandleMessage(hookPayload)
        try {
            when (frame.type) {
                MessageType.Sync, MessageType.SyncReply -> handleSync(frame)
                MessageType.Awareness -> handleAwareness(frame)
                MessageType.Auth -> handleTokenSync(frame)
                MessageType.QueryAwareness -> sendAwarenessUpdate(document.encodeAwarenessUpdate())
                MessageType.Stateless -> handleStateless(frame)
                MessageType.BroadcastStateless -> throw ProtocolException(
                    "BroadcastStateless is a server-internal opcode and cannot be sent from a client",
                )
                MessageType.Close -> abort(CloseEvent(1000, "provider_initiated"))
                MessageType.Ping -> sendFrame(MessageType.Pong)
                MessageType.Pong, MessageType.SyncStatus -> Unit
            }
        } finally {
            if (hookPayload != null) {
                runCatching { session.server.afterHandleMessage(hookPayload) }
                    .onFailure(session.server::reportError)
            }
        }
    }

    private fun processMessageWithoutSuspension(message: InboundFrame): Boolean {
        val frame = message.frame
        if (frame.routingKey.documentName != document.name) return true
        if (routingKey.sessionId != null && frame.routingKey != routingKey) return true
        if (message.rawMessage != null) return false
        return when (frame.type) {
            MessageType.Sync, MessageType.SyncReply -> {
                if (session.server.hasBeforeSyncHooks) {
                    false
                } else {
                    applySync(decodeSync(frame))
                    true
                }
            }
            MessageType.Pong, MessageType.SyncStatus -> true
            else -> false
        }
    }

    private suspend fun handleSync(frame: HocuspocusFrameView) {
        val message = decodeSync(frame)
        if (session.server.hasBeforeSyncHooks) {
            session.server.beforeSync(
                SyncHookPayload(this, message.type, message.updateOrStateVector.copyOf()),
            )
        }
        applySync(message)
    }

    private fun decodeSync(frame: HocuspocusFrameView): SyncMessage {
        val message: SyncMessage = SyncCodec.decode(frame.payloadReader(payloadLimits))
        if (
            message.type != SyncMessageType.StepOne &&
            message.updateOrStateVector.size > session.server.configuration.maxCrdtUpdateSize
        ) {
            throw ProtocolException("CRDT update exceeds configured size limit")
        }
        return message
    }

    private fun applySync(message: SyncMessage) {
        when (message.type) {
            SyncMessageType.StepOne -> {
                val update = document.updateFor(message.updateOrStateVector)
                sendFrame(MessageType.Sync, SyncCodec.encode(SyncMessageType.StepTwo, update))
                sendFrame(
                    MessageType.Sync,
                    SyncCodec.encode(SyncMessageType.StepOne, document.stateVector()),
                )
            }
            SyncMessageType.StepTwo -> {
                val saved = if (readOnly) {
                    document.containsUpdate(message.updateOrStateVector)
                } else {
                    document.applyClientUpdate(this, message.updateOrStateVector)
                    true
                }
                sendSyncStatus(saved)
            }
            SyncMessageType.Update -> {
                if (!readOnly) {
                    document.applyClientUpdate(this, message.updateOrStateVector)
                }
                sendSyncStatus(!readOnly)
            }
        }
    }

    private suspend fun handleAwareness(frame: HocuspocusFrameView) {
        val reader = frame.payloadReader(awarenessLimits)
        val incomingUpdate = reader.readVarByteArray()
        reader.requireFullyConsumed("awareness message")
        val decoded = AwarenessCodec.decode(incomingUpdate, awarenessLimits)
        document.applyAwareness(this, decoded, transactionOrigin)
    }

    private suspend fun handleTokenSync(frame: HocuspocusFrameView) {
        val authentication = AuthenticationCodec.decodeClient(
            frame.payloadReader(authenticationLimits),
        )
        try {
            session.server.tokenSync(TokenSyncPayload(this, authentication.token))
        } catch (error: Throwable) {
            session.server.reportError(error)
            close(CloseEvents.Unauthorized)
        }
    }

    private suspend fun handleStateless(frame: HocuspocusFrameView) {
        val reader = frame.payloadReader(statelessLimits)
        val payload = reader.readVarString()
        reader.requireFullyConsumed("stateless message")
        session.server.stateless(StatelessPayload(this, payload))
    }

    private fun sendSyncStatus(saved: Boolean) {
        if (!closed.get()) {
            session.send(if (saved) syncStatusSavedFrame else syncStatusUnsavedFrame)
        }
    }

    private fun sendFrame(
        type: MessageType,
        payload: ByteArray = ByteArray(0),
        allowClosed: Boolean = false,
    ) {
        if (closed.get() && !allowClosed) return
        session.send(FrameCodec.encode(routingKey, type, payload))
    }

    private fun discardQueuedMessages() {
        while (true) {
            val message = incoming.tryReceive().getOrNull() ?: break
            queuedBytes.addAndGet(-message.size.toLong())
        }
        queuedBytes.set(0)
    }
}
