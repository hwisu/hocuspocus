package ai.hocuspocus.redis

import ai.hocuspocus.core.AwarenessUpdatePayload
import ai.hocuspocus.core.BroadcastStatelessPayload
import ai.hocuspocus.core.ChangePayload
import ai.hocuspocus.core.ConfigurePayload
import ai.hocuspocus.core.DocumentHookPayload
import ai.hocuspocus.core.HocuspocusDocument
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.SkipFurtherHooksException
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.core.TransactionOrigin
import ai.hocuspocus.core.UnloadDocumentPayload
import ai.hocuspocus.protocol.AwarenessCodec
import ai.hocuspocus.protocol.DecodeLimits
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.Lib0Reader
import ai.hocuspocus.protocol.Lib0Writer
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.protocol.SyncCodec
import ai.hocuspocus.protocol.SyncMessageType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

public data class RedisExtensionConfiguration(
    val prefix: String = "hocuspocus",
    val identifier: String = "host-${UUID.randomUUID()}",
    val lockTimeout: Duration = 1.seconds,
    val disconnectDelay: Duration = 1.seconds,
    val initialSyncTimeout: Duration = 1.seconds,
    val changeFlushTimeout: Duration = 5.seconds,
    val changeRetryDelay: Duration = 100.milliseconds,
    val inboundQueueCapacity: Int = 1_024,
    val maxInboundQueueBytes: Long = 64L * 1024L * 1024L,
    val outboundQueueCapacity: Int = 4_096,
    val maxOutboundQueueBytes: Long = 64L * 1024L * 1024L,
    val publishBatchSize: Int = 256,
) {
    init {
        require(prefix.isNotBlank()) { "prefix must not be blank" }
        require(identifier.isNotBlank()) { "identifier must not be blank" }
        require(identifier.toByteArray(StandardCharsets.UTF_8).size <= 127) {
            "identifier must fit the upstream Redis extension's one-byte UTF-8 length prefix"
        }
        require(lockTimeout.isPositive() && lockTimeout.isFinite()) {
            "lockTimeout must be positive and finite"
        }
        require(!disconnectDelay.isNegative() && disconnectDelay.isFinite()) {
            "disconnectDelay must be finite and not negative"
        }
        require(!initialSyncTimeout.isNegative() && initialSyncTimeout.isFinite()) {
            "initialSyncTimeout must be finite and not negative"
        }
        require(changeFlushTimeout.isPositive() && changeFlushTimeout.isFinite()) {
            "changeFlushTimeout must be positive and finite"
        }
        require(changeRetryDelay.isPositive() && changeRetryDelay.isFinite()) {
            "changeRetryDelay must be positive and finite"
        }
        require(inboundQueueCapacity > 0) { "inboundQueueCapacity must be positive" }
        require(maxInboundQueueBytes > 0) { "maxInboundQueueBytes must be positive" }
        require(outboundQueueCapacity > 0) { "outboundQueueCapacity must be positive" }
        require(maxOutboundQueueBytes > 0) { "maxOutboundQueueBytes must be positive" }
        require(publishBatchSize > 0) { "publishBatchSize must be positive" }
    }
}

public class RedisExtension<C : Any>(
    private val busFactory: RedisBusFactory,
    public val configuration: RedisExtensionConfiguration = RedisExtensionConfiguration(),
) : HocuspocusExtension<C> {
    public constructor(
        redisUri: String,
        configuration: RedisExtensionConfiguration = RedisExtensionConfiguration(),
    ) : this(RedisBusFactory { LettuceRedisBus.connect(redisUri) }, configuration)

    override val priority: Int = 1_000
    override val name: String = "redis"

    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val documents: ConcurrentHashMap<String, HocuspocusDocument<C>> = ConcurrentHashMap()
    private val inboxes: ConcurrentHashMap<String, RedisInbox> = ConcurrentHashMap()
    private val changePublishers: ConcurrentHashMap<String, ChangePublisher<C>> = ConcurrentHashMap()
    private val outbound: Channel<RedisPublication> = Channel(configuration.outboundQueueCapacity)
    private val outboundBytes: AtomicLong = AtomicLong()
    private val outboundStopped: AtomicBoolean = AtomicBoolean()
    private val pendingInitialSync: ConcurrentHashMap<String, CompletableDeferred<Unit>> = ConcurrentHashMap()
    private val locks: ConcurrentHashMap<String, String> = ConcurrentHashMap()
    private lateinit var server: HocuspocusServer<C>
    private lateinit var bus: RedisBus
    private lateinit var publisherJob: Job

    override suspend fun onConfigure(payload: ConfigurePayload<C>) {
        server = payload.server
        if (!::bus.isInitialized) bus = busFactory.create()
        if (!::publisherJob.isInitialized) {
            publisherJob = scope.launch { publishResponses() }
        }
    }

    override suspend fun afterLoadDocument(payload: DocumentHookPayload<C>) {
        val document = payload.document
        documents[document.name] = document
        val inbox = createInbox(document.name)
        if (inboxes.putIfAbsent(document.name, inbox) != null) {
            documents.remove(document.name, document)
            stopInbox(inbox)
            error("Redis inbox already exists for ${document.name}")
        }
        val changePublisher = createChangePublisher(document)
        if (changePublishers.putIfAbsent(document.name, changePublisher) != null) {
            documents.remove(document.name, document)
            inboxes.remove(document.name, inbox)
            stopInbox(inbox)
            stopChangePublisher(changePublisher)
            error("Redis change publisher already exists for ${document.name}")
        }
        try {
            bus.subscribe(channel(document.name)) { message ->
                enqueue(inbox, message)
            }
            val waitForPeer = configuration.initialSyncTimeout.isPositive() &&
                runCatching { bus.subscriberCount(channel(document.name)) > 1L }
                    .getOrDefault(true)
            val completion = if (waitForPeer) {
                CompletableDeferred<Unit>().also { pendingInitialSync[document.name] = it }
            } else {
                null
            }
            publishSync(
                document,
                SyncMessageType.StepOne,
                document.encodeStateVector(),
            )
            publishFrame(document.name, MessageType.QueryAwareness)
            completion?.let {
                withTimeoutOrNull(configuration.initialSyncTimeout) { it.await() }
                pendingInitialSync.remove(document.name, it)
            }
        } catch (error: Throwable) {
            documents.remove(document.name, document)
            pendingInitialSync.remove(document.name)?.cancel()
            runCatching { bus.unsubscribe(channel(document.name)) }
            inboxes.remove(document.name, inbox)
            stopInbox(inbox)
            changePublishers.remove(document.name, changePublisher)
            stopChangePublisher(changePublisher)
            throw error
        }
    }

    override suspend fun onChange(payload: ChangePayload<C>) {
        if (payload.transactionOrigin == TransactionOrigin.Redis) return
        changePublishers[payload.document.name]?.signal(payload.update)
    }

    override suspend fun onAwarenessUpdate(payload: AwarenessUpdatePayload<C>) {
        if (payload.transactionOrigin == TransactionOrigin.Redis) return
        if (payload.document.connections().isEmpty()) return
        val update = payload.document.encodeAwarenessUpdate(payload.change.changedClients)
        publishFrame(
            payload.document.name,
            MessageType.Awareness,
            Lib0Writer().writeVarByteArray(update).toByteArray(),
        )
    }

    override suspend fun beforeBroadcastStateless(payload: BroadcastStatelessPayload<C>) {
        publishFrame(
            payload.document.name,
            MessageType.BroadcastStateless,
            Lib0Writer().writeVarString(payload.payload).toByteArray(),
        )
    }

    override suspend fun onStoreDocument(payload: StorePayload<C>) {
        val key = lockKey(payload.document.name)
        val token = UUID.randomUUID().toString()
        if (!bus.tryAcquireLock(key, token, configuration.lockTimeout)) {
            throw SkipFurtherHooksException()
        }
        locks[key] = token
    }

    override suspend fun afterStoreDocument(payload: StorePayload<C>) {
        val key = lockKey(payload.document.name)
        locks.remove(key)?.let { token ->
            runCatching { bus.releaseLock(key, token) }
                .onFailure(server.configuration.onError)
        }
        if (
            payload.lastTransactionOrigin is TransactionOrigin.Local &&
            configuration.disconnectDelay.isPositive()
        ) {
            delay(configuration.disconnectDelay)
        }
    }

    override suspend fun beforeUnloadDocument(payload: UnloadDocumentPayload<C>) {
        flushChangePublisher(changePublishers[payload.document.name])
        if (configuration.disconnectDelay.isPositive()) delay(configuration.disconnectDelay)
    }

    override suspend fun afterUnloadDocument(payload: UnloadDocumentPayload<C>) {
        val document = payload.document
        if (server.document(document.name) != null) return
        documents.remove(document.name, document)
        pendingInitialSync.remove(document.name)?.cancel()
        runCatching { bus.unsubscribe(channel(document.name)) }
            .onFailure(server.configuration.onError)
        inboxes.remove(document.name)?.let { stopInbox(it) }
        changePublishers.remove(document.name)?.let { stopChangePublisher(it) }
    }

    override suspend fun onDestroy(server: HocuspocusServer<C>) {
        val failures = mutableListOf<Throwable>()
        pendingInitialSync.values.forEach { it.cancel() }
        pendingInitialSync.clear()
        inboxes.values.forEach { stopInbox(it) }
        inboxes.clear()
        changePublishers.values.forEach { publisher ->
            try {
                flushChangePublisher(publisher)
            } catch (error: Throwable) {
                failures += error
            }
            try {
                stopChangePublisher(publisher)
            } catch (error: Throwable) {
                failures += error
            }
        }
        changePublishers.clear()
        outboundStopped.set(true)
        outbound.close()
        if (::publisherJob.isInitialized) {
            val drained = withTimeoutOrNull(configuration.changeFlushTimeout) {
                publisherJob.join()
                true
            } ?: false
            if (!drained) {
                publisherJob.cancel()
                failures += IllegalStateException(
                    "Redis outbound response publisher did not drain within " +
                        configuration.changeFlushTimeout,
                )
            }
        }
        scope.cancel()
        if (::bus.isInitialized) {
            try {
                bus.close()
            } catch (error: Throwable) {
                failures += error
            }
        }
        if (failures.isNotEmpty()) {
            throw IllegalStateException(
                "Redis extension shutdown failed with ${failures.size} error(s)",
                failures.first(),
            ).also { failure ->
                failures.drop(1).forEach(failure::addSuppressed)
            }
        }
    }

    private fun createInbox(documentName: String): RedisInbox {
        val inbox = RedisInbox(
            documentName = documentName,
            channel = Channel(configuration.inboundQueueCapacity),
        )
        inbox.job = scope.launch(start = CoroutineStart.UNDISPATCHED) {
            for (message in inbox.channel) {
                inbox.queuedBytes.addAndGet(-message.size.toLong())
                try {
                    handleIncoming(message)
                } catch (error: Throwable) {
                    if (error is CancellationException) throw error
                    reportError(error)
                }
            }
        }
        return inbox
    }

    private fun createChangePublisher(document: HocuspocusDocument<C>): ChangePublisher<C> {
        val publisher = ChangePublisher(document, Channel(Channel.CONFLATED))
        publisher.job = scope.launch(start = CoroutineStart.UNDISPATCHED) {
            for (ignored in publisher.signals) {
                do {
                    val notification = publisher.latest.get() ?: break
                    val publishedGeneration = publisher.publishedGeneration.get()
                    if (notification.generation <= publishedGeneration) break
                    val hasContiguousUpdate =
                        notification.generation == publishedGeneration + 1
                    try {
                        if (hasContiguousUpdate) {
                            publishSync(
                                publisher.document,
                                SyncMessageType.Update,
                                notification.update,
                            )
                        } else {
                            publishSync(
                                publisher.document,
                                SyncMessageType.StepOne,
                                publisher.document.encodeStateVector(),
                            )
                        }
                        publisher.publishedGeneration.set(notification.generation)
                    } catch (error: Throwable) {
                        if (error is CancellationException) throw error
                        reportError(error)
                        scope.launch {
                            runCatching { server.closeConnections(publisher.document.name) }
                                .onFailure(::reportError)
                        }
                        delay(configuration.changeRetryDelay)
                        if (!publisher.stopped.get()) publisher.signals.trySend(Unit)
                    }
                } while (
                    !publisher.stopped.get() &&
                    (publisher.latest.get()?.generation ?: 0) > notification.generation
                )
            }
        }
        return publisher
    }

    private suspend fun flushChangePublisher(publisher: ChangePublisher<C>?) {
        publisher ?: return
        val targetGeneration = publisher.latest.get()?.generation ?: 0
        val flushed = withTimeoutOrNull(configuration.changeFlushTimeout) {
            while (
                !publisher.stopped.get() &&
                publisher.job.isActive &&
                publisher.publishedGeneration.get() < targetGeneration
            ) {
                delay(1)
            }
            publisher.publishedGeneration.get() >= targetGeneration
        } ?: false
        if (!flushed) {
            throw IllegalStateException(
                "Redis change publisher for ${publisher.document.name} did not flush " +
                    "generation $targetGeneration within ${configuration.changeFlushTimeout}",
            )
        }
    }

    private suspend fun stopChangePublisher(publisher: ChangePublisher<C>) {
        publisher.stopped.set(true)
        publisher.signals.cancel()
        if (publisher.job.isActive) publisher.job.cancelAndJoin()
    }

    private fun enqueue(inbox: RedisInbox, message: ByteArray) {
        if (inbox.stopped.get()) return
        if (!reserveBytes(inbox, message.size.toLong())) {
            failInbox(
                inbox,
                IllegalStateException(
                    "Redis inbound queue for ${inbox.documentName} exceeded " +
                        "${configuration.maxInboundQueueBytes} bytes",
                ),
            )
            return
        }
        if (inbox.channel.trySend(message).isFailure) {
            inbox.queuedBytes.addAndGet(-message.size.toLong())
            if (!inbox.stopped.get()) {
                failInbox(
                    inbox,
                    IllegalStateException(
                        "Redis inbound queue for ${inbox.documentName} exceeded " +
                            "${configuration.inboundQueueCapacity} messages",
                    ),
                )
            }
        }
    }

    private fun reserveBytes(inbox: RedisInbox, byteCount: Long): Boolean {
        while (true) {
            val current = inbox.queuedBytes.get()
            val next = current + byteCount
            if (next < current || next > configuration.maxInboundQueueBytes) return false
            if (inbox.queuedBytes.compareAndSet(current, next)) return true
        }
    }

    private fun failInbox(inbox: RedisInbox, error: Throwable) {
        if (!inbox.stopped.compareAndSet(false, true)) return
        inbox.channel.cancel()
        scope.launch {
            reportError(error)
            runCatching { server.closeConnections(inbox.documentName) }
                .onFailure(::reportError)
        }
    }

    private suspend fun stopInbox(inbox: RedisInbox) {
        inbox.stopped.set(true)
        inbox.channel.cancel()
        if (::server.isInitialized && inbox.job.isActive) inbox.job.cancelAndJoin()
    }

    private fun reportError(error: Throwable) {
        try {
            server.configuration.onError(error)
        } catch (reportingError: Throwable) {
            if (reportingError !== error) reportingError.addSuppressed(error)
            System.getLogger("ai.hocuspocus.redis")
                .log(System.Logger.Level.ERROR, "Hocuspocus onError callback failed", reportingError)
        }
    }

    private suspend fun handleIncoming(message: ByteArray) {
        val envelope = Lib0Reader(message, redisDecodeLimits())
        val sender = envelope.readVarString()
        if (sender == configuration.identifier) return
        val frame = FrameCodec.decode(envelope.readRemainingBytes(), redisDecodeLimits())
        val document = documents[frame.routingKey.documentName] ?: return
        when (frame.type) {
            MessageType.Sync, MessageType.SyncReply -> {
                val sync = SyncCodec.decode(frame.payload, redisDecodeLimits())
                when (sync.type) {
                    SyncMessageType.StepOne -> {
                        if (frame.type != MessageType.SyncReply) {
                            enqueueSync(
                                document,
                                SyncMessageType.StepOne,
                                document.encodeStateVector(),
                                MessageType.SyncReply,
                            )
                        }
                        enqueueSync(
                            document,
                            SyncMessageType.StepTwo,
                            document.encodeStateAsUpdate(sync.updateOrStateVector),
                        )
                    }
                    SyncMessageType.StepTwo, SyncMessageType.Update -> {
                        document.applyRemoteUpdate(sync.updateOrStateVector)
                        pendingInitialSync[document.name]?.complete(Unit)
                    }
                }
            }
            MessageType.Awareness -> {
                val reader = Lib0Reader(frame.payload, redisDecodeLimits())
                val update = reader.readVarByteArray()
                reader.requireFullyConsumed("Redis awareness message")
                document.applyRemoteAwareness(update)
            }
            MessageType.QueryAwareness -> {
                val update = document.encodeAwarenessUpdate()
                if (AwarenessCodec.decode(update, redisDecodeLimits()).isNotEmpty()) {
                    enqueueFrame(
                        document.name,
                        MessageType.Awareness,
                        Lib0Writer().writeVarByteArray(update).toByteArray(),
                    )
                }
            }
            MessageType.BroadcastStateless -> {
                val reader = Lib0Reader(frame.payload, redisDecodeLimits())
                val payload = reader.readVarString()
                reader.requireFullyConsumed("Redis stateless message")
                document.broadcastRemoteStateless(payload)
            }
            MessageType.Auth,
            MessageType.Close,
            MessageType.Ping,
            MessageType.Pong,
            MessageType.Stateless,
            MessageType.SyncStatus,
            -> Unit
        }
    }

    private suspend fun publishSync(
        document: HocuspocusDocument<C>,
        type: SyncMessageType,
        payload: ByteArray,
        messageType: MessageType = MessageType.Sync,
    ) {
        publishFrame(document.name, messageType, SyncCodec.encode(type, payload))
    }

    private suspend fun publishFrame(
        documentName: String,
        type: MessageType,
        payload: ByteArray = ByteArray(0),
    ) {
        val frame = FrameCodec.encode(RoutingKey(documentName), type, payload)
        val envelope = Lib0Writer()
            .writeVarString(configuration.identifier)
            .writeBytes(frame)
            .toByteArray()
        bus.publish(channel(documentName), envelope)
    }

    private fun enqueueSync(
        document: HocuspocusDocument<C>,
        type: SyncMessageType,
        payload: ByteArray,
        messageType: MessageType = MessageType.Sync,
    ) {
        enqueueFrame(document.name, messageType, SyncCodec.encode(type, payload))
    }

    private fun enqueueFrame(
        documentName: String,
        type: MessageType,
        payload: ByteArray = ByteArray(0),
    ) {
        val frame = FrameCodec.encode(RoutingKey(documentName), type, payload)
        val envelope = Lib0Writer()
            .writeVarString(configuration.identifier)
            .writeBytes(frame)
            .toByteArray()
        val publication = RedisPublication(documentName, channel(documentName), envelope)
        if (outboundStopped.get()) return
        if (!reserveOutboundBytes(envelope.size.toLong())) {
            failOutbound(documentName, "byte capacity")
            return
        }
        if (outbound.trySend(publication).isFailure) {
            outboundBytes.addAndGet(-envelope.size.toLong())
            if (!outboundStopped.get()) failOutbound(documentName, "message capacity")
        }
    }

    private fun reserveOutboundBytes(byteCount: Long): Boolean {
        while (true) {
            val current = outboundBytes.get()
            val next = current + byteCount
            if (next < current || next > configuration.maxOutboundQueueBytes) return false
            if (outboundBytes.compareAndSet(current, next)) return true
        }
    }

    private fun failOutbound(documentName: String, capacity: String) {
        val error = IllegalStateException(
            "Redis outbound response queue for $documentName exceeded configured $capacity",
        )
        scope.launch {
            reportError(error)
            runCatching { server.closeConnections(documentName) }
                .onFailure(::reportError)
        }
    }

    private suspend fun publishResponses() {
        for (first in outbound) {
            val batch = ArrayList<RedisPublication>(configuration.publishBatchSize)
            batch += first
            while (batch.size < configuration.publishBatchSize) {
                val next = outbound.tryReceive().getOrNull() ?: break
                batch += next
            }
            try {
                bus.publishBatch(batch.map { it.channel to it.message })
            } catch (error: Throwable) {
                if (error is CancellationException) throw error
                reportError(error)
                batch.asSequence()
                    .map(RedisPublication::documentName)
                    .distinct()
                    .forEach { documentName ->
                        runCatching { server.closeConnections(documentName) }
                            .onFailure(::reportError)
                    }
            } finally {
                outboundBytes.addAndGet(-batch.sumOf { it.message.size.toLong() })
            }
        }
    }

    private fun channel(documentName: String): String = "${configuration.prefix}:$documentName"

    private fun lockKey(documentName: String): String = "${channel(documentName)}:lock"

    private fun redisDecodeLimits(): DecodeLimits = DecodeLimits(
        maxByteArraySize = server.configuration.maxFrameSize,
        maxStringSize = maxOf(
            server.configuration.maxRoutingKeyLength,
            configuration.identifier.length,
        ),
        maxAwarenessEntries = server.configuration.maxAwarenessEntriesPerMessage,
    )

    private class RedisInbox(
        val documentName: String,
        val channel: Channel<ByteArray>,
        val queuedBytes: AtomicLong = AtomicLong(),
        val stopped: AtomicBoolean = AtomicBoolean(),
    ) {
        lateinit var job: Job
    }

    private data class RedisPublication(
        val documentName: String,
        val channel: String,
        val message: ByteArray,
    )

    private class ChangePublisher<C : Any>(
        val document: HocuspocusDocument<C>,
        val signals: Channel<Unit>,
        val latest: AtomicReference<ChangeNotification?> = AtomicReference(),
        val publishedGeneration: AtomicLong = AtomicLong(),
        val stopped: AtomicBoolean = AtomicBoolean(),
    ) {
        lateinit var job: Job

        fun signal(update: ByteArray) {
            if (stopped.get()) return
            latest.updateAndGet { current ->
                ChangeNotification((current?.generation ?: 0) + 1, update.copyOf())
            }
            signals.trySend(Unit)
        }
    }

    private data class ChangeNotification(
        val generation: Long,
        val update: ByteArray,
    )
}
