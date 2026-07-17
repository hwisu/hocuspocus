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
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

public data class RedisExtensionConfiguration(
    val prefix: String = "hocuspocus",
    val identifier: String = "host-${UUID.randomUUID()}",
    val lockTimeout: Duration = 1.seconds,
    val disconnectDelay: Duration = 1.seconds,
    val initialSyncTimeout: Duration = 1.seconds,
) {
    init {
        require(prefix.isNotBlank()) { "prefix must not be blank" }
        require(identifier.isNotBlank()) { "identifier must not be blank" }
        require(lockTimeout.isPositive() && lockTimeout.isFinite()) {
            "lockTimeout must be positive and finite"
        }
        require(!disconnectDelay.isNegative() && disconnectDelay.isFinite()) {
            "disconnectDelay must be finite and not negative"
        }
        require(!initialSyncTimeout.isNegative() && initialSyncTimeout.isFinite()) {
            "initialSyncTimeout must be finite and not negative"
        }
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
    private val documentMutexes: ConcurrentHashMap<String, Mutex> = ConcurrentHashMap()
    private val pendingInitialSync: ConcurrentHashMap<String, CompletableDeferred<Unit>> = ConcurrentHashMap()
    private val locks: ConcurrentHashMap<String, String> = ConcurrentHashMap()
    private lateinit var server: HocuspocusServer<C>
    private lateinit var bus: RedisBus

    override suspend fun onConfigure(payload: ConfigurePayload<C>) {
        server = payload.server
        if (!::bus.isInitialized) bus = busFactory.create()
    }

    override suspend fun afterLoadDocument(payload: DocumentHookPayload<C>) {
        val document = payload.document
        documents[document.name] = document
        try {
            bus.subscribe(channel(document.name)) { message ->
                scope.launch {
                    documentMutex(document.name).withLock {
                        runCatching { handleIncoming(message) }
                            .onFailure(server.configuration.onError)
                    }
                }
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
            throw error
        }
    }

    override suspend fun onChange(payload: ChangePayload<C>) {
        if (payload.transactionOrigin == TransactionOrigin.Redis) return
        publishSync(payload.document, SyncMessageType.Update, payload.update)
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
        if (configuration.disconnectDelay.isPositive()) delay(configuration.disconnectDelay)
    }

    override suspend fun afterUnloadDocument(payload: UnloadDocumentPayload<C>) {
        val document = payload.document
        if (server.document(document.name) != null) return
        documents.remove(document.name, document)
        pendingInitialSync.remove(document.name)?.cancel()
        documentMutexes.remove(document.name)
        runCatching { bus.unsubscribe(channel(document.name)) }
            .onFailure(server.configuration.onError)
    }

    override suspend fun onDestroy(server: HocuspocusServer<C>) {
        pendingInitialSync.values.forEach { it.cancel() }
        pendingInitialSync.clear()
        scope.cancel()
        if (::bus.isInitialized) bus.close()
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
                    SyncMessageType.StepOne -> publishSync(
                        document,
                        SyncMessageType.StepTwo,
                        document.encodeStateAsUpdate(sync.updateOrStateVector),
                    )
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
                    publishFrame(
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
    ) {
        publishFrame(document.name, MessageType.Sync, SyncCodec.encode(type, payload))
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

    private fun channel(documentName: String): String = "${configuration.prefix}:$documentName"

    private fun lockKey(documentName: String): String = "${channel(documentName)}:lock"

    private fun documentMutex(documentName: String): Mutex =
        documentMutexes.computeIfAbsent(documentName) { Mutex() }

    private fun redisDecodeLimits(): DecodeLimits = DecodeLimits(
        maxByteArraySize = server.configuration.maxFrameSize,
        maxStringSize = maxOf(
            server.configuration.maxRoutingKeyLength,
            configuration.identifier.length,
        ),
        maxAwarenessEntries = server.configuration.maxAwarenessEntriesPerMessage,
    )
}
