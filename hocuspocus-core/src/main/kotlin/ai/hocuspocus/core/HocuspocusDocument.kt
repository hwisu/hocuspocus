package ai.hocuspocus.core

import ai.hocuspocus.protocol.AwarenessEntry
import ai.hocuspocus.protocol.AwarenessCodec
import ai.hocuspocus.protocol.DecodeLimits
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.Lib0Writer
import ai.hocuspocus.protocol.MAX_SAFE_INTEGER
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.ProtocolException
import ai.hocuspocus.protocol.SyncMessageType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlinx.serialization.json.JsonElement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.coroutineContext
import kotlin.reflect.KClass
import kotlin.time.Duration

public class HocuspocusDocument<C : Any> internal constructor(
    internal val server: HocuspocusServer<C>,
    public val name: String,
    internal val crdt: CrdtDocument,
) {
    private val mutationLock: ReentrantLock = ReentrantLock()
    private val connections: ConcurrentHashMap<String, HocuspocusConnection<C>> = ConcurrentHashMap()
    private val directConnections: AtomicInteger = AtomicInteger()
    private val pendingChangeHooks: MutableSet<Job> = ConcurrentHashMap.newKeySet()
    private val pendingBroadcastUpdates: MutableList<ByteArray> = mutableListOf()
    private val pendingAwarenessClientIds: MutableSet<Long> = linkedSetOf()
    private var pendingBroadcastBytes: Long = 0
    private var pendingFlushJob: Job? = null

    internal val awareness: AwarenessStore = AwarenessStore()

    @Volatile
    public var isLoading: Boolean = true
        internal set

    @Volatile
    public var isDestroyed: Boolean = false
        private set

    @Volatile
    private var isUnloading: Boolean = false

    @Volatile
    public var lastChangeTimeMillis: Long = 0
        private set

    private val storeScheduler: DocumentStoreScheduler<C> = DocumentStoreScheduler(
        scope = server.scope,
        configuration = server.configuration,
        onStore = { server.storeDocument(this) },
        onError = server::reportError,
    )

    private val awarenessCleanupJob: Job = server.scope.launch {
        val interval = server.configuration.awarenessTimeout / 2
        while (isActive) {
            delay(interval)
            removeStaleAwareness()
        }
    }

    public val connectionsCount: Int
        get() = connections.size + directConnections.get()

    public fun connections(): List<HocuspocusConnection<C>> = connections.values.asSequence().toList()

    public fun encodeStateVector(): ByteArray = withMutationLock {
        crdt.encodeStateVector()
    }

    public fun encodeStateAsUpdate(encodedStateVector: ByteArray = ByteArray(0)): ByteArray = withMutationLock {
        crdt.encodeStateAsUpdate(encodedStateVector)
    }

    public fun isEmpty(fieldName: String): Boolean = withMutationLock {
        crdt.isFieldEmpty(fieldName)
    }

    public suspend fun hasAwarenessStates(): Boolean = withMutationLock {
        awareness.states().isNotEmpty()
    }

    /** Returns the awareness client IDs currently owned by [connection]. */
    public suspend fun getClients(connection: HocuspocusConnection<C>): Set<Long> =
        withMutationLock {
            if (connection.document !== this || connections[connection.id] !== connection) {
                emptySet()
            } else {
                connection.ownedAwarenessClientIds.toSet()
            }
        }

    public suspend fun merge(
        document: HocuspocusDocument<*>,
        context: C? = null,
        skipStoreHooks: Boolean = false,
    ): HocuspocusDocument<C> = merge(listOf(document), context, skipStoreHooks)

    public suspend fun merge(
        documents: Iterable<HocuspocusDocument<*>>,
        context: C? = null,
        skipStoreHooks: Boolean = false,
    ): HocuspocusDocument<C> {
        val updates = documents.map(HocuspocusDocument<*>::encodeStateAsUpdate)
        val origin = TransactionOrigin.Local(context, skipStoreHooks)
        withMutationLock {
            ensureWritable()
            updates.forEach { update ->
                crdt.applyUpdate(update, origin).forEach { emitted ->
                    broadcastUpdate(emitted.data)
                    server.documentUpdated(this, null, context, emitted.data, origin)
                }
            }
        }
        return this
    }

    public suspend fun <N : Any> transact(
        nativeType: KClass<N>,
        context: C? = null,
        skipStoreHooks: Boolean = false,
        mutation: (N) -> Unit,
    ) {
        val origin = TransactionOrigin.Local(context, skipStoreHooks)
        withMutationLock {
            ensureWritable()
            crdt.transact(nativeType, origin, mutation).forEach { emitted ->
                broadcastUpdate(emitted.data)
                server.documentUpdated(this, null, context, emitted.data, origin)
            }
        }
    }

    public suspend fun broadcastStateless(
        payload: String,
        filter: (HocuspocusConnection<C>) -> Boolean = { true },
    ) {
        check(!server.isClosed) { "Hocuspocus server is closed" }
        server.beforeBroadcastStateless(BroadcastStatelessPayload(this, payload))
        broadcastFrame(
            connections.values.filter(filter),
            MessageType.Stateless,
            Lib0Writer().writeVarString(payload).toByteArray(),
        )
    }

    /**
     * Broadcasts a stateless payload received from another collaboration node
     * without invoking [HocuspocusExtension.beforeBroadcastStateless] again.
     */
    public fun broadcastRemoteStateless(
        payload: String,
        filter: (HocuspocusConnection<C>) -> Boolean = { true },
    ) {
        check(!server.isClosed) { "Hocuspocus server is closed" }
        broadcastFrame(
            connections.values.filter(filter),
            MessageType.Stateless,
            Lib0Writer().writeVarString(payload).toByteArray(),
        )
    }

    /** Immediately broadcasts all updates and awareness changes buffered by [HocuspocusConfiguration.flushDelay]. */
    public fun flush(): HocuspocusDocument<C> = withMutationLock {
        pendingFlushJob?.cancel()
        pendingFlushJob = null
        flushPendingBroadcasts()
        this
    }

    /**
     * Applies a standard Yjs V1 update received from another collaboration node.
     * The update is broadcast locally and invokes change hooks with a Redis
     * origin, but does not schedule this node's persistence hooks.
     */
    public suspend fun applyRemoteUpdate(update: ByteArray) {
        validateCrdtUpdate(update)
        val origin = TransactionOrigin.Redis
        withMutationLock {
            ensureWritable()
            crdt.applyUpdate(update, origin).forEach { emitted ->
                broadcastUpdate(emitted.data)
                server.documentUpdated(this, null, null, emitted.data, origin)
            }
        }
    }

    /** Applies and broadcasts a y-protocols awareness update from another node. */
    public suspend fun applyRemoteAwareness(update: ByteArray) {
        if (update.size > server.configuration.maxAwarenessUpdateSize) {
            throw ProtocolException("awareness update exceeds configured size limit")
        }
        val limits = awarenessDecodeLimits()
        val entries = AwarenessCodec.decode(update, limits)
        applyAwareness(null, entries, TransactionOrigin.Redis)
    }

    internal suspend fun addConnection(connection: HocuspocusConnection<C>) {
        val currentAwareness = withMutationLock {
            check(!isDestroyed && !isUnloading) { "Hocuspocus document is unloading" }
            connections[connection.id] = connection
            if (awareness.states().isEmpty()) null else awareness.encode()
        }
        currentAwareness?.let(connection::sendAwarenessUpdate)
    }

    internal suspend fun removeConnection(connection: HocuspocusConnection<C>): Boolean {
        connections.remove(connection.id)
        withMutationLock {
            val change = awareness.remove(connection.ownedAwarenessClientIds)
            connection.ownedAwarenessClientIds.clear()
            if (!change.isEmpty) {
                broadcastAwareness(change)
                server.awarenessUpdated(this, connection, change, null)
            }
        }
        return connectionsCount == 0
    }

    internal fun applyClientUpdate(connection: HocuspocusConnection<C>, update: ByteArray) {
        validateCrdtUpdate(update)
        val origin = connection.transactionOrigin
        withMutationLock {
            ensureWritable()
            crdt.applyUpdate(update, origin).forEach { emitted ->
                broadcastUpdate(emitted.data)
                server.documentUpdated(this, connection, connection.context, emitted.data, origin)
            }
        }
    }

    internal fun containsUpdate(update: ByteArray): Boolean = withMutationLock {
        crdt.containsUpdate(update)
    }

    internal fun updateFor(stateVector: ByteArray): ByteArray = withMutationLock {
        crdt.encodeStateAsUpdate(stateVector)
    }

    internal fun stateVector(): ByteArray = withMutationLock {
        crdt.encodeStateVector()
    }

    internal suspend fun applyAwareness(
        connection: HocuspocusConnection<C>?,
        entries: List<AwarenessEntry>,
        origin: TransactionOrigin,
    ) {
        val states: MutableMap<Long, JsonElement> = linkedMapOf()
        entries.forEach { entry -> entry.state?.let { states[entry.clientId] = it } }
        val ignoredClientIds = linkedSetOf<Long>()
        server.beforeHandleAwareness(
            AwarenessHookPayload(this, connection, states, origin, ignoredClientIds),
        )

        withMutationLock {
            ensureWritable()
            val remainingStates = states.toMutableMap()
            val rewritten = entries.asSequence()
                .filterNot { entry -> entry.clientId in ignoredClientIds }
                .mapNotNull { entry ->
                if (entry.state == null) {
                    entry
                } else {
                    remainingStates.remove(entry.clientId)?.let { state ->
                        AwarenessEntry(entry.clientId, entry.clock, state)
                    }
                }
            }.toMutableList()
            remainingStates.forEach { (clientId, state) ->
                val nextClock = (awareness.currentClock(clientId) ?: 0L) + 1L
                if (nextClock > MAX_SAFE_INTEGER) {
                    throw ProtocolException("awareness clock exceeds JavaScript's MAX_SAFE_INTEGER")
                }
                rewritten += AwarenessEntry(clientId, nextClock, state)
            }
            val ownedEntries = filterForeignAwarenessEntries(connection, rewritten)
            validateAwarenessLimits(connection, ownedEntries)
            val change = awareness.apply(ownedEntries)
            if (connection != null) {
                change.added.forEach(connection.ownedAwarenessClientIds::add)
                change.removed.forEach(connection.ownedAwarenessClientIds::remove)
            }
            if (!change.isEmpty) {
                broadcastAwareness(change)
                server.awarenessUpdated(this, connection, change, origin)
            }
        }
    }

    public suspend fun awarenessStates(): Map<Long, JsonElement> = withMutationLock {
        awareness.states()
    }

    public suspend fun encodeAwarenessUpdate(
        clientIds: Collection<Long>? = null,
    ): ByteArray = withMutationLock {
        if (clientIds == null) awareness.encode() else awareness.encode(clientIds)
    }

    internal suspend fun addDirectConnection() {
        withMutationLock {
            check(!isDestroyed && !isUnloading) { "Hocuspocus document is unloading" }
            directConnections.incrementAndGet()
        }
    }

    internal fun removeDirectConnection(): Boolean {
        directConnections.updateAndGet { current -> if (current > 0) current - 1 else 0 }
        return connectionsCount == 0
    }

    internal fun markDirtyAndSchedule(context: C?, origin: TransactionOrigin) {
        storeScheduler.markDirty(context, origin)
    }

    internal suspend fun flushStore() {
        storeScheduler.flush()
    }

    internal suspend fun performStore(block: suspend (StorePayload<C>) -> Unit) {
        storeScheduler.performStore { context, origin ->
            block(StorePayload(this, context, origin))
        }
    }

    internal fun isDirty(): Boolean = storeScheduler.isDirty()

    internal fun trackChangeHook(job: Job) {
        pendingChangeHooks += job
        job.invokeOnCompletion {
            pendingChangeHooks -= job
        }
    }

    internal suspend fun awaitMutations() {
        withMutationLock { }
        val currentJob = coroutineContext[Job]
        while (true) {
            val pending = pendingChangeHooks.filterNot { it === currentJob }
            if (pending.isEmpty()) return
            pending.forEach { it.join() }
            withMutationLock { }
        }
    }

    internal suspend fun beginUnload(force: Boolean): Boolean = withMutationLock {
        if (isDestroyed || isUnloading) return@withMutationLock false
        if (!force && (connectionsCount > 0 || isDirty())) return@withMutationLock false
        isUnloading = true
        true
    }

    internal suspend fun cancelUnload() {
        withMutationLock {
            if (!isDestroyed) isUnloading = false
        }
    }

    internal suspend fun destroy() {
        if (isDestroyed) return
        awarenessCleanupJob.cancel()
        storeScheduler.cancelPendingStore()
        withMutationLock {
            if (isDestroyed) return
            isUnloading = true
            pendingFlushJob?.cancel()
            pendingFlushJob = null
            flushPendingBroadcasts()
            crdt.close()
            isDestroyed = true
        }
    }

    private fun broadcastUpdate(update: ByteArray) {
        lastChangeTimeMillis = System.currentTimeMillis()
        if (server.configuration.flushDelay == null) {
            broadcastUpdateNow(update)
            return
        }
        pendingBroadcastUpdates += update
        pendingBroadcastBytes += update.size.toLong()
        if (pendingBroadcastBytes >= server.configuration.flushMaxBytes.toLong()) {
            pendingFlushJob?.cancel()
            pendingFlushJob = null
            flushPendingBroadcasts()
        } else {
            scheduleFlush()
        }
    }

    private fun broadcastUpdateNow(update: ByteArray) {
        broadcastEncoded(connections.values) { routingKey ->
            FrameCodec.encodeSync(routingKey, SyncMessageType.Update, update)
        }
    }

    private fun broadcastAwareness(change: AwarenessChange) {
        if (server.configuration.flushDelay == null) {
            broadcastAwarenessNow(change.changedClients)
            return
        }
        pendingAwarenessClientIds += change.changedClients
        scheduleFlush()
    }

    private fun broadcastAwarenessNow(clientIds: Collection<Long>) {
        val encoded = awareness.encode(clientIds)
        broadcastFrame(
            connections.values,
            MessageType.Awareness,
            Lib0Writer().writeVarByteArray(encoded).toByteArray(),
        )
    }

    private fun scheduleFlush() {
        if (pendingFlushJob != null) return
        val flushDelay = checkNotNull(server.configuration.flushDelay)
        lateinit var scheduled: Job
        scheduled = server.scope.launch(start = kotlinx.coroutines.CoroutineStart.LAZY) {
            try {
                if (flushDelay == Duration.ZERO) yield() else delay(flushDelay)
                withMutationLock {
                    if (pendingFlushJob !== scheduled) return@withMutationLock
                    pendingFlushJob = null
                    flushPendingBroadcasts()
                }
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                server.reportError(error)
            }
        }
        pendingFlushJob = scheduled
        scheduled.start()
    }

    private fun flushPendingBroadcasts() {
        if (pendingBroadcastUpdates.isNotEmpty()) {
            val update = if (pendingBroadcastUpdates.size == 1) {
                pendingBroadcastUpdates.single()
            } else {
                crdt.mergeUpdates(pendingBroadcastUpdates)
            }
            pendingBroadcastUpdates.clear()
            pendingBroadcastBytes = 0L
            broadcastUpdateNow(update)
        }
        if (pendingAwarenessClientIds.isNotEmpty()) {
            val clients = pendingAwarenessClientIds.toList()
            pendingAwarenessClientIds.clear()
            broadcastAwarenessNow(clients)
        }
    }

    private fun broadcastFrame(
        recipients: Collection<HocuspocusConnection<C>>,
        type: MessageType,
        payload: ByteArray,
    ) {
        broadcastEncoded(recipients) { routingKey ->
            FrameCodec.encode(routingKey, type, payload)
        }
    }

    private inline fun broadcastEncoded(
        recipients: Collection<HocuspocusConnection<C>>,
        encode: (ai.hocuspocus.protocol.RoutingKey) -> ByteArray,
    ) {
        val iterator = recipients.iterator()
        if (!iterator.hasNext()) return

        val firstConnection = iterator.next()
        val firstRoutingKey = firstConnection.routingKey
        val firstFrame = encode(firstRoutingKey)
        firstConnection.sendEncodedFrame(firstFrame)

        var framesByRoutingKey: HashMap<ai.hocuspocus.protocol.RoutingKey, ByteArray>? = null
        while (iterator.hasNext()) {
            val connection = iterator.next()
            val routingKey = connection.routingKey
            val frame = if (routingKey == firstRoutingKey) {
                firstFrame
            } else {
                val cachedFrames = framesByRoutingKey
                    ?: HashMap<ai.hocuspocus.protocol.RoutingKey, ByteArray>().also { cache ->
                        cache[firstRoutingKey] = firstFrame
                        framesByRoutingKey = cache
                    }
                cachedFrames.getOrPut(routingKey) { encode(routingKey) }
            }
            connection.sendEncodedFrame(frame)
        }
    }

    private suspend fun removeStaleAwareness() {
        withMutationLock {
            val stale = awareness.staleClientIds(server.configuration.awarenessTimeout.inWholeMilliseconds)
            if (stale.isNotEmpty()) {
                val change = awareness.remove(stale)
                connections.values.forEach { it.ownedAwarenessClientIds.removeAll(stale.toSet()) }
                broadcastAwareness(change)
                server.awarenessUpdated(this, null, change, null)
            }
            val metadataRetention = server.configuration.awarenessMetadataRetention
            if (metadataRetention.isFinite()) {
                awareness.pruneInactiveMetadata(metadataRetention.inWholeMilliseconds)
            }
        }
    }

    private fun validateCrdtUpdate(update: ByteArray) {
        if (update.size > server.configuration.maxCrdtUpdateSize) {
            throw ProtocolException(
                "CRDT update size ${update.size} exceeds configured limit " +
                    server.configuration.maxCrdtUpdateSize,
            )
        }
    }

    private fun ensureWritable() {
        check(!server.isClosed) { "Hocuspocus server is closed" }
        check(!isDestroyed) { "Hocuspocus document is destroyed" }
        check(!isUnloading) { "Hocuspocus document is unloading" }
    }

    private inline fun <T> withMutationLock(block: () -> T): T = mutationLock.withLock(block)

    private fun validateAwarenessLimits(
        connection: HocuspocusConnection<C>?,
        entries: Collection<AwarenessEntry>,
    ) {
        if (entries.size > server.configuration.maxAwarenessEntriesPerMessage) {
            throw ProtocolException("awareness entry count exceeds configured message limit")
        }

        val projectedKnown = awareness.projectedKnownClientIds(entries)
        if (projectedKnown.size > server.configuration.maxAwarenessClientsPerDocument) {
            throw ProtocolException("awareness client count exceeds configured document limit")
        }
        if (
            awareness.projectedActiveClientIds(entries).size >
            server.configuration.maxAwarenessClientsPerDocument
        ) {
            throw ProtocolException("active awareness client count exceeds configured document limit")
        }

        connection ?: return
        val projectedOwned = connection.ownedAwarenessClientIds.toMutableSet()
        entries.forEach { entry ->
            if (entry.state == null) projectedOwned.remove(entry.clientId) else projectedOwned.add(entry.clientId)
        }
        if (projectedOwned.size > server.configuration.maxAwarenessClientsPerConnection) {
            throw ProtocolException("awareness client count exceeds configured connection limit")
        }
    }

    private fun filterForeignAwarenessEntries(
        connection: HocuspocusConnection<C>?,
        entries: Collection<AwarenessEntry>,
    ): List<AwarenessEntry> {
        connection ?: return entries.toList()
        val clientsOwnedByOtherConnections = connections.values
            .asSequence()
            .filter { candidate -> candidate !== connection }
            .flatMap { candidate -> candidate.ownedAwarenessClientIds.asSequence() }
            .toSet()
        return entries.filter { entry ->
            entry.clientId !in clientsOwnedByOtherConnections ||
                entry.clientId in connection.ownedAwarenessClientIds
        }
    }

    private fun awarenessDecodeLimits(): DecodeLimits = DecodeLimits(
        maxByteArraySize = server.configuration.maxAwarenessUpdateSize,
        maxStringSize = server.configuration.maxAwarenessUpdateSize,
        maxAwarenessEntries = server.configuration.maxAwarenessEntriesPerMessage,
    )
}
