package ai.hocuspocus.core

import ai.hocuspocus.protocol.ProtocolException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

public class HocuspocusServer<C : Any>(
    public val configuration: HocuspocusConfiguration<C>,
    parentScope: CoroutineScope? = null,
) : AutoCloseable {
    private val parentContext = parentScope?.coroutineContext ?: Dispatchers.Default
    internal val scope: CoroutineScope = CoroutineScope(parentContext + SupervisorJob(parentContext[Job]))
    internal val extensions: List<HocuspocusExtension<C>> = configuration.extensions
        .sortedByDescending(HocuspocusExtension<C>::priority)
    private val extensionsByHook: Map<ExtensionHook, List<HocuspocusExtension<C>>> =
        ExtensionHook.entries.associateWith { hook ->
            extensions.filter { extension -> extension.implements(hook) }
        }
    internal val hasMessageHooks: Boolean =
        extensionsByHook.getValue(ExtensionHook.BeforeHandleMessage).isNotEmpty() ||
            extensionsByHook.getValue(ExtensionHook.AfterHandleMessage).isNotEmpty()
    internal val hasBeforeSyncHooks: Boolean =
        extensionsByHook.getValue(ExtensionHook.BeforeSync).isNotEmpty()
    internal val hasChangeHooks: Boolean =
        extensionsByHook.getValue(ExtensionHook.OnChange).isNotEmpty()

    private val started: AtomicBoolean = AtomicBoolean()
    private val closed: AtomicBoolean = AtomicBoolean()
    private val shutdownComplete: AtomicBoolean = AtomicBoolean()
    private val startMutex: Mutex = Mutex()
    private val lifecycleMutex: Mutex = Mutex()
    private val shutdownMutex: Mutex = Mutex()
    private val documentsMutex: Mutex = Mutex()
    private val documents: ConcurrentHashMap<String, HocuspocusDocument<C>> = ConcurrentHashMap()
    private val loadingDocuments: MutableMap<String, CompletableDeferred<HocuspocusDocument<C>>> = linkedMapOf()
    private val unloadingDocuments: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val sessions: ConcurrentHashMap<String, ClientSession<C>> = ConcurrentHashMap()
    private val directConnections: ConcurrentHashMap<String, DirectConnection<C>> = ConcurrentHashMap()

    public val isStarted: Boolean
        get() = started.get()

    public val isClosed: Boolean
        get() = closed.get()

    public val documentsCount: Int
        get() = documents.size

    public val connectionsCount: Int
        get() {
            val socketIds = documents.values
                .asSequence()
                .flatMap { it.connections().asSequence() }
                .map(HocuspocusConnection<C>::socketId)
                .toSet()
            return socketIds.size + directConnections.size
        }

    public suspend fun start() {
        startMutex.withLock {
            check(!closed.get()) { "Hocuspocus server is closed" }
            if (!started.compareAndSet(false, true)) return
            try {
                runHooks(ExtensionHook.OnConfigure) { extension ->
                    extension.onConfigure(ConfigurePayload(this, JVM_PROTOCOL_VERSION))
                }
            } catch (error: Throwable) {
                started.set(false)
                throw error
            }
        }
    }

    public suspend fun openSession(
        transport: SocketTransport,
        request: HocuspocusRequest,
        initialContext: C,
        socketId: String = UUID.randomUUID().toString(),
    ): ClientSession<C> {
        start()
        return lifecycleMutex.withLock {
            check(!closed.get()) { "Hocuspocus server is closed" }
            val session = ClientSession(this, transport, request, socketId, initialContext)
            check(sessions.putIfAbsent(socketId, session) == null) { "duplicate socketId $socketId" }
            session
        }
    }

    public suspend fun document(name: String): HocuspocusDocument<C>? = documentsMutex.withLock {
        documents[name]
    }

    /** Returns a detached snapshot suitable for health and administration endpoints. */
    public suspend fun documentNames(): Set<String> = documentsMutex.withLock {
        documents.keys.toSortedSet()
    }

    public suspend fun openDirectConnection(
        documentName: String,
        context: C,
    ): DirectConnection<C> {
        require(documentName.isNotBlank()) { "document name must not be blank" }
        require(documentName.length <= configuration.maxDocumentNameLength) {
            "document name exceeds maxDocumentNameLength"
        }
        start()
        val attempt = ConnectionAttempt(
            server = this,
            request = HocuspocusRequest("http://localhost"),
            routingKey = ai.hocuspocus.protocol.RoutingKey(documentName),
            socketId = UUID.randomUUID().toString(),
            context = MutableContext(context),
            connectionConfiguration = ConnectionConfiguration(
                readOnly = false,
                isAuthenticated = true,
            ),
        )
        val document = getOrLoadDocument(attempt)
        return lifecycleMutex.withLock {
            check(!closed.get()) { "Hocuspocus server is closed" }
            document.addDirectConnection()
            val connection = DirectConnection(this, document, context)
            directConnections[connection.id] = connection
            connection
        }
    }

    public suspend fun flushPendingStores() {
        val snapshot = documentsMutex.withLock { documents.values.asSequence().toList() }
        val failures = mutableListOf<Throwable>()
        snapshot.forEach { document ->
            try {
                document.awaitMutations()
                if (document.isDirty()) {
                    document.flushStore()
                }
            } catch (error: Throwable) {
                failures += error
            }
        }
        if (failures.isNotEmpty()) throw HocuspocusPersistenceException(failures)
    }

    /**
     * Gracefully closes every WebSocket document connection, or only those
     * attached to [documentName]. Direct connections are not affected.
     */
    public suspend fun closeConnections(
        documentName: String? = null,
        event: CloseEvent = CloseEvents.ResetConnection,
    ) {
        val targets = documentsMutex.withLock {
            if (documentName == null) {
                documents.values.flatMap(HocuspocusDocument<C>::connections)
            } else {
                documents[documentName]?.connections().orEmpty()
            }
        }
        targets.forEach { connection -> connection.close(event) }
    }

    public suspend fun shutdown() {
        shutdownMutex.withLock {
            if (shutdownComplete.get()) return
            lifecycleMutex.withLock { closed.set(true) }

            val failures = mutableListOf<Throwable>()
            sessions.values.asSequence().toList().forEach { session ->
                try {
                    session.terminate(CloseEvents.ResetConnection)
                } catch (error: Throwable) {
                    failures += error
                }
            }
            directConnections.values.asSequence().toList().forEach { connection ->
                try {
                    // One global flush below attempts every dirty document even
                    // if an earlier connection or store operation fails.
                    connection.disconnectForShutdown()
                } catch (error: Throwable) {
                    failures += error
                }
            }
            try {
                flushPendingStores()
            } catch (error: HocuspocusPersistenceException) {
                failures += error.failures
            } catch (error: Throwable) {
                failures += error
            }

            if (failures.isEmpty()) {
                documentsMutex.withLock { documents.values.asSequence().toList() }.forEach { document ->
                    try {
                        unloadDocument(document, force = true)
                    } catch (error: Throwable) {
                        failures += error
                    }
                }
            }
            if (failures.isEmpty()) {
                try {
                    runHooks(ExtensionHook.OnDestroy) { extension -> extension.onDestroy(this) }
                } catch (error: Throwable) {
                    failures += error
                }
            }
            if (failures.isNotEmpty()) {
                failures.forEach(::reportError)
                throw HocuspocusShutdownException(failures)
            }

            shutdownComplete.set(true)
            scope.cancel()
        }
    }

    override fun close() {
        runBlocking { shutdown() }
    }

    internal suspend fun connect(payload: ConnectionAttempt<C>) {
        runHooks(ExtensionHook.OnConnect) { extension -> extension.onConnect(payload) }
    }

    internal suspend fun authenticate(payload: AuthenticatePayload<C>) {
        val authenticator = configuration.authenticator
        if (authenticator == null) {
            if (!configuration.allowAnonymous) {
                throw HocuspocusAuthenticationException(
                    CloseEvents.Unauthorized,
                    "No authenticator is configured",
                )
            }
        } else {
            authenticator.authenticate(payload)
        }
        runHooks(ExtensionHook.OnAuthenticate) { extension -> extension.onAuthenticate(payload) }
    }

    internal suspend fun connected(payload: ConnectedPayload<C>) {
        runHooks(ExtensionHook.Connected) { extension -> extension.connected(payload) }
    }

    internal suspend fun tokenSync(payload: TokenSyncPayload<C>) {
        runHooks(ExtensionHook.OnTokenSync) { extension -> extension.onTokenSync(payload) }
    }

    internal suspend fun beforeHandleMessage(payload: MessageHookPayload<C>) {
        runHooks(ExtensionHook.BeforeHandleMessage) { extension ->
            extension.beforeHandleMessage(payload)
        }
    }

    internal suspend fun afterHandleMessage(payload: MessageHookPayload<C>) {
        runHooks(ExtensionHook.AfterHandleMessage) { extension ->
            extension.afterHandleMessage(payload)
        }
    }

    internal suspend fun beforeHandleAwareness(payload: AwarenessHookPayload<C>) {
        runHooks(ExtensionHook.BeforeHandleAwareness) { extension ->
            extension.beforeHandleAwareness(payload)
        }
    }

    internal suspend fun beforeSync(payload: SyncHookPayload<C>) {
        runHooks(ExtensionHook.BeforeSync) { extension -> extension.beforeSync(payload) }
    }

    internal suspend fun beforeBroadcastStateless(payload: BroadcastStatelessPayload<C>) {
        runHooks(ExtensionHook.BeforeBroadcastStateless) { extension ->
            extension.beforeBroadcastStateless(payload)
        }
    }

    internal suspend fun stateless(payload: StatelessPayload<C>) {
        runHooks(ExtensionHook.OnStateless) { extension -> extension.onStateless(payload) }
    }

    internal fun documentUpdated(
        document: HocuspocusDocument<C>,
        connection: HocuspocusConnection<C>?,
        context: C?,
        update: ByteArray,
        origin: TransactionOrigin,
    ) {
        document.markDirtyAndSchedule(context, origin)
        if (!hasChangeHooks) return
        val payload = ChangePayload(document, connection, context, update.copyOf(), origin)
        launchSafely {
            runHooks(ExtensionHook.OnChange) { extension -> extension.onChange(payload) }
        }
    }

    internal fun awarenessUpdated(
        document: HocuspocusDocument<C>,
        connection: HocuspocusConnection<C>?,
        change: AwarenessChange,
        origin: TransactionOrigin?,
    ) {
        if (extensionsByHook.getValue(ExtensionHook.OnAwarenessUpdate).isEmpty()) return
        launchSafely {
            val payload = AwarenessUpdatePayload(
                document,
                connection,
                change,
                document.awarenessStates(),
                origin,
            )
            runHooks(ExtensionHook.OnAwarenessUpdate) { extension ->
                extension.onAwarenessUpdate(
                    payload,
                )
            }
        }
    }

    internal suspend fun storeDocument(document: HocuspocusDocument<C>) {
        document.performStore { payload ->
            try {
                runHooks(ExtensionHook.OnStoreDocument) { extension ->
                    extension.onStoreDocument(payload)
                }
                runHooks(ExtensionHook.AfterStoreDocument) { extension ->
                    extension.afterStoreDocument(payload)
                }
            } catch (_: SkipFurtherHooksException) {
                // A higher-priority store extension has durably handled this generation.
            }
        }
        if (document.connectionsCount == 0 && !document.isDirty()) {
            unloadDocument(document)
        }
    }

    internal suspend fun disconnected(connection: HocuspocusConnection<C>, lastConnection: Boolean) {
        runCatching {
            runHooks(ExtensionHook.OnDisconnect) { extension ->
                extension.onDisconnect(
                    DisconnectPayload(
                        this,
                        connection.document,
                        connection.socketId,
                        connection.context,
                        connection.attempt.request,
                    ),
                )
            }
        }.onFailure(::reportError)

        if (!lastConnection) return
        if (connection.document.isDirty() && configuration.unloadImmediately) {
            runCatching { connection.document.flushStore() }.onFailure(::reportError)
        }
        if (!connection.document.isDirty()) {
            unloadDocument(connection.document)
        }
    }

    internal suspend fun directDisconnected(
        document: HocuspocusDocument<C>,
        context: C,
        lastConnection: Boolean,
        unloadImmediately: Boolean,
        retainDocument: Boolean,
    ) {
        document.awaitMutations()
        if (!retainDocument && document.isDirty() && unloadImmediately) {
            document.flushStore()
        }
        if (!lastConnection) return
        runCatching {
            runHooks(ExtensionHook.OnDisconnect) { extension ->
                extension.onDisconnect(
                    DisconnectPayload(
                        this,
                        document,
                        "server",
                        context,
                        HocuspocusRequest("http://localhost"),
                    ),
                )
            }
        }.onFailure(::reportError)
        if (retainDocument) return
        if (!document.isDirty()) unloadDocument(document)
    }

    internal suspend fun getOrLoadDocument(attempt: ConnectionAttempt<C>): HocuspocusDocument<C> {
        require(attempt.routingKey.documentName.isNotBlank()) { "document name must not be blank" }
        require(attempt.routingKey.documentName.length <= configuration.maxDocumentNameLength) {
            "document name exceeds maxDocumentNameLength"
        }
        var immediate: HocuspocusDocument<C>? = null
        lateinit var deferred: CompletableDeferred<HocuspocusDocument<C>>
        var ownsLoad = false
        documentsMutex.withLock {
            immediate = documents[attempt.routingKey.documentName]
            if (immediate == null) {
                val existingLoad = loadingDocuments[attempt.routingKey.documentName]
                if (existingLoad != null) {
                    deferred = existingLoad
                } else {
                    if (documents.size + loadingDocuments.size >= configuration.maxLoadedDocuments) {
                        throw ProtocolException(
                            "loaded document limit ${configuration.maxLoadedDocuments} exceeded",
                        )
                    }
                    deferred = CompletableDeferred()
                    loadingDocuments[attempt.routingKey.documentName] = deferred
                    ownsLoad = true
                }
            }
        }
        immediate?.let { return it }
        if (!ownsLoad) return deferred.await()

        try {
            val loaded = loadDocument(attempt)
            val published = lifecycleMutex.withLock {
                if (closed.get()) {
                    false
                } else {
                    documentsMutex.withLock {
                        documents[loaded.name] = loaded
                        loadingDocuments.remove(loaded.name)
                    }
                    true
                }
            }
            if (!published) {
                loaded.destroy()
                error("Hocuspocus server is closed")
            }
            deferred.complete(loaded)
            return loaded
        } catch (error: Throwable) {
            documentsMutex.withLock { loadingDocuments.remove(attempt.routingKey.documentName) }
            deferred.completeExceptionally(error)
            throw error
        }
    }

    internal suspend fun unloadDocument(document: HocuspocusDocument<C>, force: Boolean = false) {
        if (!unloadingDocuments.add(document.name)) return
        try {
            if (!force && document.connectionsCount > 0) return
            val payload = UnloadDocumentPayload(this, document)
            try {
                runHooks(ExtensionHook.BeforeUnloadDocument) { extension ->
                    extension.beforeUnloadDocument(payload)
                }
            } catch (error: Throwable) {
                reportError(error)
                return
            }
            if (!document.beginUnload(force)) return
            try {
                document.destroy()
            } catch (error: Throwable) {
                document.cancelUnload()
                throw error
            }
            documentsMutex.withLock {
                documents.remove(document.name, document)
            }
            runHooks(ExtensionHook.AfterUnloadDocument) { extension ->
                extension.afterUnloadDocument(payload)
            }
        } finally {
            unloadingDocuments.remove(document.name)
        }
    }

    internal fun sessionClosed(session: ClientSession<C>) {
        sessions.remove(session.socketId, session)
    }

    internal fun directConnectionClosed(connection: DirectConnection<C>) {
        directConnections.remove(connection.id, connection)
    }

    private suspend fun loadDocument(attempt: ConnectionAttempt<C>): HocuspocusDocument<C> {
        var options = configuration.documentOptions
        for (extension in extensionsByHook.getValue(ExtensionHook.OnCreateDocument)) {
            extension.onCreateDocument(CreateDocumentPayload(attempt, options))?.let { options = it }
        }

        val crdt = configuration.documentFactory.create(options)
        val document = HocuspocusDocument(this, attempt.routingKey.documentName, crdt)
        val payload = DocumentHookPayload(this, document, attempt)
        try {
            for (extension in extensionsByHook.getValue(ExtensionHook.OnLoadDocument)) {
                extension.onLoadDocument(payload)?.let { update ->
                    crdt.applyUpdate(update, TransactionOrigin.Local(skipStoreHooks = true))
                }
            }
            document.isLoading = false
            runHooks(ExtensionHook.AfterLoadDocument) { extension ->
                extension.afterLoadDocument(payload)
            }
            return document
        } catch (error: Throwable) {
            document.destroy()
            throw error
        }
    }

    private suspend inline fun runHooks(
        name: ExtensionHook,
        crossinline hook: suspend (HocuspocusExtension<C>) -> Unit,
    ) {
        for (extension in extensionsByHook.getValue(name)) hook(extension)
    }

    internal fun launchSafely(block: suspend CoroutineScope.() -> Unit) {
        scope.launch {
            try {
                block()
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                reportError(error)
            }
        }
    }

    internal fun reportError(error: Throwable) {
        try {
            configuration.onError(error)
        } catch (reportingError: Throwable) {
            if (reportingError !== error) reportingError.addSuppressed(error)
            System.getLogger("ai.hocuspocus")
                .log(System.Logger.Level.ERROR, "Hocuspocus onError callback failed", reportingError)
        }
    }

    public companion object {
        public const val JVM_PROTOCOL_VERSION: String = "4.4.0-jvm.1"
    }
}

private enum class ExtensionHook(
    val methodName: String,
) {
    OnConfigure("onConfigure"),
    OnConnect("onConnect"),
    OnAuthenticate("onAuthenticate"),
    Connected("connected"),
    OnTokenSync("onTokenSync"),
    OnCreateDocument("onCreateDocument"),
    OnLoadDocument("onLoadDocument"),
    AfterLoadDocument("afterLoadDocument"),
    BeforeHandleMessage("beforeHandleMessage"),
    AfterHandleMessage("afterHandleMessage"),
    BeforeHandleAwareness("beforeHandleAwareness"),
    BeforeSync("beforeSync"),
    BeforeBroadcastStateless("beforeBroadcastStateless"),
    OnStateless("onStateless"),
    OnChange("onChange"),
    OnStoreDocument("onStoreDocument"),
    AfterStoreDocument("afterStoreDocument"),
    OnAwarenessUpdate("onAwarenessUpdate"),
    OnDisconnect("onDisconnect"),
    BeforeUnloadDocument("beforeUnloadDocument"),
    AfterUnloadDocument("afterUnloadDocument"),
    OnDestroy("onDestroy"),
}

private fun HocuspocusExtension<*>.implements(hook: ExtensionHook): Boolean =
    javaClass.methods.any { method ->
        method.name == hook.methodName &&
            method.parameterCount == 2 &&
            !method.isBridge &&
            method.declaringClass != HocuspocusExtension::class.java
    }
