package ai.hocuspocus.core

/** Minimal document-name storage contract retained for simple deployments. */
public interface DocumentStorage {
    public suspend fun load(documentName: String): ByteArray?

    public suspend fun store(documentName: String, state: ByteArray)
}

public data class DocumentLoadRequest<C : Any>(
    val documentName: String,
    val context: C,
    val request: HocuspocusRequest,
    val socketId: String,
    val connectionConfiguration: ConnectionConfiguration,
)

public class DocumentStoreRequest<C : Any>(
    public val documentName: String,
    public val state: ByteArray,
    public val activeConnections: Int,
    public val lastContext: C?,
    public val lastTransactionOrigin: TransactionOrigin?,
)

/** Persistence contract with the request and transaction context exposed by Hocuspocus v4. */
public interface ContextualDocumentStorage<C : Any> {
    public suspend fun load(request: DocumentLoadRequest<C>): ByteArray?

    public suspend fun store(request: DocumentStoreRequest<C>)
}

/** Generic persistence extension for databases, object storage, or key/value stores. */
public class DatabaseExtension<C : Any> private constructor(
    private val loadDocument: suspend (DocumentHookPayload<C>) -> ByteArray?,
    private val storeDocument: suspend (StorePayload<C>, ByteArray) -> Unit,
    private val closeStorage: (() -> Unit)?,
    override val priority: Int = 100,
) : HocuspocusExtension<C> {
    public constructor(
        storage: DocumentStorage,
        priority: Int = 100,
        closeOnDestroy: Boolean = false,
    ) : this(
        loadDocument = { payload -> storage.load(payload.document.name) },
        storeDocument = { payload, state -> storage.store(payload.document.name, state) },
        closeStorage = closeAction(storage, closeOnDestroy),
        priority = priority,
    )

    public constructor(
        storage: ContextualDocumentStorage<C>,
        priority: Int = 100,
        closeOnDestroy: Boolean = false,
    ) : this(
        loadDocument = { payload ->
            storage.load(
                DocumentLoadRequest(
                    documentName = payload.document.name,
                    context = payload.attempt.context.value,
                    request = payload.attempt.request,
                    socketId = payload.attempt.socketId,
                    connectionConfiguration = payload.attempt.connectionConfiguration,
                ),
            )
        },
        storeDocument = { payload, state ->
            storage.store(
                DocumentStoreRequest(
                    documentName = payload.document.name,
                    state = state,
                    activeConnections = payload.document.connectionsCount,
                    lastContext = payload.lastContext,
                    lastTransactionOrigin = payload.lastTransactionOrigin,
                ),
            )
        },
        closeStorage = closeAction(storage, closeOnDestroy),
        priority = priority,
    )

    override val name: String = "database"

    override suspend fun onLoadDocument(payload: DocumentHookPayload<C>): ByteArray? =
        loadDocument(payload)

    override suspend fun onStoreDocument(payload: StorePayload<C>) {
        storeDocument(payload, payload.document.encodeStateAsUpdate())
    }

    override suspend fun onDestroy(server: HocuspocusServer<C>) {
        closeStorage?.invoke()
    }

    private companion object {
        private fun closeAction(storage: Any, closeOnDestroy: Boolean): (() -> Unit)? {
            if (!closeOnDestroy) return null
            require(storage is AutoCloseable) {
                "closeOnDestroy requires storage to implement AutoCloseable"
            }
            return storage::close
        }
    }
}
