package ai.hocuspocus.core

import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.protocol.SyncMessageType
import kotlinx.serialization.json.JsonElement

public data class ConfigurePayload<C : Any>(
    val server: HocuspocusServer<C>,
    val version: String,
)

public class ConnectionAttempt<C : Any>(
    public val server: HocuspocusServer<C>,
    public val request: HocuspocusRequest,
    public val routingKey: RoutingKey,
    public val socketId: String,
    public val context: MutableContext<C>,
    public val connectionConfiguration: ConnectionConfiguration = ConnectionConfiguration(),
    public var providerVersion: String? = null,
)

public data class AuthenticatePayload<C : Any>(
    val attempt: ConnectionAttempt<C>,
    val token: String,
)

public data class ConnectedPayload<C : Any>(
    val attempt: ConnectionAttempt<C>,
    val connection: HocuspocusConnection<C>,
)

public data class TokenSyncPayload<C : Any>(
    val connection: HocuspocusConnection<C>,
    val token: String,
)

public data class CreateDocumentPayload<C : Any>(
    val attempt: ConnectionAttempt<C>,
    val options: CrdtDocumentOptions,
)

public data class DocumentHookPayload<C : Any>(
    val server: HocuspocusServer<C>,
    val document: HocuspocusDocument<C>,
    val attempt: ConnectionAttempt<C>,
)

public data class MessageHookPayload<C : Any>(
    val connection: HocuspocusConnection<C>,
    val rawMessage: ByteArray,
)

public data class AwarenessHookPayload<C : Any>(
    val document: HocuspocusDocument<C>,
    val connection: HocuspocusConnection<C>?,
    val states: MutableMap<Long, JsonElement>,
    val transactionOrigin: TransactionOrigin,
)

public data class SyncHookPayload<C : Any>(
    val connection: HocuspocusConnection<C>,
    val type: SyncMessageType,
    val payload: ByteArray,
)

public data class StatelessPayload<C : Any>(
    val connection: HocuspocusConnection<C>,
    val payload: String,
)

public data class BroadcastStatelessPayload<C : Any>(
    val document: HocuspocusDocument<C>,
    val payload: String,
)

public data class ChangePayload<C : Any>(
    val document: HocuspocusDocument<C>,
    val connection: HocuspocusConnection<C>?,
    val context: C?,
    val update: ByteArray,
    val transactionOrigin: TransactionOrigin,
)

public data class StorePayload<C : Any>(
    val document: HocuspocusDocument<C>,
    val lastContext: C?,
    val lastTransactionOrigin: TransactionOrigin?,
)

public data class AwarenessUpdatePayload<C : Any>(
    val document: HocuspocusDocument<C>,
    val connection: HocuspocusConnection<C>?,
    val change: AwarenessChange,
    val states: Map<Long, JsonElement>,
    val transactionOrigin: TransactionOrigin?,
)

public data class DisconnectPayload<C : Any>(
    val server: HocuspocusServer<C>,
    val document: HocuspocusDocument<C>,
    val socketId: String,
    val context: C,
    val request: HocuspocusRequest,
) {
    public val clientsCount: Int
        get() = document.connectionsCount
}

public data class UnloadDocumentPayload<C : Any>(
    val server: HocuspocusServer<C>,
    val document: HocuspocusDocument<C>,
)

/** Idiomatic suspending equivalent of the @hocuspocus/server v4 hook surface. */
public interface HocuspocusExtension<C : Any> {
    public val priority: Int
        get() = 100

    public val name: String
        get() = this::class.simpleName ?: "anonymous-extension"

    public suspend fun onConfigure(payload: ConfigurePayload<C>) {}

    public suspend fun onConnect(payload: ConnectionAttempt<C>) {}

    public suspend fun onAuthenticate(payload: AuthenticatePayload<C>) {}

    public suspend fun connected(payload: ConnectedPayload<C>) {}

    public suspend fun onTokenSync(payload: TokenSyncPayload<C>) {}

    public suspend fun onCreateDocument(payload: CreateDocumentPayload<C>): CrdtDocumentOptions? = null

    public suspend fun onLoadDocument(payload: DocumentHookPayload<C>): ByteArray? = null

    public suspend fun afterLoadDocument(payload: DocumentHookPayload<C>) {}

    public suspend fun beforeHandleMessage(payload: MessageHookPayload<C>) {}

    public suspend fun afterHandleMessage(payload: MessageHookPayload<C>) {}

    public suspend fun beforeHandleAwareness(payload: AwarenessHookPayload<C>) {}

    public suspend fun beforeSync(payload: SyncHookPayload<C>) {}

    public suspend fun beforeBroadcastStateless(payload: BroadcastStatelessPayload<C>) {}

    public suspend fun onStateless(payload: StatelessPayload<C>) {}

    public suspend fun onChange(payload: ChangePayload<C>) {}

    public suspend fun onStoreDocument(payload: StorePayload<C>) {}

    public suspend fun afterStoreDocument(payload: StorePayload<C>) {}

    public suspend fun onAwarenessUpdate(payload: AwarenessUpdatePayload<C>) {}

    public suspend fun onDisconnect(payload: DisconnectPayload<C>) {}

    public suspend fun beforeUnloadDocument(payload: UnloadDocumentPayload<C>) {}

    public suspend fun afterUnloadDocument(payload: UnloadDocumentPayload<C>) {}

    public suspend fun onDestroy(server: HocuspocusServer<C>) {}
}
