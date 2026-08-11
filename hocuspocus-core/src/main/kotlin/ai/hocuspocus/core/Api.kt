package ai.hocuspocus.core

import kotlin.time.Duration
import kotlin.time.Duration.Companion.ZERO
import kotlin.time.Duration.Companion.seconds

public data class HocuspocusRequest(
    val uri: String,
    val headers: Map<String, List<String>> = emptyMap(),
    val parameters: Map<String, List<String>> = emptyMap(),
    val remoteAddress: String? = null,
)

/** Non-blocking transport owned by the HTTP/WebSocket integration. */
public interface SocketTransport {
    public val isOpen: Boolean

    /**
     * Enqueues an immutable binary frame and returns false when outbound
     * backpressure rejected it. The buffer remains valid after this method
     * returns, so a transport may retain it. Neither the transport nor its
     * downstream writer may mutate [bytes]; copy only at a boundary that
     * cannot honor that rule.
     */
    public fun send(bytes: ByteArray): Boolean

    public fun close(code: Int, reason: String)
}

public data class CloseEvent(
    val code: Int,
    val reason: String,
)

public object CloseEvents {
    public val MessageTooBig: CloseEvent = CloseEvent(1009, "Message Too Big")
    public val ResetConnection: CloseEvent = CloseEvent(4205, "Reset Connection")
    public val Unauthorized: CloseEvent = CloseEvent(4401, "Unauthorized")
    public val Forbidden: CloseEvent = CloseEvent(4403, "Forbidden")
    public val ConnectionTimeout: CloseEvent = CloseEvent(4408, "Connection Timeout")
    public val Normal: CloseEvent = CloseEvent(1000, "Server closed the connection")
}

public class ConnectionConfiguration(
    public var readOnly: Boolean = false,
    public var isAuthenticated: Boolean = false,
)

public class MutableContext<C : Any>(initial: C) {
    public var value: C = initial
}

/**
 * Mandatory authentication boundary for WebSocket clients unless anonymous
 * access is explicitly enabled. Throw [HocuspocusAuthenticationException] to
 * reject an attempt with a protocol-specific close event.
 */
public fun interface HocuspocusAuthenticator<C : Any> {
    public suspend fun authenticate(payload: AuthenticatePayload<C>)
}

public data class HocuspocusConfiguration<C : Any>(
    val documentFactory: CrdtDocumentFactory,
    val authenticator: HocuspocusAuthenticator<C>? = null,
    val allowAnonymous: Boolean = false,
    val extensions: List<HocuspocusExtension<C>> = emptyList(),
    val documentOptions: CrdtDocumentOptions = CrdtDocumentOptions(),
    val timeout: Duration = 60.seconds,
    val debounce: Duration = 2.seconds,
    val maxDebounce: Duration = 10.seconds,
    /** Null sends every update immediately; zero coalesces updates from the current scheduler turn. */
    val flushDelay: Duration? = ZERO,
    val flushMaxBytes: Int = 1024 * 1024,
    val awarenessTimeout: Duration = 30.seconds,
    val awarenessMetadataRetention: Duration = Duration.INFINITE,
    val unloadImmediately: Boolean = true,
    val maxFrameSize: Int = 5 * 1024 * 1024,
    val maxUnauthenticatedQueueSize: Int = 5 * 1024 * 1024,
    val maxUnauthenticatedQueueMessages: Int = 1_000,
    val maxPendingDocuments: Int = 100,
    val maxDocumentsPerSocket: Int = 100,
    val maxLoadedDocuments: Int = 10_000,
    val maxDocumentNameLength: Int = minOf(1_024, maxFrameSize),
    val maxRoutingKeyLength: Int = minOf(2_048, maxFrameSize),
    val maxAuthenticationStringLength: Int = minOf(16 * 1024, maxFrameSize),
    val maxStatelessPayloadSize: Int = minOf(256 * 1024, maxFrameSize),
    val maxEstablishedQueueSize: Int = 5 * 1024 * 1024,
    val maxEstablishedQueueMessages: Int = 1_000,
    val maxCrdtUpdateSize: Int = minOf(512 * 1024, maxFrameSize),
    val maxAwarenessUpdateSize: Int = minOf(256 * 1024, maxFrameSize),
    val maxAwarenessEntriesPerMessage: Int = 1_024,
    val maxAwarenessClientsPerDocument: Int = 10_000,
    val maxAwarenessClientsPerConnection: Int = minOf(128, maxAwarenessClientsPerDocument),
    val onError: (Throwable) -> Unit = { error ->
        System.getLogger("ai.hocuspocus")
            .log(System.Logger.Level.ERROR, "Unhandled Hocuspocus failure", error)
    },
) {
    init {
        require(timeout.isPositive()) { "timeout must be positive" }
        require(!debounce.isNegative()) { "debounce must not be negative" }
        require(maxDebounce >= debounce) { "maxDebounce must be greater than or equal to debounce" }
        require(flushDelay?.isNegative() != true) { "flushDelay must not be negative" }
        require(flushMaxBytes > 0) { "flushMaxBytes must be positive" }
        require(awarenessTimeout.isPositive()) { "awarenessTimeout must be positive" }
        require(awarenessMetadataRetention >= awarenessTimeout) {
            "awarenessMetadataRetention must be greater than or equal to awarenessTimeout"
        }
        require(maxFrameSize > 0) { "maxFrameSize must be positive" }
        require(maxUnauthenticatedQueueSize > 0) { "maxUnauthenticatedQueueSize must be positive" }
        require(maxUnauthenticatedQueueMessages > 0) { "maxUnauthenticatedQueueMessages must be positive" }
        require(maxPendingDocuments > 0) { "maxPendingDocuments must be positive" }
        require(maxDocumentsPerSocket > 0) { "maxDocumentsPerSocket must be positive" }
        require(maxLoadedDocuments > 0) { "maxLoadedDocuments must be positive" }
        require(maxDocumentNameLength > 0) { "maxDocumentNameLength must be positive" }
        require(maxRoutingKeyLength >= maxDocumentNameLength) {
            "maxRoutingKeyLength must be greater than or equal to maxDocumentNameLength"
        }
        require(maxAuthenticationStringLength > 0) { "maxAuthenticationStringLength must be positive" }
        require(maxAuthenticationStringLength <= maxFrameSize) {
            "maxAuthenticationStringLength must not exceed maxFrameSize"
        }
        require(maxStatelessPayloadSize > 0) { "maxStatelessPayloadSize must be positive" }
        require(maxStatelessPayloadSize <= maxFrameSize) {
            "maxStatelessPayloadSize must not exceed maxFrameSize"
        }
        require(maxEstablishedQueueSize > 0) { "maxEstablishedQueueSize must be positive" }
        require(maxEstablishedQueueMessages > 0) { "maxEstablishedQueueMessages must be positive" }
        require(maxCrdtUpdateSize > 0) { "maxCrdtUpdateSize must be positive" }
        require(maxCrdtUpdateSize <= maxFrameSize) { "maxCrdtUpdateSize must not exceed maxFrameSize" }
        require(maxAwarenessUpdateSize > 0) { "maxAwarenessUpdateSize must be positive" }
        require(maxAwarenessUpdateSize <= maxFrameSize) { "maxAwarenessUpdateSize must not exceed maxFrameSize" }
        require(maxAwarenessEntriesPerMessage > 0) { "maxAwarenessEntriesPerMessage must be positive" }
        require(maxAwarenessClientsPerConnection > 0) { "maxAwarenessClientsPerConnection must be positive" }
        require(maxAwarenessClientsPerDocument > 0) { "maxAwarenessClientsPerDocument must be positive" }
        require(maxAwarenessClientsPerConnection <= maxAwarenessClientsPerDocument) {
            "maxAwarenessClientsPerConnection must not exceed maxAwarenessClientsPerDocument"
        }
    }
}

/**
 * Marks a store operation as handled by a higher-priority extension.
 *
 * This is accepted only from `onStoreDocument` and `afterStoreDocument`.
 * Throwing it from any other hook fails that operation.
 */
public class SkipFurtherHooksException : RuntimeException()

public class HocuspocusAuthenticationException(
    public val event: CloseEvent = CloseEvents.Forbidden,
    message: String = event.reason,
) : RuntimeException(message)

public class HocuspocusShutdownException(
    public val failures: List<Throwable>,
) : IllegalStateException("Hocuspocus shutdown failed in ${failures.size} operation(s)") {
    init {
        failures.forEach(::addSuppressed)
    }
}

public class HocuspocusPersistenceException(
    public val failures: List<Throwable>,
) : IllegalStateException("Hocuspocus persistence failed for ${failures.size} document(s)") {
    init {
        failures.forEach(::addSuppressed)
    }
}
