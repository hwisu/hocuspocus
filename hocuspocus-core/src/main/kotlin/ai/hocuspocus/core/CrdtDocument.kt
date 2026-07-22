package ai.hocuspocus.core

import kotlin.reflect.KClass

public data class CrdtDocumentOptions(
    val garbageCollection: Boolean = true,
    val garbageCollectionFilter: ((CrdtStructInfo) -> Boolean)? = null,
)

public enum class CrdtStructKind {
    Item,
    GarbageCollected,
    Skip,
    Other,
}

/** Engine-neutral metadata exposed to the Hocuspocus `gcFilter` equivalent. */
public data class CrdtStructInfo(
    val clientId: Long,
    val clock: Long,
    val length: Long,
    val deleted: Boolean,
    val kind: CrdtStructKind,
)

public data class CrdtUpdate(
    val data: ByteArray,
    val origin: Any?,
) {
    override fun equals(other: Any?): Boolean =
        other is CrdtUpdate && origin == other.origin && data.contentEquals(other.data)

    override fun hashCode(): Int = 31 * data.contentHashCode() + (origin?.hashCode() ?: 0)
}

/**
 * The CRDT boundary required by the Hocuspocus server.
 *
 * Implementations must accept and emit genuine Yjs update V1 bytes. A private
 * implementation-specific envelope must never be returned by this API.
 */
public interface CrdtDocument : AutoCloseable {
    public fun encodeStateVector(): ByteArray

    public fun encodeStateAsUpdate(encodedStateVector: ByteArray = ByteArray(0)): ByteArray

    /** Returns true when every struct and delete in [update] already exists in this document. */
    public fun containsUpdate(update: ByteArray): Boolean

    /**
     * Returns whether the named root has no visible list/text content or map entries.
     *
     * Custom engines should override this when they support Hocuspocus's
     * `Document.isEmpty(fieldName)` convenience API.
     */
    public fun isFieldEmpty(fieldName: String): Boolean =
        throw UnsupportedOperationException("This CRDT engine does not expose root emptiness")

    /** Applies one standard update and returns the standard updates emitted by the transaction. */
    public fun applyUpdate(update: ByteArray, origin: Any? = null): List<CrdtUpdate>

    /** Runs one typed local transaction and returns the standard updates emitted by it. */
    public fun <N : Any> transact(
        nativeType: KClass<N>,
        origin: Any? = null,
        mutation: (N) -> Unit,
    ): List<CrdtUpdate>

    override fun close()
}

public fun interface CrdtDocumentFactory {
    public fun create(options: CrdtDocumentOptions): CrdtDocument
}

public sealed interface TransactionOrigin {
    public data class Connection(
        val socketId: String,
        val routingKey: String,
    ) : TransactionOrigin

    public data object Redis : TransactionOrigin

    public data class Local(
        val context: Any? = null,
        val skipStoreHooks: Boolean = false,
    ) : TransactionOrigin
}

public fun TransactionOrigin?.shouldSkipStoreHooks(): Boolean = when (this) {
    is TransactionOrigin.Connection, null -> false
    TransactionOrigin.Redis -> true
    is TransactionOrigin.Local -> skipStoreHooks
}
