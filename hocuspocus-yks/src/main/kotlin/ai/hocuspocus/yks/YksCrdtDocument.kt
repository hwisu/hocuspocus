package ai.hocuspocus.yks

import ai.hocuspocus.core.CrdtDocument
import ai.hocuspocus.core.CrdtDocumentFactory
import ai.hocuspocus.core.CrdtDocumentOptions
import ai.hocuspocus.core.CrdtStructInfo
import ai.hocuspocus.core.CrdtStructKind
import ai.hocuspocus.core.CrdtUpdate
import dev.yks.AbstractStruct
import dev.yks.GC
import dev.yks.ItemStruct
import dev.yks.Skip
import dev.yks.YDoc
import dev.yks.YDocOptions
import dev.yks.YDocRuntimeOptions
import dev.yks.YStandardUpdatePolicy
import dev.yks.YThreadAccessPolicy
import dev.yks.mergeUpdates as mergeYjsUpdates
import dev.yks.snapshot
import dev.yks.snapshotContainsUpdate
import kotlin.reflect.KClass

/** Standard-wire Hocuspocus document backed by the pure Kotlin YKS runtime. */
public class YksCrdtDocument(
    public val document: YDoc,
) : CrdtDocument {
    private val monitor: Any = Any()
    private var closed: Boolean = false
    private var activeCapture: MutableList<CrdtUpdate>? = null
    private val updateSubscription = document.observeUpdates { update, emittedOrigin ->
        // YKS creates this standard update for the completed transaction and
        // does not mutate or reuse it after the synchronous callback returns.
        activeCapture?.add(CrdtUpdate(update, emittedOrigin))
    }

    override fun encodeStateVector(): ByteArray = synchronized(monitor) {
        ensureOpen()
        document.encodeStateVector()
    }

    override fun encodeStateAsUpdate(encodedStateVector: ByteArray): ByteArray = synchronized(monitor) {
        ensureOpen()
        document.encodeStateAsUpdate(encodedStateVector)
    }

    override fun mergeUpdates(updates: List<ByteArray>): ByteArray = synchronized(monitor) {
        ensureOpen()
        require(updates.isNotEmpty()) { "at least one CRDT update is required" }
        mergeYjsUpdates(updates)
    }

    override fun containsUpdate(update: ByteArray): Boolean = synchronized(monitor) {
        ensureOpen()
        snapshotContainsUpdate(snapshot(document), update)
    }

    override fun isFieldEmpty(fieldName: String): Boolean = synchronized(monitor) {
        ensureOpen()
        document.isRootEmpty(fieldName)
    }

    override fun applyUpdate(update: ByteArray, origin: Any?): List<CrdtUpdate> = synchronized(monitor) {
        ensureOpen()
        captureUpdates { document.applyUpdate(update, origin) }
    }

    override fun <N : Any> transact(
        nativeType: KClass<N>,
        origin: Any?,
        mutation: (N) -> Unit,
    ): List<CrdtUpdate> = synchronized(monitor) {
        ensureOpen()
        require(nativeType.isInstance(document)) {
            "YKS native document is not ${nativeType.qualifiedName}"
        }
        val nativeDocument = nativeType.java.cast(document)

        captureUpdates {
            document.transact(origin = origin) {
                mutation(nativeDocument)
            }
        }
    }

    override fun close() {
        synchronized(monitor) {
            if (closed) return
            closed = true
            updateSubscription.close()
            document.destroy()
        }
    }

    private inline fun captureUpdates(block: () -> Unit): List<CrdtUpdate> {
        val updates = mutableListOf<CrdtUpdate>()
        check(activeCapture == null) { "nested CRDT update capture is not supported" }
        activeCapture = updates
        try {
            block()
        } finally {
            activeCapture = null
        }
        return updates
    }

    private fun ensureOpen() {
        check(!closed) { "CRDT document is closed" }
    }
}

public class YksDocumentFactory(
    private val createDocument: (CrdtDocumentOptions) -> YDoc = { options ->
        YDoc(
            YDocOptions(
                gc = options.garbageCollection,
                gcFilter = { struct ->
                    options.garbageCollectionFilter?.invoke(struct.toCrdtStructInfo()) ?: true
                },
            ),
            YDocRuntimeOptions(
                threadAccessPolicy = YThreadAccessPolicy.EXTERNALLY_SERIALIZED,
                standardUpdatePolicy = YStandardUpdatePolicy.REQUIRE_STANDARD,
            ),
        )
    },
) : CrdtDocumentFactory {
    override fun create(options: CrdtDocumentOptions): CrdtDocument =
        YksCrdtDocument(createDocument(options))
}

public fun CrdtDocument.requireYDoc(): YDoc =
    (this as? YksCrdtDocument)?.document ?: error("CRDT document is not backed by YKS")

private fun AbstractStruct.toCrdtStructInfo(): CrdtStructInfo = CrdtStructInfo(
    clientId = id.client,
    clock = id.clock,
    length = length,
    deleted = deleted,
    kind = when (this) {
        is ItemStruct -> CrdtStructKind.Item
        is GC -> CrdtStructKind.GarbageCollected
        is Skip -> CrdtStructKind.Skip
        else -> CrdtStructKind.Other
    },
)
