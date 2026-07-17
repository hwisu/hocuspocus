package ai.hocuspocus.core

import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import kotlin.reflect.KClass

public class DirectConnection<C : Any> internal constructor(
    private val server: HocuspocusServer<C>,
    document: HocuspocusDocument<C>,
    public val context: C,
) : AutoCloseable {
    internal val id: String = UUID.randomUUID().toString()
    private val open: AtomicBoolean = AtomicBoolean(true)
    private var currentDocument: HocuspocusDocument<C>? = document

    public val document: HocuspocusDocument<C>
        get() = checkNotNull(currentDocument) { "direct connection is closed" }

    public val isOpen: Boolean
        get() = open.get()

    public suspend fun <N : Any> transact(
        nativeType: KClass<N>,
        skipStoreHooks: Boolean = false,
        mutation: (N) -> Unit,
    ) {
        check(open.get()) { "direct connection is closed" }
        document.transact(nativeType, context, skipStoreHooks, mutation)
    }

    public suspend fun disconnect(unloadImmediately: Boolean = true) {
        disconnectInternal(unloadImmediately, retainDocument = false)
    }

    internal suspend fun disconnectForShutdown() {
        disconnectInternal(unloadImmediately = false, retainDocument = true)
    }

    private suspend fun disconnectInternal(unloadImmediately: Boolean, retainDocument: Boolean) {
        if (!open.compareAndSet(true, false)) return
        val closingDocument = currentDocument ?: return
        currentDocument = null
        server.directConnectionClosed(this)
        val lastConnection = closingDocument.removeDirectConnection()
        server.directDisconnected(
            closingDocument,
            context,
            lastConnection,
            unloadImmediately,
            retainDocument,
        )
    }

    /** Blocking [AutoCloseable] bridge that does not return before persistence finishes. */
    override fun close() {
        runBlocking { disconnect() }
    }
}
