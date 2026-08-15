package ai.hocuspocus.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds

/**
 * The debounced persistence state machine for one [HocuspocusDocument].
 *
 * Dirtiness is tracked as a monotonic generation rather than a flag, so a write
 * that lands while a store is in flight cannot be mistaken for durable: the
 * store only advances [storedGeneration] to the generation it actually captured.
 *
 * Owns its own [stateLock], which is independent of the document's mutation
 * lock. Nothing here takes the mutation lock, so the two never nest.
 */
internal class DocumentStoreScheduler<C : Any>(
    private val scope: CoroutineScope,
    private val configuration: HocuspocusConfiguration<C>,
    private val onStore: suspend () -> Unit,
    private val onError: (Throwable) -> Unit,
) {
    private val stateLock: Any = Any()
    private val storeMutex: Mutex = Mutex()
    private var dirtyGeneration: Long = 0
    private var storedGeneration: Long = 0
    private var firstDirtyNanos: Long? = null
    private var lastDirtyNanos: Long? = null
    private var pendingStoreJob: Job? = null
    private var lastContext: C? = null
    private var lastOrigin: TransactionOrigin? = null

    fun isDirty(): Boolean = synchronized(stateLock) {
        storedGeneration < dirtyGeneration
    }

    /** Records a write and starts the debounce window if one is not already running. */
    fun markDirty(context: C?, origin: TransactionOrigin) {
        if (origin.shouldSkipStoreHooks()) return
        synchronized(stateLock) {
            dirtyGeneration += 1
            lastContext = context
            lastOrigin = origin
            val now = System.nanoTime()
            if (firstDirtyNanos == null) firstDirtyNanos = now
            lastDirtyNanos = now

            if (pendingStoreJob == null) {
                lateinit var job: Job
                job = scope.launch(start = CoroutineStart.LAZY) {
                    awaitStoreDeadline(job)
                    runCatching { onStore() }
                        .onFailure { error ->
                            if (error !is CancellationException) onError(error)
                        }
                }
                pendingStoreJob = job
                job.start()
            }
        }
    }

    /**
     * Waits until the document has been quiet for [HocuspocusConfiguration.debounce]
     * or has been dirty for [HocuspocusConfiguration.maxDebounce], whichever comes first.
     */
    private suspend fun awaitStoreDeadline(job: Job) {
        while (true) {
            val wait = synchronized(stateLock) {
                if (pendingStoreJob !== job) return
                val firstDirty = firstDirtyNanos
                val lastDirty = lastDirtyNanos
                if (firstDirty == null || lastDirty == null) {
                    pendingStoreJob = null
                    return
                }
                val now = System.nanoTime()
                val elapsedFromFirst = (now - firstDirty).nanoseconds
                val elapsedFromLast = (now - lastDirty).nanoseconds
                val remainingMax =
                    (configuration.maxDebounce - elapsedFromFirst).coerceAtLeast(Duration.ZERO)
                val remainingDebounce =
                    (configuration.debounce - elapsedFromLast).coerceAtLeast(Duration.ZERO)
                minOf(remainingDebounce, remainingMax).also { remaining ->
                    if (remaining == Duration.ZERO) pendingStoreJob = null
                }
            }
            if (wait == Duration.ZERO) return
            delay(wait)
        }
    }

    fun cancelPendingStore() {
        synchronized(stateLock) {
            pendingStoreJob?.cancel()
            pendingStoreJob = null
        }
    }

    /** Cancels the debounce window and stores until no generation is outstanding. */
    suspend fun flush() {
        cancelPendingStore()
        while (isDirty()) {
            onStore()
        }
    }

    /**
     * Runs [block] against the generation outstanding at entry, then marks that
     * generation durable. Serialized so two stores cannot interleave.
     */
    suspend fun performStore(block: suspend (C?, TransactionOrigin?) -> Unit) {
        storeMutex.withLock {
            val snapshot = synchronized(stateLock) {
                if (storedGeneration >= dirtyGeneration) return
                StoreSnapshot(dirtyGeneration, lastContext, lastOrigin)
            }
            block(snapshot.context, snapshot.origin)
            synchronized(stateLock) {
                storedGeneration = maxOf(storedGeneration, snapshot.generation)
                if (storedGeneration >= dirtyGeneration) {
                    firstDirtyNanos = null
                    lastDirtyNanos = null
                }
            }
        }
    }

    private data class StoreSnapshot<C : Any>(
        val generation: Long,
        val context: C?,
        val origin: TransactionOrigin?,
    )
}
