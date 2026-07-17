package ai.hocuspocus.core

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.reflect.KClass
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class HocuspocusServerSemanticsTest {
    @Test
    fun `owned document storage closes during shutdown`() = runBlocking {
        val closed = AtomicBoolean()
        val storage = object : DocumentStorage, AutoCloseable {
            override suspend fun load(documentName: String): ByteArray? = null

            override suspend fun store(documentName: String, state: ByteArray) = Unit

            override fun close() {
                closed.set(true)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                extensions = listOf(
                    DatabaseExtension<Unit>(storage, closeOnDestroy = true),
                ),
            ),
        )

        server.start()
        server.shutdown()

        assertTrue(closed.get())
    }

    @Test
    fun `malformed outer frames close as unauthorized even when error reporting fails`() = runBlocking {
        val transport = RecordingTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = fakeDocumentFactory(),
                onError = { error("error callback failed") },
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)

        session.handleBinary(byteArrayOf(0x80.toByte()))

        assertEquals(CloseEvents.Unauthorized, withTimeout(2.seconds) { transport.closed.receive() })
        server.shutdown()
    }

    @Test
    fun `skip further hooks fails outside the store lifecycle`() = runBlocking {
        val created = AtomicBoolean()
        val lowerPriorityCalled = AtomicBoolean()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = CrdtDocumentFactory {
                    created.set(true)
                    FakeCrdtDocument()
                },
                extensions = listOf(
                    object : HocuspocusExtension<Unit> {
                        override val priority: Int = 200

                        override suspend fun onCreateDocument(
                            payload: CreateDocumentPayload<Unit>,
                        ): CrdtDocumentOptions? = throw SkipFurtherHooksException()
                    },
                    object : HocuspocusExtension<Unit> {
                        override val priority: Int = 100

                        override suspend fun onCreateDocument(
                            payload: CreateDocumentPayload<Unit>,
                        ): CrdtDocumentOptions? {
                            lowerPriorityCalled.set(true)
                            return null
                        }
                    },
                ),
            ),
        )

        assertFailsWith<SkipFurtherHooksException> {
            server.openDirectConnection("rejected", Unit)
        }
        assertFalse(created.get())
        assertFalse(lowerPriorityCalled.get())
        server.shutdown()
    }

    @Test
    fun `skip further hooks marks a store generation as handled`() = runBlocking {
        val highPriorityStores = AtomicInteger()
        val lowerPriorityStores = AtomicInteger()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                extensions = listOf(
                    object : HocuspocusExtension<Unit> {
                        override val priority: Int = 200

                        override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                            highPriorityStores.incrementAndGet()
                            throw SkipFurtherHooksException()
                        }
                    },
                    object : HocuspocusExtension<Unit> {
                        override val priority: Int = 100

                        override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                            lowerPriorityStores.incrementAndGet()
                        }
                    },
                ),
            ),
        )
        val direct = server.openDirectConnection("stored", Unit)

        direct.transact(FakeCrdtDocument::class) { it.value += 1 }
        direct.disconnect()

        assertEquals(1, highPriorityStores.get())
        assertEquals(0, lowerPriorityStores.get())
        assertEquals(0, server.documentsCount)
        server.shutdown()
    }

    @Test
    fun `contextual storage receives request and last transaction context`() = runBlocking {
        var loadRequest: DocumentLoadRequest<String>? = null
        var storeRequest: DocumentStoreRequest<String>? = null
        val storage = object : ContextualDocumentStorage<String> {
            override suspend fun load(request: DocumentLoadRequest<String>): ByteArray? {
                loadRequest = request
                return null
            }

            override suspend fun store(request: DocumentStoreRequest<String>) {
                storeRequest = request
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                extensions = listOf(DatabaseExtension(storage)),
            ),
        )
        val direct = server.openDirectConnection("tenant-a:document", "tenant-a")

        direct.transact(FakeCrdtDocument::class) { it.value += 1 }
        direct.disconnect()

        assertEquals("tenant-a", assertNotNull(loadRequest).context)
        assertEquals("tenant-a:document", assertNotNull(loadRequest).documentName)
        assertEquals("tenant-a", assertNotNull(storeRequest).lastContext)
        assertEquals(0, assertNotNull(storeRequest).activeConnections)
        assertTrue(assertNotNull(storeRequest).state.isNotEmpty())
        server.shutdown()
    }

    private fun fakeDocumentFactory(): CrdtDocumentFactory = CrdtDocumentFactory { FakeCrdtDocument() }

    private class FakeCrdtDocument : CrdtDocument {
        var value: Int = 0
        private var closed: Boolean = false

        override fun encodeStateVector(): ByteArray = byteArrayOf(value.toByte())

        override fun encodeStateAsUpdate(encodedStateVector: ByteArray): ByteArray = byteArrayOf(value.toByte())

        override fun containsUpdate(update: ByteArray): Boolean = update.contentEquals(encodeStateAsUpdate())

        override fun applyUpdate(update: ByteArray, origin: Any?): List<CrdtUpdate> {
            check(!closed)
            value = update.firstOrNull()?.toInt() ?: 0
            return listOf(CrdtUpdate(update.copyOf(), origin))
        }

        override fun <N : Any> transact(
            nativeType: KClass<N>,
            origin: Any?,
            mutation: (N) -> Unit,
        ): List<CrdtUpdate> {
            check(!closed)
            require(nativeType.isInstance(this))
            @Suppress("UNCHECKED_CAST")
            mutation(this as N)
            return listOf(CrdtUpdate(encodeStateAsUpdate(), origin))
        }

        override fun close() {
            closed = true
        }
    }

    private class RecordingTransport : SocketTransport {
        private val open = AtomicBoolean(true)
        val closed: Channel<CloseEvent> = Channel(Channel.UNLIMITED)

        override val isOpen: Boolean
            get() = open.get()

        override fun send(bytes: ByteArray): Boolean = open.get()

        override fun close(code: Int, reason: String) {
            if (open.compareAndSet(true, false)) closed.trySend(CloseEvent(code, reason))
        }
    }
}
