package ai.hocuspocus.core

import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.AwarenessEntry
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonPrimitive
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class HocuspocusServerSemanticsTest {
    @Test
    fun `connection cannot overwrite or remove awareness owned by another connection`() = runBlocking {
        var ignoredSocketId: String? = null
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                allowAnonymous = true,
                extensions = listOf(
                    object : HocuspocusExtension<Unit> {
                        override suspend fun beforeHandleAwareness(payload: AwarenessHookPayload<Unit>) {
                            if (payload.connection?.socketId == ignoredSocketId) {
                                payload.ignoredClientIds += 7
                                payload.states.remove(8)
                            }
                        }
                    },
                ),
            ),
        )
        val firstSession = server.openSession(
            RecordingTransport(),
            HocuspocusRequest("ws://test/collab"),
            Unit,
        )
        val secondSession = server.openSession(
            RecordingTransport(),
            HocuspocusRequest("ws://test/collab"),
            Unit,
        )
        val document = HocuspocusDocument(server, "owned-awareness", FakeCrdtDocument())
        val first = HocuspocusConnection(
            firstSession,
            document,
            ConnectionAttempt(
                server,
                firstSession.request,
                RoutingKey(document.name),
                firstSession.socketId,
                MutableContext(Unit),
            ),
        )
        val second = HocuspocusConnection(
            secondSession,
            document,
            ConnectionAttempt(
                server,
                secondSession.request,
                RoutingKey(document.name),
                secondSession.socketId,
                MutableContext(Unit),
            ),
        )
        first.start()
        second.start()
        document.applyAwareness(
            first,
            listOf(AwarenessEntry(7, 1, JsonPrimitive("first"))),
            first.transactionOrigin,
        )
        ignoredSocketId = second.socketId

        document.applyAwareness(
            second,
            listOf(AwarenessEntry(7, 2, JsonPrimitive("second"))),
            second.transactionOrigin,
        )
        document.applyAwareness(
            second,
            listOf(AwarenessEntry(7, 2, null)),
            second.transactionOrigin,
        )
        document.applyAwareness(
            second,
            listOf(AwarenessEntry(8, 1, JsonPrimitive("ignored"))),
            second.transactionOrigin,
        )
        assertEquals(JsonPrimitive("first"), document.awarenessStates()[7])
        assertTrue(8 !in document.awarenessStates())

        first.abort()
        second.abort()
        firstSession.terminate(CloseEvents.ResetConnection)
        secondSession.terminate(CloseEvents.ResetConnection)
        server.shutdown()
    }

    @Test
    fun `dispatches only hooks actually implemented by an extension`() = runBlocking {
        val persistenceOnly = DatabaseExtension<Unit>(
            storage = object : DocumentStorage {
                override suspend fun load(documentName: String): ByteArray? = null

                override suspend fun store(documentName: String, state: ByteArray) = Unit
            },
        )
        val changeOnly = object : HocuspocusExtension<Unit> {
            override suspend fun onChange(payload: ChangePayload<Unit>) = Unit
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                extensions = listOf(persistenceOnly, changeOnly),
            ),
        )

        assertFalse(server.hasMessageHooks)
        assertFalse(server.hasBeforeSyncHooks)
        assertTrue(server.hasChangeHooks)
        server.shutdown()
    }

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
    fun `shutdown fails and remains retryable when document unload is vetoed`() = runBlocking {
        val rejectUnload = AtomicBoolean(true)
        val reported = mutableListOf<Throwable>()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                extensions = listOf(
                    object : HocuspocusExtension<Unit> {
                        override suspend fun beforeUnloadDocument(payload: UnloadDocumentPayload<Unit>) {
                            if (rejectUnload.get()) error("keep document loaded")
                        }
                    },
                ),
                onError = reported::add,
            ),
        )
        server.openDirectConnection("vetoed", Unit)

        val failure = assertFailsWith<HocuspocusShutdownException> { server.shutdown() }

        assertEquals("keep document loaded", failure.failures.single().message)
        assertEquals(listOf("keep document loaded"), reported.map(Throwable::message))
        assertEquals(1, server.documentsCount)
        assertNotNull(server.document("vetoed"))

        rejectUnload.set(false)
        server.shutdown()

        assertEquals(0, server.documentsCount)
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
    fun `rejected transport writes schedule one session termination`() = runBlocking {
        val dispatcher = PausedDispatcher()
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = fakeDocumentFactory(),
                allowAnonymous = true,
            ),
            parentScope = CoroutineScope(dispatcher),
        )
        val session = server.openSession(
            RejectingTransport(),
            HocuspocusRequest("ws://test/collab"),
            Unit,
        )
        val baselineTasks = dispatcher.pendingTaskCount

        repeat(100) { session.send(byteArrayOf(1)) }

        assertEquals(baselineTasks + 1, dispatcher.pendingTaskCount)
        session.terminate(CloseEvents.ResetConnection)
        server.shutdown()
    }

    @Test
    fun `established queue overflow schedules one connection abort`() = runBlocking {
        val dispatcher = PausedDispatcher()
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = fakeDocumentFactory(),
                allowAnonymous = true,
                maxEstablishedQueueSize = 1,
            ),
            parentScope = CoroutineScope(dispatcher),
        )
        val session = server.openSession(
            RejectingTransport(),
            HocuspocusRequest("ws://test/collab"),
            Unit,
        )
        val routingKey = RoutingKey("queued")
        val document = HocuspocusDocument(server, routingKey.documentName, FakeCrdtDocument())
        val connection = HocuspocusConnection(
            session,
            document,
            ConnectionAttempt(
                server,
                session.request,
                routingKey,
                session.socketId,
                MutableContext(Unit),
            ),
        )
        val bytes = FrameCodec.encode(routingKey, MessageType.Ping)
        val inbound = InboundFrame(null, FrameCodec.decodeView(bytes), bytes.size)
        val baselineTasks = dispatcher.pendingTaskCount

        repeat(100) { assertFalse(connection.enqueue(inbound)) }

        assertEquals(baselineTasks + 1, dispatcher.pendingTaskCount)
        connection.abort()
        session.terminate(CloseEvents.ResetConnection)
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
    fun `document unload waits for change hooks scheduled by completed mutations`() = runBlocking {
        val changeStarted = CompletableDeferred<Unit>()
        val releaseChange = CompletableDeferred<Unit>()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = fakeDocumentFactory(),
                extensions = listOf(
                    object : HocuspocusExtension<Unit> {
                        override suspend fun onChange(payload: ChangePayload<Unit>) {
                            changeStarted.complete(Unit)
                            releaseChange.await()
                        }
                    },
                ),
            ),
        )
        val direct = server.openDirectConnection("pending-change", Unit)

        direct.transact(FakeCrdtDocument::class) { it.value += 1 }
        changeStarted.await()
        val disconnect = async { direct.disconnect() }

        assertNotNull(server.document("pending-change"))
        assertFalse(disconnect.isCompleted)
        releaseChange.complete(Unit)
        disconnect.await()

        assertNull(server.document("pending-change"))
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

    private class RejectingTransport : SocketTransport {
        override val isOpen: Boolean = true

        override fun send(bytes: ByteArray): Boolean = false

        override fun close(code: Int, reason: String) = Unit
    }

    private class PausedDispatcher : CoroutineDispatcher() {
        private val tasks = ConcurrentLinkedQueue<Runnable>()

        val pendingTaskCount: Int
            get() = tasks.size

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            tasks.add(block)
        }
    }
}
