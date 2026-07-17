package ai.hocuspocus.yks

import ai.hocuspocus.core.DatabaseExtension
import ai.hocuspocus.core.DisconnectPayload
import ai.hocuspocus.core.DocumentStorage
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusShutdownException
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.protocol.ProtocolException
import dev.yks.YDoc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DirectConnectionTest {
    @Test
    fun `direct disconnect stores first and notifies only for the last connection`() = runBlocking {
        val events = mutableListOf<String>()
        lateinit var server: HocuspocusServer<Unit>
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                events += "store"
            }

            override suspend fun onDisconnect(payload: DisconnectPayload<Unit>) {
                assertSame(server, payload.server)
                assertEquals(0, payload.clientsCount)
                assertEquals("http://localhost", payload.request.uri)
                events += "disconnect"
            }
        }
        server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(extension),
            ),
        )
        val first = server.openDirectConnection("lifecycle", Unit)
        val second = server.openDirectConnection("lifecycle", Unit)

        first.transactYks { it.getText("body").insert(0, "first") }
        first.disconnect()
        assertEquals(listOf("store"), events)

        second.transactYks { it.getText("body").insert(5, " second") }
        second.disconnect()
        assertEquals(listOf("store", "store", "disconnect"), events)
        assertNull(server.document("lifecycle"))
        server.shutdown()
    }

    @Test
    fun `deferred direct disconnect keeps the document warm and coalesces stores`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
                debounce = 200.milliseconds,
                maxDebounce = 200.milliseconds,
            ),
        )
        val direct = server.openDirectConnection("deferred", Unit)
        direct.transactYks { it.getText("body").insert(0, "a") }
        direct.transactYks { it.getText("body").insert(1, "b") }
        direct.transactYks { it.getText("body").insert(2, "c") }

        direct.disconnect(unloadImmediately = false)

        assertNotNull(server.document("deferred"))
        assertEquals(0, storage.storeCalls.get())
        withTimeout(2.seconds) {
            while (storage.storeCalls.get() == 0) delay(5.milliseconds)
            while (server.document("deferred") != null) delay(5.milliseconds)
        }
        assertEquals(1, storage.storeCalls.get())
        assertEquals("abc", textValue(storage.values.getValue("deferred")))
        server.shutdown()
    }

    @Test
    fun `before unload failure keeps the document loaded without failing disconnect`() = runBlocking {
        val rejectFirstUnload = AtomicBoolean(true)
        val reported = mutableListOf<Throwable>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun beforeUnloadDocument(payload: ai.hocuspocus.core.UnloadDocumentPayload<Unit>) {
                if (rejectFirstUnload.compareAndSet(true, false)) error("keep loaded")
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(extension),
                onError = reported::add,
            ),
        )

        server.openDirectConnection("veto-unload", Unit).disconnect()

        assertNotNull(server.document("veto-unload"))
        assertEquals("keep loaded", reported.single().message)

        server.openDirectConnection("veto-unload", Unit).disconnect()
        assertNull(server.document("veto-unload"))
        server.shutdown()
    }

    @Test
    fun `persists direct transactions and reloads the document`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )

        val first = server.openDirectConnection("direct", Unit)
        assertEquals(1, server.connectionsCount)
        first.transactYks { it.getText("body").insert(0, "durable") }
        first.disconnect()

        assertFalse(first.isOpen)
        assertEquals(0, server.connectionsCount)
        assertNull(server.document("direct"))
        assertTrue(storage.values.containsKey("direct"))

        val second = server.openDirectConnection("direct", Unit)
        assertEquals("durable", textValue(second.document.encodeStateAsUpdate()))
        second.disconnect()
        server.shutdown()
    }

    @Test
    fun `document convenience API reports emptiness and merges standard state`() = runBlocking {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = YksDocumentFactory(),
            ),
        )
        val source = server.openDirectConnection("merge-source", Unit)
        val target = server.openDirectConnection("merge-target", Unit)

        assertTrue(source.document.isEmpty("body"))
        assertTrue(target.document.isEmpty("body"))
        source.transactYks { it.getText("body").insert(0, "merged") }
        assertFalse(source.document.isEmpty("body"))
        target.transactYks { it.getText("body") }

        target.document.merge(source.document)

        assertFalse(target.document.isEmpty("body"))
        assertEquals("merged", textValue(target.document.encodeStateAsUpdate()))
        source.disconnect()
        target.disconnect()
        server.shutdown()
    }

    @Test
    fun `server shutdown closes and persists active direct connections`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        val direct = server.openDirectConnection("shutdown", Unit)
        direct.transactYks { it.getText("body").insert(0, "saved on shutdown") }

        server.shutdown()

        assertFalse(direct.isOpen)
        val restored = YDoc()
        restored.applyUpdate(storage.values.getValue("shutdown"))
        assertEquals("saved on shutdown", restored.getText("body").toString())
    }

    @Test
    fun `remote update is applied without writing it back to shared storage`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        val direct = server.openDirectConnection("remote", Unit)
        val remote = YDoc(clientId = 501)
        remote.getText("body").insert(0, "from another node")

        direct.document.applyRemoteUpdate(remote.encodeStateAsUpdate())

        assertEquals("from another node", textValue(direct.document.encodeStateAsUpdate()))
        direct.disconnect()
        assertFalse(storage.values.containsKey("remote"))
        remote.destroy()
        server.shutdown()
    }

    @Test
    fun `auto close persists before returning`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        val direct = server.openDirectConnection("blocking-close", Unit)
        direct.transactYks { it.getText("body").insert(0, "closed durably") }

        direct.close()

        assertFalse(direct.isOpen)
        assertEquals("closed durably", textValue(storage.values.getValue("blocking-close")))
        server.shutdown()
    }

    @Test
    fun `shutdown attempts every store and can retry after persistence failure`() = runBlocking {
        val storage = FailingStorage(failingNames = mutableSetOf("bad"))
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        server.openDirectConnection("bad", Unit)
            .transactYks { it.getText("body").insert(0, "retry me") }
        server.openDirectConnection("good", Unit)
            .transactYks { it.getText("body").insert(0, "store me") }

        assertFailsWith<HocuspocusShutdownException> { server.shutdown() }
        assertTrue("bad" in storage.attemptedNames)
        assertTrue("good" in storage.attemptedNames)
        assertTrue("good" in storage.values)
        assertEquals(1, server.documentsCount)
        assertTrue(server.document("bad") != null)

        storage.failingNames.clear()
        server.shutdown()

        assertEquals("retry me", textValue(storage.values.getValue("bad")))
        assertEquals(0, server.documentsCount)
    }

    @Test
    fun `bounds loaded documents and remote update bytes`() = runBlocking {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = YksDocumentFactory(),
                maxLoadedDocuments = 1,
                maxCrdtUpdateSize = 1,
            ),
        )
        val first = server.openDirectConnection("one", Unit)

        assertFailsWith<ProtocolException> { server.openDirectConnection("two", Unit) }
        assertFailsWith<ProtocolException> { first.document.applyRemoteUpdate(ByteArray(2)) }

        first.disconnect()
        server.shutdown()
    }

    @Test
    fun `shutdown waits for an in flight direct transaction before storing`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        val direct = server.openDirectConnection("in-flight", Unit)
        val retainedDocument = direct.document
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val transaction = async(Dispatchers.Default) {
            direct.transactYks {
                entered.countDown()
                release.await()
                it.getText("body").insert(0, "finished before store")
            }
        }
        assertTrue(entered.await(2, TimeUnit.SECONDS), "transaction did not enter within the test timeout")

        val shutdown = async(Dispatchers.Default) { server.shutdown() }
        delay(25.milliseconds)
        assertFalse(shutdown.isCompleted)
        release.countDown()

        transaction.await()
        shutdown.await()
        assertEquals("finished before store", textValue(storage.values.getValue("in-flight")))
        assertFailsWith<IllegalStateException> {
            retainedDocument.transactYks { it.getText("body").insert(0, "too late") }
        }
        Unit
    }

    @Test
    fun `disconnect waits for an in flight direct transaction before storing`() = runBlocking {
        val storage = MemoryStorage()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(DatabaseExtension<Unit>(storage)),
            ),
        )
        val direct = server.openDirectConnection("disconnect-in-flight", Unit)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val transaction = async(Dispatchers.Default) {
            direct.transactYks {
                entered.countDown()
                release.await()
                it.getText("body").insert(0, "stored before disconnect")
            }
        }
        assertTrue(entered.await(2, TimeUnit.SECONDS), "transaction did not enter within the test timeout")

        val disconnect = async(Dispatchers.Default) { direct.disconnect() }
        delay(25.milliseconds)
        assertFalse(disconnect.isCompleted)
        release.countDown()

        transaction.await()
        disconnect.await()
        assertEquals("stored before disconnect", textValue(storage.values.getValue("disconnect-in-flight")))
        server.shutdown()
    }

    private class MemoryStorage : DocumentStorage {
        val values = mutableMapOf<String, ByteArray>()
        val storeCalls = AtomicInteger()

        override suspend fun load(documentName: String): ByteArray? = values[documentName]?.copyOf()

        override suspend fun store(documentName: String, state: ByteArray) {
            storeCalls.incrementAndGet()
            values[documentName] = state.copyOf()
        }
    }

    private class FailingStorage(
        val failingNames: MutableSet<String>,
    ) : DocumentStorage {
        val attemptedNames = mutableSetOf<String>()
        val values = mutableMapOf<String, ByteArray>()

        override suspend fun load(documentName: String): ByteArray? = values[documentName]?.copyOf()

        override suspend fun store(documentName: String, state: ByteArray) {
            attemptedNames += documentName
            if (documentName in failingNames) error("store failed for $documentName")
            values[documentName] = state.copyOf()
        }
    }

    private fun textValue(update: ByteArray): String {
        val document = YDoc()
        return try {
            document.applyUpdate(update)
            document.getText("body").toString()
        } finally {
            document.destroy()
        }
    }
}
