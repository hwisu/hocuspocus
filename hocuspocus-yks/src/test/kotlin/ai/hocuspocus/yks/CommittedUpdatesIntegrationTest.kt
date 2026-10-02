package ai.hocuspocus.yks

import ai.hocuspocus.core.ChangePayload
import ai.hocuspocus.core.ConnectedPayload
import ai.hocuspocus.core.CrdtDocumentFactory
import ai.hocuspocus.core.DocumentHookPayload
import ai.hocuspocus.core.HocuspocusAuthenticator
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.SocketTransport
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.core.TransactionOrigin
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.protocol.SyncCodec
import ai.hocuspocus.protocol.SyncMessageType
import dev.yks.YDoc
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class CommittedUpdatesIntegrationTest {
    @Test
    fun `client observer failure still broadcasts and persists committed bytes`() = runBlocking {
        val changes = Channel<ChangePayload<Unit>>(Channel.UNLIMITED)
        val failures = Channel<Throwable>(Channel.UNLIMITED)
        val connected = CompletableDeferred<Unit>()
        val connectionCount = AtomicInteger()
        val updateLog = CopyOnWriteArrayList<ByteArray>()
        val stored = CompletableDeferred<ByteArray>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                if (connectionCount.incrementAndGet() == 2) connected.complete(Unit)
            }

            override suspend fun onChange(payload: ChangePayload<Unit>) {
                // Match consumers that append only client-origin deltas, then
                // compact that log instead of snapshotting the live document.
                if (payload.transactionOrigin is TransactionOrigin.Connection) {
                    updateLog += payload.update
                    changes.send(payload)
                }
            }

            override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                val replay = YDoc()
                try {
                    updateLog.forEach { replay.applyUpdate(it) }
                    stored.complete(replay.encodeStateAsUpdate())
                } finally {
                    replay.destroy()
                }
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = HocuspocusAuthenticator<Unit> { },
                extensions = listOf(extension),
                flushDelay = null,
                onError = { failures.trySend(it) },
            ),
        )
        val direct = server.openDirectConnection("client-failure", Unit)
        val source = YDoc(clientId = 123)
        val expected = IllegalStateException("observer failed after commit")
        try {
            direct.transactYks { it.getText("9253").observe { throw expected } }
            val sender = Transport()
            val peer = Transport()
            val senderSession = server.openSession(sender, HocuspocusRequest("ws://test"), Unit)
            val peerSession = server.openSession(peer, HocuspocusRequest("ws://test"), Unit)
            senderSession.handleBinary(authFrame("client-failure"))
            peerSession.handleBinary(authFrame("client-failure"))
            sender.receive()
            peer.receive()
            withTimeout(2.seconds) { connected.await() }

            source.getText("9253").insert(0, "committed")
            senderSession.handleBinary(FrameCodec.encode(
                RoutingKey("client-failure"),
                MessageType.Sync,
                SyncCodec.encode(SyncMessageType.Update, source.encodeStateAsUpdate()),
            ))

            assertSame(expected, withTimeout(2.seconds) { failures.receive() })
            val change = withTimeout(2.seconds) { changes.receive() }
            assertTrue(change.transactionOrigin is TransactionOrigin.Connection)
            assertEquals(Unit, change.context)
            assertEquals(setOf("9253"), change.changedRootNames)
            assertEquals("committed", textValue(change.update))
            val broadcast = SyncCodec.decode(FrameCodec.decode(peer.receive()).payload)
            assertEquals(SyncMessageType.Update, broadcast.type)
            assertEquals("committed", textValue(broadcast.updateOrStateVector))
            // The original failure still prevents a successful sync acknowledgement.
            while (true) {
                val frame = sender.outgoing.tryReceive().getOrNull() ?: break
                assertFalse(FrameCodec.decode(frame).type == MessageType.SyncStatus)
            }

            server.shutdown()
            assertEquals(1, updateLog.size)
            assertEquals("committed", textValue(withTimeout(2.seconds) { stored.await() }))
        } finally {
            source.destroy()
            server.shutdown()
        }
    }

    @Test
    fun `remote observer failure keeps Redis origin and skips local persistence`() = runBlocking {
        val changes = Channel<ChangePayload<Unit>>(Channel.UNLIMITED)
        val stores = AtomicInteger()
        val server = server(changes, stores)
        val direct = server.openDirectConnection("target", Unit)
        val source = YDoc(clientId = 123)
        val expected = IllegalStateException("remote observer failed")
        try {
            direct.transactYks { it.getText("9253").observe { throw expected } }
            source.getText("9253").insert(0, "remote")
            assertSame(expected, assertFailsWith<IllegalStateException> {
                direct.document.applyRemoteUpdate(source.encodeStateAsUpdate())
            })
            val change = withTimeout(2.seconds) { changes.receive() }
            assertEquals(TransactionOrigin.Redis, change.transactionOrigin)
            assertTrue(change.changedRootNames.isEmpty())
            assertEquals("remote", textValue(change.update))
            direct.document.applyRemoteUpdate(source.encodeStateAsUpdate())
            direct.disconnect()
            assertTrue(changes.tryReceive().isFailure, "duplicate update must remain a no-op")
            assertEquals(0, stores.get())
        } finally {
            source.destroy()
            server.shutdown()
        }
    }

    @Test
    fun `merge failure delivers committed delta without applying later documents`() = runBlocking {
        val changes = Channel<ChangePayload<Unit>>(Channel.UNLIMITED)
        val stores = AtomicInteger()
        val server = server(changes, stores)
        val target = server.openDirectConnection("target", Unit)
        val first = server.openDirectConnection("first", Unit)
        val later = server.openDirectConnection("later", Unit)
        val expected = IllegalArgumentException("merge observer failed")
        try {
            target.transactYks { it.getText("9253").observe { throw expected } }
            first.transactYks { it.getText("9253").insert(0, "first") }
            later.transactYks { it.getText("later").insert(0, "must not apply") }
            assertSame(expected, assertFailsWith<IllegalArgumentException> {
                target.document.merge(listOf(first.document, later.document), Unit)
            })
            val change = withTimeout(2.seconds) { changes.receive() }
            assertEquals(TransactionOrigin.Local(Unit), change.transactionOrigin)
            assertEquals(setOf("9253"), change.changedRootNames)
            assertEquals("first", textValue(change.update))
            assertEquals("", textValue(target.document.encodeStateAsUpdate(), "later"))
            target.disconnect()
            assertEquals(1, stores.get())
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun `cancelled local mutation preserves cancellation and skip store policy`() = runBlocking {
        for (skipStore in listOf(false, true)) {
            val changes = Channel<ChangePayload<Unit>>(Channel.UNLIMITED)
            val stores = AtomicInteger()
            val server = server(changes, stores)
            val direct = server.openDirectConnection("target", Unit)
            val expected = CancellationException("cancelled after editing")
            try {
                assertSame(expected, assertFailsWith<CancellationException> {
                    direct.transactYks(skipStoreHooks = skipStore) {
                        it.getText("9253").insert(0, "committed")
                        throw expected
                    }
                })
                val change = withTimeout(2.seconds) { changes.receive() }
                assertEquals(TransactionOrigin.Local(Unit, skipStore), change.transactionOrigin)
                assertEquals("committed", textValue(change.update))
                assertEquals(if (skipStore) emptySet() else setOf("9253"), change.changedRootNames)
                direct.disconnect()
                assertEquals(if (skipStore) 0 else 1, stores.get())
            } finally {
                server.shutdown()
            }
        }
    }

    @Test
    fun `failed initial load discards committed state and preserves original exception`() = runBlocking {
        val source = YDoc(clientId = 123)
        source.getText("9253").insert(0, "discarded")
        val expected = IllegalArgumentException("load observer failed")
        val changes = AtomicInteger()
        lateinit var native: YDoc
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onLoadDocument(payload: DocumentHookPayload<Unit>): ByteArray =
                source.encodeStateAsUpdate()

            override suspend fun onChange(payload: ChangePayload<Unit>) {
                changes.incrementAndGet()
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = CrdtDocumentFactory { options ->
                    YksDocumentFactory().create(options).also {
                        native = it.requireYDoc()
                        native.getText("9253").observe { throw expected }
                    }
                },
                extensions = listOf(extension),
            ),
        )
        try {
            assertSame(expected, assertFailsWith<IllegalArgumentException> {
                server.openDirectConnection("load-failure", Unit)
            })
            assertTrue(native.isDestroyed)
            assertTrue(server.documentNames().isEmpty())
            assertEquals(0, changes.get())
        } finally {
            source.destroy()
            server.shutdown()
        }
    }

    private fun server(changes: Channel<ChangePayload<Unit>>, stores: AtomicInteger): HocuspocusServer<Unit> =
        HocuspocusServer(HocuspocusConfiguration(
            documentFactory = YksDocumentFactory(),
            extensions = listOf(object : HocuspocusExtension<Unit> {
                override suspend fun onChange(payload: ChangePayload<Unit>) {
                    if (payload.document.name == "target") changes.send(payload)
                }

                override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                    if (payload.document.name == "target") stores.incrementAndGet()
                }
            }),
        ))

    private fun textValue(update: ByteArray, root: String = "9253"): String {
        val document = YDoc()
        return try {
            document.applyUpdate(update)
            document.getText(root).toString()
        } finally {
            document.destroy()
        }
    }

    private fun authFrame(name: String): ByteArray = FrameCodec.encode(
        RoutingKey(name),
        MessageType.Auth,
        AuthenticationCodec.encodeClient(ClientAuthentication("secret", "4.7.0")),
    )

    private class Transport : SocketTransport {
        @Volatile
        override var isOpen: Boolean = true
            private set
        val outgoing = Channel<ByteArray>(Channel.UNLIMITED)

        override fun send(bytes: ByteArray): Boolean = isOpen && outgoing.trySend(bytes).isSuccess

        override fun close(code: Int, reason: String) {
            isOpen = false
        }

        suspend fun receive(): ByteArray = withTimeout(2.seconds) { outgoing.receive() }
    }
}
