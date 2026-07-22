package ai.hocuspocus.yks

import ai.hocuspocus.core.AuthenticatePayload
import ai.hocuspocus.core.ClientSession
import ai.hocuspocus.core.CloseEvent
import ai.hocuspocus.core.ConnectedPayload
import ai.hocuspocus.core.ConnectionAttempt
import ai.hocuspocus.core.DisconnectPayload
import ai.hocuspocus.core.DocumentHookPayload
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusAuthenticationException
import ai.hocuspocus.core.HocuspocusAuthenticator
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.MessageHookPayload
import ai.hocuspocus.core.SocketTransport
import ai.hocuspocus.core.StatelessPayload
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.core.SyncHookPayload
import ai.hocuspocus.core.TokenSyncPayload
import ai.hocuspocus.protocol.AuthMessageType
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.Lib0Reader
import ai.hocuspocus.protocol.Lib0Writer
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.protocol.SyncCodec
import ai.hocuspocus.protocol.SyncMessageType
import dev.yks.YDoc
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class UpstreamServerContractTest {
    @Test
    fun `stateless hooks can fan out while client broadcast opcode is rejected`() = runBlocking {
        val beforeBroadcast = AtomicInteger()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onStateless(payload: StatelessPayload<Unit>) {
                payload.connection.document.broadcastStateless(payload.payload)
            }

            override suspend fun beforeBroadcastStateless(
                payload: ai.hocuspocus.core.BroadcastStatelessPayload<Unit>,
            ) {
                beforeBroadcast.incrementAndGet()
            }
        }
        val server = server(extension)
        val first = connect(server, "socket-1", RoutingKey("shared"))
        val second = connect(server, "socket-2", RoutingKey("shared"))

        first.session.handleBinary(
            FrameCodec.encode(
                RoutingKey("shared"),
                MessageType.Stateless,
                Lib0Writer().writeVarString("hello").toByteArray(),
            ),
        )

        assertEquals("hello", statelessPayload(first.transport.receive()))
        assertEquals("hello", statelessPayload(second.transport.receive()))
        assertEquals(1, beforeBroadcast.get())

        first.session.handleBinary(
            FrameCodec.encode(
                RoutingKey("shared"),
                MessageType.BroadcastStateless,
                Lib0Writer().writeVarString("forged").toByteArray(),
            ),
        )

        assertEquals(MessageType.Close, FrameCodec.decode(first.transport.receive()).type)
        assertTrue(second.transport.frames.tryReceive().isFailure)
        server.shutdown()
    }

    @Test
    fun `before message failure aborts after hook and closes only that route`() = runBlocking {
        val afterCalls = AtomicInteger()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun beforeHandleMessage(payload: MessageHookPayload<Unit>) {
                error("reject message")
            }

            override suspend fun afterHandleMessage(payload: MessageHookPayload<Unit>) {
                afterCalls.incrementAndGet()
            }
        }
        val server = server(extension)
        val first = connect(server, "socket-1", RoutingKey("first"))
        val second = connect(server, "socket-2", RoutingKey("second"))

        first.session.handleBinary(FrameCodec.encode(RoutingKey("first"), MessageType.QueryAwareness))

        assertEquals(MessageType.Close, FrameCodec.decode(first.transport.receive()).type)
        assertEquals(0, afterCalls.get())
        assertTrue(second.transport.isOpen)
        assertEquals(1, server.connectionsCount)
        server.shutdown()
    }

    @Test
    fun `same socket multiplexes equal document names by session id`() = runBlocking {
        val connected = AtomicInteger()
        val versions = mutableListOf<String?>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                synchronized(versions) { versions += payload.connection.providerVersion }
                connected.incrementAndGet()
            }
        }
        val server = server(extension)
        val transport = TestTransport()
        val session = server.openSession(
            transport,
            HocuspocusRequest("ws://test/collab"),
            Unit,
            "physical",
        )
        val first = RoutingKey("shared", "provider-a")
        val second = RoutingKey("shared", "provider-b")

        session.handleBinary(authFrame(first))
        assertEquals(first, FrameCodec.decode(transport.receive()).routingKey)
        eventually { connected.get() == 1 }
        session.handleBinary(authFrame(second))
        assertEquals(second, FrameCodec.decode(transport.receive()).routingKey)
        eventually { connected.get() == 2 }

        assertEquals(1, server.connectionsCount)
        assertEquals(2, server.document("shared")?.connectionsCount)
        assertEquals(listOf("4.4.0", "4.4.0"), synchronized(versions) { versions.toList() })

        session.handleBinary(FrameCodec.encode(second, MessageType.QueryAwareness))
        val awareness = FrameCodec.decode(transport.receive())
        assertEquals(second, awareness.routingKey)
        assertEquals(MessageType.Awareness, awareness.type)
        server.shutdown()
    }

    @Test
    fun `authentication failure is isolated to one multiplexed route`() = runBlocking {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(
                documentFactory = YksDocumentFactory(),
                authenticator = HocuspocusAuthenticator { payload ->
                    if (payload.token == "bad") {
                        throw HocuspocusAuthenticationException(message = "rejected")
                    }
                },
            ),
        )
        val fixture = open(server, "physical")
        val rejected = RoutingKey("shared", "rejected")
        val accepted = RoutingKey("shared", "accepted")

        fixture.session.handleBinary(authFrame(rejected, token = "bad"))
        val deniedFrame = FrameCodec.decode(fixture.transport.receive())
        assertEquals(rejected, deniedFrame.routingKey)
        assertEquals(AuthMessageType.PermissionDenied, authType(deniedFrame))

        fixture.session.handleBinary(authFrame(accepted, token = "good"))
        val authenticatedFrame = FrameCodec.decode(fixture.transport.receive())
        assertEquals(accepted, authenticatedFrame.routingKey)
        assertEquals(AuthMessageType.Authenticated, authType(authenticatedFrame))

        eventually { server.document("shared")?.connectionsCount == 1 }
        assertTrue(fixture.transport.isOpen)
        assertEquals(1, server.connectionsCount)
        server.shutdown()
    }

    @Test
    fun `concurrent load failure rejects every waiter without unload callbacks`() = runBlocking {
        val loadEntered = CompletableDeferred<Unit>()
        val releaseLoad = CompletableDeferred<Unit>()
        val afterLoads = AtomicInteger()
        val afterUnloads = AtomicInteger()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onLoadDocument(payload: DocumentHookPayload<Unit>): ByteArray? {
                loadEntered.complete(Unit)
                releaseLoad.await()
                error("load failed")
            }

            override suspend fun afterLoadDocument(payload: DocumentHookPayload<Unit>) {
                afterLoads.incrementAndGet()
            }

            override suspend fun afterUnloadDocument(payload: ai.hocuspocus.core.UnloadDocumentPayload<Unit>) {
                afterUnloads.incrementAndGet()
            }
        }
        val server = server(extension)
        val first = open(server, "first")
        val second = open(server, "second")
        first.session.handleBinary(authFrame(RoutingKey("failed")))
        second.session.handleBinary(authFrame(RoutingKey("failed")))
        assertEquals(AuthMessageType.Authenticated, authType(first.transport.receive()))
        assertEquals(AuthMessageType.Authenticated, authType(second.transport.receive()))
        withTimeout(2.seconds) { loadEntered.await() }
        releaseLoad.complete(Unit)

        assertEquals(AuthMessageType.PermissionDenied, authType(first.transport.receive()))
        assertEquals(AuthMessageType.PermissionDenied, authType(second.transport.receive()))
        assertEquals(0, afterLoads.get())
        assertEquals(0, afterUnloads.get())
        assertEquals(0, server.documentsCount)
        assertEquals(0, server.connectionsCount)
        server.shutdown()
    }

    @Test
    fun `store hooks run by priority and disconnect waits for the store`() = runBlocking {
        val events = mutableListOf<String>()
        val low = object : HocuspocusExtension<Unit> {
            override val priority: Int = 10
            override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                events += "low-store"
                delay(25.milliseconds)
            }

            override suspend fun afterStoreDocument(payload: StorePayload<Unit>) {
                events += "low-after"
            }
        }
        val high = object : HocuspocusExtension<Unit> {
            override val priority: Int = 20
            override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                events += "high-store"
            }

            override suspend fun afterStoreDocument(payload: StorePayload<Unit>) {
                events += "high-after"
            }
        }
        val server = server(low, high)
        val direct = server.openDirectConnection("ordered-store", Unit)
        direct.transactYks { it.getText("body").insert(0, "change") }

        direct.disconnect()

        assertEquals(
            listOf("high-store", "low-store", "high-after", "low-after"),
            events,
        )
        assertNull(server.document("ordered-store"))
        server.shutdown()
    }

    @Test
    fun `provider close removes one route and keeps the physical socket reusable`() = runBlocking {
        val disconnected = CompletableDeferred<DisconnectPayload<Unit>>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onDisconnect(payload: DisconnectPayload<Unit>) {
                disconnected.complete(payload)
            }
        }
        val server = server(extension)
        val fixture = connect(server, "physical", RoutingKey("first"))

        fixture.session.handleBinary(FrameCodec.encode(RoutingKey("first"), MessageType.Close))

        assertEquals(MessageType.Close, FrameCodec.decode(fixture.transport.receive()).type)
        val payload = withTimeout(2.seconds) { disconnected.await() }
        assertSame(server, payload.server)
        assertEquals(0, payload.clientsCount)
        assertTrue(fixture.transport.isOpen)
        assertEquals(0, server.connectionsCount)

        fixture.session.handleBinary(authFrame(RoutingKey("second")))
        assertEquals(AuthMessageType.Authenticated, authType(fixture.transport.receive()))
        eventually { server.connectionsCount == 1 }
        server.shutdown()
    }

    @Test
    fun `token sync failure closes the affected routed connection`() = runBlocking {
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onTokenSync(payload: TokenSyncPayload<Unit>) {
                error("expired token")
            }
        }
        val server = server(extension)
        val fixture = connect(server, "physical", RoutingKey("token"))

        fixture.session.handleBinary(
            FrameCodec.encode(
                RoutingKey("token"),
                MessageType.Auth,
                AuthenticationCodec.encodeClient(ClientAuthentication("expired", "4.4.0")),
            ),
        )

        assertEquals(MessageType.Close, FrameCodec.decode(fixture.transport.receive()).type)
        eventually { server.connectionsCount == 0 }
        assertTrue(fixture.transport.isOpen)
        server.shutdown()
    }

    @Test
    fun `before sync observes each subtype before state is applied`() = runBlocking {
        val observed = Channel<SyncMessageType>(Channel.UNLIMITED)
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun beforeSync(payload: SyncHookPayload<Unit>) {
                observed.send(payload.type)
                if (payload.type == SyncMessageType.Update) {
                    assertEquals("", textValue(payload.connection.document.encodeStateAsUpdate()))
                }
            }
        }
        val server = server(extension)
        val fixture = connect(server, "physical", RoutingKey("sync"))
        val client = YDoc(clientId = 401)
        client.getText("body").insert(0, "applied later")

        fixture.session.handleBinary(
            FrameCodec.encodeSync(
                RoutingKey("sync"),
                SyncMessageType.StepOne,
                client.encodeStateVector(),
            ),
        )
        assertEquals(SyncMessageType.StepOne, withTimeout(2.seconds) { observed.receive() })
        fixture.transport.receive()
        fixture.transport.receive()

        fixture.session.handleBinary(
            FrameCodec.encodeSync(
                RoutingKey("sync"),
                SyncMessageType.Update,
                client.encodeStateAsUpdate(),
            ),
        )
        assertEquals(SyncMessageType.Update, withTimeout(2.seconds) { observed.receive() })
        eventually {
            server.document("sync")?.let { textValue(it.encodeStateAsUpdate()) } == "applied later"
        }
        client.destroy()
        server.shutdown()
    }

    private fun server(vararg extensions: HocuspocusExtension<Unit>): HocuspocusServer<Unit> =
        HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                allowAnonymous = true,
                extensions = extensions.toList(),
            ),
        )

    private suspend fun open(server: HocuspocusServer<Unit>, socketId: String): Fixture {
        val transport = TestTransport()
        val session = server.openSession(
            transport,
            HocuspocusRequest("ws://test/collab"),
            Unit,
            socketId,
        )
        return Fixture(session, transport)
    }

    private suspend fun connect(
        server: HocuspocusServer<Unit>,
        socketId: String,
        routingKey: RoutingKey,
    ): Fixture {
        val fixture = open(server, socketId)
        fixture.session.handleBinary(authFrame(routingKey))
        assertEquals(AuthMessageType.Authenticated, authType(fixture.transport.receive()))
        eventually {
            server.document(routingKey.documentName)?.connectionsCount?.let { it > 0 } == true
        }
        return fixture
    }

    private fun authFrame(routingKey: RoutingKey, token: String = ""): ByteArray = FrameCodec.encode(
        routingKey,
        MessageType.Auth,
        AuthenticationCodec.encodeClient(ClientAuthentication(token, "4.4.0")),
    )

    private fun authType(bytes: ByteArray): AuthMessageType {
        val frame = FrameCodec.decode(bytes)
        return authType(frame)
    }

    private fun authType(frame: ai.hocuspocus.protocol.HocuspocusFrame): AuthMessageType {
        assertEquals(MessageType.Auth, frame.type)
        return checkNotNull(AuthMessageType.fromWireValue(Lib0Reader(frame.payload).readVarUint()))
    }

    private fun statelessPayload(bytes: ByteArray): String {
        val frame = FrameCodec.decode(bytes)
        assertEquals(MessageType.Stateless, frame.type)
        return Lib0Reader(frame.payload).readVarString()
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

    private suspend fun eventually(assertion: suspend () -> Boolean) {
        withTimeout(2.seconds) {
            while (!assertion()) delay(5.milliseconds)
        }
    }

    private data class Fixture(
        val session: ClientSession<Unit>,
        val transport: TestTransport,
    )

    private class TestTransport : SocketTransport {
        private val open: AtomicBoolean = AtomicBoolean(true)
        val frames: Channel<ByteArray> = Channel(Channel.UNLIMITED)
        val closes: Channel<CloseEvent> = Channel(Channel.UNLIMITED)

        override val isOpen: Boolean
            get() = open.get()

        override fun send(bytes: ByteArray): Boolean =
            open.get() && frames.trySend(bytes.copyOf()).isSuccess

        override fun close(code: Int, reason: String) {
            if (open.compareAndSet(true, false)) closes.trySend(CloseEvent(code, reason))
        }

        suspend fun receive(): ByteArray = withTimeout(2.seconds) { frames.receive() }
    }
}
