package ai.hocuspocus.yks

import ai.hocuspocus.core.AuthenticatePayload
import ai.hocuspocus.core.AwarenessHookPayload
import ai.hocuspocus.core.AwarenessUpdatePayload
import ai.hocuspocus.core.ClientSession
import ai.hocuspocus.core.ChangePayload
import ai.hocuspocus.core.CloseEvent
import ai.hocuspocus.core.ConnectedPayload
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusAuthenticator
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.MessageHookPayload
import ai.hocuspocus.core.DocumentHookPayload
import ai.hocuspocus.core.SocketTransport
import ai.hocuspocus.core.StorePayload
import ai.hocuspocus.core.TokenSyncPayload
import ai.hocuspocus.core.TransactionOrigin
import ai.hocuspocus.protocol.AuthMessageType
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.AwarenessCodec
import ai.hocuspocus.protocol.AwarenessEntry
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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.ZERO

class HocuspocusServerIntegrationTest {
    @Test
    fun `remote awareness crosses hooks with a redis origin and can be rewritten`() = runBlocking {
        val updated = CompletableDeferred<AwarenessUpdatePayload<Unit>>()
        lateinit var expectedDocument: ai.hocuspocus.core.HocuspocusDocument<Unit>
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun beforeHandleAwareness(payload: AwarenessHookPayload<Unit>) {
                assertSame(expectedDocument, payload.document)
                assertNull(payload.connection)
                assertEquals(TransactionOrigin.Redis, payload.transactionOrigin)
                payload.states[41] = buildJsonObject { put("name", "filtered") }
            }

            override suspend fun onAwarenessUpdate(payload: AwarenessUpdatePayload<Unit>) {
                updated.complete(payload)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(extension),
            ),
        )
        val direct = server.openDirectConnection("remote-awareness", Unit)
        expectedDocument = direct.document
        val update = AwarenessCodec.encode(
            listOf(AwarenessEntry(41, 1, buildJsonObject { put("name", "untrusted") })),
        )

        direct.document.applyRemoteAwareness(update)

        assertEquals(
            buildJsonObject { put("name", "filtered") },
            direct.document.awarenessStates().getValue(41),
        )
        val payload = withTimeout(2.seconds) { updated.await() }
        assertNull(payload.connection)
        assertEquals(TransactionOrigin.Redis, payload.transactionOrigin)
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `remote awareness hook rejection leaves state unchanged`() = runBlocking {
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun beforeHandleAwareness(payload: AwarenessHookPayload<Unit>) {
                if (payload.transactionOrigin == TransactionOrigin.Redis) error("rejected")
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(extension),
            ),
        )
        val direct = server.openDirectConnection("rejected-awareness", Unit)
        val update = AwarenessCodec.encode(
            listOf(AwarenessEntry(42, 1, buildJsonObject { put("name", "blocked") })),
        )

        assertFailsWith<IllegalStateException> {
            direct.document.applyRemoteAwareness(update)
        }
        assertTrue(direct.document.awarenessStates().isEmpty())
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `targeted closeConnections closes only matching websocket routes`() = runBlocking {
        val connectedCount = AtomicInteger()
        val bothConnected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                if (connectedCount.incrementAndGet() == 2) bothConnected.complete(Unit)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
            ),
        )
        val firstTransport = FakeTransport()
        val secondTransport = FakeTransport()
        val firstSession = server.openSession(
            firstTransport,
            HocuspocusRequest("ws://test/collab"),
            Unit,
            socketId = "socket-one",
        )
        val secondSession = server.openSession(
            secondTransport,
            HocuspocusRequest("ws://test/collab"),
            Unit,
            socketId = "socket-two",
        )
        firstSession.handleBinary(authFrame("first"))
        secondSession.handleBinary(authFrame("second"))
        assertEquals(MessageType.Auth, FrameCodec.decode(firstTransport.receive()).type)
        assertEquals(MessageType.Auth, FrameCodec.decode(secondTransport.receive()).type)
        withTimeout(2.seconds) { bothConnected.await() }
        assertTrue(server.isStarted)
        assertFalse(server.isClosed)
        assertEquals(setOf("first", "second"), server.documentNames())

        server.closeConnections("first")

        assertEquals(MessageType.Close, FrameCodec.decode(firstTransport.receive()).type)
        assertTrue(secondTransport.outgoing.tryReceive().isFailure)
        assertNull(server.document("first"))
        assertNotNull(server.document("second"))
        assertEquals(setOf("second"), server.documentNames())
        assertEquals(1, server.connectionsCount)
        assertTrue(firstTransport.isOpen)

        server.closeConnections()

        assertEquals(MessageType.Close, FrameCodec.decode(secondTransport.receive()).type)
        assertEquals(0, server.connectionsCount)
        server.shutdown()
        assertTrue(server.isClosed)
    }

    @Test
    fun `authenticates a v4 provider and synchronizes standard Yjs updates`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onAuthenticate(payload: AuthenticatePayload<Unit>) {
                assertEquals("secret", payload.token)
                assertEquals("4.6.0", payload.attempt.providerVersion)
            }

            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }
        }
        val fixture = server(extension)
        try {
            fixture.session.handleBinary(authFrame("doc"))
            val authResponse = FrameCodec.decode(fixture.transport.receive())
            assertEquals(MessageType.Auth, authResponse.type)
            val authReader = Lib0Reader(authResponse.payload)
            assertEquals(AuthMessageType.Authenticated.wireValue, authReader.readVarUint())
            assertEquals("read-write", authReader.readVarString())
            withTimeout(2.seconds) { connected.await() }

            val client = YDoc(clientId = 11, gc = false)
            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("doc"),
                    MessageType.Sync,
                    SyncCodec.encode(SyncMessageType.StepOne, client.encodeStateVector()),
                ),
            )
            val stepTwo = FrameCodec.decode(fixture.transport.receive())
            val serverStepOne = FrameCodec.decode(fixture.transport.receive())
            assertEquals(SyncMessageType.StepTwo, SyncCodec.decode(stepTwo.payload).type)
            assertEquals(SyncMessageType.StepOne, SyncCodec.decode(serverStepOne.payload).type)

            client.getText("body").insert(0, "hello 😀")
            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("doc"),
                    MessageType.Sync,
                    SyncCodec.encode(SyncMessageType.Update, client.encodeStateAsUpdate()),
                ),
            )
            val results = listOf(
                FrameCodec.decode(fixture.transport.receive()),
                FrameCodec.decode(fixture.transport.receive()),
            )
            assertTrue(results.any { it.type == MessageType.Sync })
            val status = results.single { it.type == MessageType.SyncStatus }
            assertEquals(1, Lib0Reader(status.payload).readVarUint())
            assertEquals(
                "hello 😀",
                textValue(assertNotNull(fixture.server.document("doc")).encodeStateAsUpdate()),
            )
        } finally {
            fixture.server.shutdown()
        }
    }

    @Test
    fun `enforces readonly updates while acknowledging already-known sync step two`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onAuthenticate(payload: AuthenticatePayload<Unit>) {
                payload.attempt.connectionConfiguration.readOnly = true
            }

            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }
        }
        val fixture = server(extension)
        try {
            fixture.session.handleBinary(authFrame("readonly"))
            fixture.transport.receive()
            withTimeout(2.seconds) { connected.await() }
            val client = YDoc(clientId = 12)
            client.getText("body").insert(0, "must not persist")
            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("readonly"),
                    MessageType.Sync,
                    SyncCodec.encode(SyncMessageType.Update, client.encodeStateAsUpdate()),
                ),
            )

            val status = FrameCodec.decode(fixture.transport.receive())
            assertEquals(MessageType.SyncStatus, status.type)
            assertEquals(0, Lib0Reader(status.payload).readVarUint())
            val serverDoc = assertNotNull(fixture.server.document("readonly"))
            assertEquals("", textValue(serverDoc.encodeStateAsUpdate()))
        } finally {
            fixture.server.shutdown()
        }
    }

    @Test
    fun `rewrites and broadcasts awareness through the suspending hook chain`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val awarenessSeen = CompletableDeferred<AwarenessUpdatePayload<Unit>>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }

            override suspend fun beforeHandleAwareness(payload: AwarenessHookPayload<Unit>) {
                payload.states[77] = buildJsonObject { put("name", "server-approved") }
            }

            override suspend fun onAwarenessUpdate(payload: AwarenessUpdatePayload<Unit>) {
                awarenessSeen.complete(payload)
            }
        }
        val fixture = server(extension)
        try {
            fixture.session.handleBinary(authFrame("aware"))
            fixture.transport.receive()
            withTimeout(2.seconds) { connected.await() }
            val incoming = AwarenessCodec.encode(
                listOf(AwarenessEntry(77, 1, buildJsonObject { put("name", "spoofed") })),
            )
            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("aware"),
                    MessageType.Awareness,
                    Lib0Writer().writeVarByteArray(incoming).toByteArray(),
                ),
            )

            val broadcast = FrameCodec.decode(fixture.transport.receive())
            assertEquals(MessageType.Awareness, broadcast.type)
            val awarenessBytes = Lib0Reader(broadcast.payload).readVarByteArray()
            val state = AwarenessCodec.decode(awarenessBytes).single().state as JsonObject
            assertEquals("server-approved", state["name"]?.toString()?.trim('"'))
            assertEquals(listOf(77L), withTimeout(2.seconds) { awarenessSeen.await() }.change.added)
            val document = assertNotNull(fixture.server.document("aware"))
            assertTrue(document.hasAwarenessStates())
            assertEquals(setOf(77L), document.getClients(document.connections().single()))
        } finally {
            fixture.server.shutdown()
        }
    }

    @Test
    fun `accepts provider version on v4 token refresh responses`() = runBlocking {
        val tokenSeen = CompletableDeferred<String>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                payload.connection.requestToken()
            }

            override suspend fun onTokenSync(payload: TokenSyncPayload<Unit>) {
                tokenSeen.complete(payload.token)
            }
        }
        val fixture = server(extension)
        try {
            fixture.session.handleBinary(authFrame("token-sync"))
            fixture.transport.receive()
            val request = FrameCodec.decode(fixture.transport.receive())
            assertEquals(MessageType.Auth, request.type)
            assertEquals(AuthMessageType.Token.wireValue, Lib0Reader(request.payload).readVarUint())

            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("token-sync"),
                    MessageType.Auth,
                    AuthenticationCodec.encodeClient(ClientAuthentication("refreshed", "4.6.0")),
                ),
            )
            assertEquals("refreshed", withTimeout(2.seconds) { tokenSeen.await() })
        } finally {
            fixture.server.shutdown()
        }
    }

    @Test
    fun `bounds messages queued before authentication`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                maxUnauthenticatedQueueMessages = 1,
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        val frame = FrameCodec.encode(
            RoutingKey("pending"),
            MessageType.Stateless,
            Lib0Writer().writeVarString("queued").toByteArray(),
        )

        session.handleBinary(frame)
        session.handleBinary(frame)

        val close = withTimeout(2.seconds) { transport.closed.receive() }
        assertEquals(4205, close.code)
        assertFalse(transport.isOpen)
        server.shutdown()
    }

    @Test
    fun `rejects websocket authentication by default when no authenticator is configured`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)

        session.handleBinary(authFrame("secure-default"))

        val response = FrameCodec.decode(transport.receive())
        assertEquals(MessageType.Auth, response.type)
        val reader = Lib0Reader(response.payload)
        assertEquals(AuthMessageType.PermissionDenied.wireValue, reader.readVarUint())
        assertTrue(reader.readVarString().isNotBlank())
        assertEquals(0, server.connectionsCount)
        server.shutdown()
    }

    @Test
    fun `bounds total established routes on one physical socket`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                maxDocumentsPerSocket = 1,
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("one"))
        transport.receive()
        withTimeout(2.seconds) {
            while (server.connectionsCount != 1) delay(5.milliseconds)
        }

        session.handleBinary(authFrame("two"))

        assertEquals(4205, withTimeout(2.seconds) { transport.closed.receive() }.code)
        assertFalse(transport.isOpen)
        server.shutdown()
    }

    @Test
    fun `bounds awareness identities owned by one connection`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                maxAwarenessClientsPerConnection = 1,
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("awareness-limit"))
        transport.receive()
        withTimeout(2.seconds) {
            while (server.connectionsCount != 1) delay(5.milliseconds)
        }
        val state = buildJsonObject { put("name", "client") }
        val update = AwarenessCodec.encode(
            listOf(AwarenessEntry(1, 1, state), AwarenessEntry(2, 1, state)),
        )

        session.handleBinary(
            FrameCodec.encode(
                RoutingKey("awareness-limit"),
                MessageType.Awareness,
                Lib0Writer().writeVarByteArray(update).toByteArray(),
            ),
        )

        assertEquals(MessageType.Close, FrameCodec.decode(transport.receive()).type)
        server.shutdown()
    }

    @Test
    fun `bounds established document queues by bytes`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                maxEstablishedQueueSize = 1,
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("bounded-established"))
        transport.receive()

        session.handleBinary(FrameCodec.encode(RoutingKey("bounded-established"), MessageType.QueryAwareness))

        val close = FrameCodec.decode(transport.receive())
        assertEquals(MessageType.Close, close.type)
        assertEquals(0, server.connectionsCount)
        server.shutdown()
    }

    @Test
    fun `loads a shared document once across concurrent socket authentication`() = runBlocking {
        val loadCount = AtomicInteger()
        val connectedCount = AtomicInteger()
        val bothConnected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun onLoadDocument(payload: DocumentHookPayload<Unit>): ByteArray? {
                loadCount.incrementAndGet()
                delay(100.milliseconds)
                return null
            }

            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                if (connectedCount.incrementAndGet() == 2) bothConnected.complete(Unit)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
            ),
        )
        val firstTransport = FakeTransport()
        val secondTransport = FakeTransport()
        val first = server.openSession(firstTransport, HocuspocusRequest("ws://test"), Unit)
        val second = server.openSession(secondTransport, HocuspocusRequest("ws://test"), Unit)
        assertEquals(0, server.connectionsCount)

        first.handleBinary(authFrame("shared"))
        second.handleBinary(authFrame("shared"))
        firstTransport.receive()
        secondTransport.receive()
        withTimeout(2.seconds) { bothConnected.await() }

        assertEquals(1, loadCount.get())
        assertEquals(1, server.documentsCount)
        assertEquals(2, server.connectionsCount)
        server.shutdown()
    }

    @Test
    fun `afterHandleMessage failure is reported without closing the connection`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val reported = CompletableDeferred<Throwable>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }

            override suspend fun afterHandleMessage(payload: MessageHookPayload<Unit>) {
                error("after hook failed")
            }
        }
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
                onError = { reported.complete(it) },
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test"), Unit)
        session.handleBinary(authFrame("after-hook"))
        transport.receive()
        withTimeout(2.seconds) { connected.await() }

        session.handleBinary(FrameCodec.encode(RoutingKey("after-hook"), MessageType.QueryAwareness))
        assertEquals(MessageType.Awareness, FrameCodec.decode(transport.receive()).type)
        assertEquals("after hook failed", withTimeout(2.seconds) { reported.await() }.message)
        assertTrue(transport.isOpen)
        server.shutdown()
    }

    @Test
    fun `authentication timeout closes an idle unauthenticated socket`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                timeout = 50.milliseconds,
            ),
        )
        server.openSession(transport, HocuspocusRequest("ws://test"), Unit)

        assertEquals(4408, withTimeout(2.seconds) { transport.closed.receive() }.code)
        assertFalse(transport.isOpen)
        server.shutdown()
    }

    @Test
    fun `idle timeout remains active after authentication`() = runBlocking {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                timeout = 50.milliseconds,
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test"), Unit)
        session.handleBinary(authFrame("idle"))
        transport.receive()

        assertEquals(4408, withTimeout(2.seconds) { transport.closed.receive() }.code)
        assertFalse(transport.isOpen)
        server.shutdown()
    }

    @Test
    fun `socket close waits for queued document messages before disconnect`() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val stored = CompletableDeferred<String>()
        val updateCount = AtomicInteger()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun beforeSync(payload: ai.hocuspocus.core.SyncHookPayload<Unit>) {
                if (payload.type == SyncMessageType.Update && updateCount.incrementAndGet() == 1) {
                    entered.complete(Unit)
                    release.await()
                }
            }

            override suspend fun onStoreDocument(payload: StorePayload<Unit>) {
                stored.complete(textValue(payload.document.encodeStateAsUpdate()))
            }
        }
        val fixture = server(extension)
        try {
            fixture.session.handleBinary(authFrame("ordered-close"))
            fixture.transport.receive()
            val client = YDoc(clientId = 99)
            client.getText("body").insert(0, "a")
            val firstUpdate = client.encodeStateAsUpdate()
            val firstStateVector = client.encodeStateVector()
            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("ordered-close"),
                    MessageType.Sync,
                    SyncCodec.encode(SyncMessageType.Update, firstUpdate),
                ),
            )
            withTimeout(2.seconds) { entered.await() }
            client.getText("body").insert(1, "b")
            fixture.session.handleBinary(
                FrameCodec.encode(
                    RoutingKey("ordered-close"),
                    MessageType.Sync,
                    SyncCodec.encode(
                        SyncMessageType.Update,
                        client.encodeStateAsUpdate(firstStateVector),
                    ),
                ),
            )

            val closing = async { fixture.session.close() }
            delay(25.milliseconds)
            assertFalse(closing.isCompleted)
            release.complete(Unit)
            withTimeout(2.seconds) { closing.await() }

            assertEquals("ab", withTimeout(2.seconds) { stored.await() })
            assertEquals(2, updateCount.get())
            assertNull(fixture.server.document("ordered-close"))
        } finally {
            fixture.server.shutdown()
        }
    }

    @Test
    fun `batches update broadcasts while change hooks remain per transaction`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val changes = AtomicInteger()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }

            override suspend fun onChange(payload: ChangePayload<Unit>) {
                changes.incrementAndGet()
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
                flushDelay = 1.seconds,
            ),
        )
        val transport = FakeTransport()
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("batch-updates"))
        transport.receive()
        withTimeout(2.seconds) { connected.await() }
        val direct = server.openDirectConnection("batch-updates", Unit)

        direct.transactYks { it.getMap("values").set("a", 1) }
        direct.transactYks { it.getMap("values").set("b", 2) }
        direct.transactYks { it.getMap("values").set("c", 3) }

        withTimeout(2.seconds) {
            while (changes.get() != 3) delay(1.milliseconds)
        }
        assertTrue(transport.outgoing.tryReceive().isFailure)
        direct.document.flush()
        val frame = FrameCodec.decode(transport.receive())
        assertEquals(MessageType.Sync, frame.type)
        val sync = SyncCodec.decode(frame.payload)
        assertEquals(SyncMessageType.Update, sync.type)
        val client = YDoc(clientId = 800)
        client.applyUpdate(sync.updateOrStateVector)
        assertEquals(1L, client.getMap("values").get("a"))
        assertEquals(2L, client.getMap("values").get("b"))
        assertEquals(3L, client.getMap("values").get("c"))
        delay(25.milliseconds)
        assertTrue(transport.outgoing.tryReceive().isFailure)

        client.destroy()
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `batches by default at the next scheduler turn and flush is idempotent`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
            ),
            parentScope = this,
        )
        assertEquals(ZERO, server.configuration.flushDelay)
        val transport = FakeTransport()
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("default-batch"))
        transport.receive()
        withTimeout(2.seconds) { connected.await() }
        val direct = server.openDirectConnection("default-batch", Unit)

        direct.document.flush().flush()
        assertTrue(transport.outgoing.tryReceive().isFailure)
        direct.transactYks { it.getMap("values").set("a", 1) }
        direct.transactYks { it.getMap("values").set("b", 2) }
        assertTrue(transport.outgoing.tryReceive().isFailure)

        yield()
        val update = SyncCodec.decode(FrameCodec.decode(transport.receive()).payload).updateOrStateVector
        val client = YDoc(clientId = 801)
        client.applyUpdate(update)
        assertEquals(1L, client.getMap("values").get("a"))
        assertEquals(2L, client.getMap("values").get("b"))
        client.destroy()
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `disabled batching sends one update message per change`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
                flushDelay = null,
            ),
        )
        val transport = FakeTransport()
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("unbatched"))
        transport.receive()
        withTimeout(2.seconds) { connected.await() }
        val direct = server.openDirectConnection("unbatched", Unit)

        direct.transactYks { it.getMap("values").set("a", 1) }
        direct.transactYks { it.getMap("values").set("b", 2) }
        direct.transactYks { it.getMap("values").set("c", 3) }

        val client = YDoc(clientId = 802)
        repeat(3) {
            val frame = FrameCodec.decode(transport.receive())
            assertEquals(SyncMessageType.Update, SyncCodec.decode(frame.payload).type)
            client.applyUpdate(SyncCodec.decode(frame.payload).updateOrStateVector)
        }
        assertEquals(1L, client.getMap("values").get("a"))
        assertEquals(2L, client.getMap("values").get("b"))
        assertEquals(3L, client.getMap("values").get("c"))
        assertTrue(transport.outgoing.tryReceive().isFailure)
        client.destroy()
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `flush threshold sends pending updates immediately`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
                flushMaxBytes = 1,
            ),
        )
        val transport = FakeTransport()
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("flush-threshold"))
        transport.receive()
        withTimeout(2.seconds) { connected.await() }
        val direct = server.openDirectConnection("flush-threshold", Unit)

        direct.transactYks { it.getMap("values").set("a", 1) }
        direct.transactYks { it.getMap("values").set("b", 2) }

        assertEquals(SyncMessageType.Update, SyncCodec.decode(FrameCodec.decode(transport.receive()).payload).type)
        assertEquals(SyncMessageType.Update, SyncCodec.decode(FrameCodec.decode(transport.receive()).payload).type)
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `batches awareness to the latest state until an explicit flush`() = runBlocking {
        val connected = CompletableDeferred<Unit>()
        val awarenessHooks = AtomicInteger()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                connected.complete(Unit)
            }

            override suspend fun onAwarenessUpdate(payload: AwarenessUpdatePayload<Unit>) {
                awarenessHooks.incrementAndGet()
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
                flushDelay = 1.seconds,
            ),
        )
        val transport = FakeTransport()
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        session.handleBinary(authFrame("batch-awareness"))
        transport.receive()
        withTimeout(2.seconds) { connected.await() }
        val document = assertNotNull(server.document("batch-awareness"))

        repeat(3) { index ->
            document.applyRemoteAwareness(
                AwarenessCodec.encode(
                    listOf(AwarenessEntry(77, (index + 1).toLong(), buildJsonObject { put("cursor", index + 1) })),
                ),
            )
        }
        withTimeout(2.seconds) {
            while (awarenessHooks.get() != 3) delay(1.milliseconds)
        }
        assertTrue(transport.outgoing.tryReceive().isFailure)

        document.flush()
        val frame = FrameCodec.decode(transport.receive())
        assertEquals(MessageType.Awareness, frame.type)
        val states = AwarenessCodec.decode(Lib0Reader(frame.payload).readVarByteArray())
        val state = states.single().state as JsonObject
        assertEquals("3", state["cursor"].toString())
        assertTrue(transport.outgoing.tryReceive().isFailure)
        server.shutdown()
    }

    @Test
    fun `shares one encoded broadcast buffer across matching routing keys`() = runBlocking {
        val connectedCount = AtomicInteger()
        val bothConnected = CompletableDeferred<Unit>()
        val extension = object : HocuspocusExtension<Unit> {
            override suspend fun connected(payload: ConnectedPayload<Unit>) {
                if (connectedCount.incrementAndGet() == 2) bothConnected.complete(Unit)
            }
        }
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
                flushDelay = null,
            ),
        )
        val firstTransport = FakeTransport()
        val secondTransport = FakeTransport()
        val first = server.openSession(firstTransport, HocuspocusRequest("ws://test/collab"), Unit, "one")
        val second = server.openSession(secondTransport, HocuspocusRequest("ws://test/collab"), Unit, "two")
        first.handleBinary(authFrame("shared-buffer"))
        second.handleBinary(authFrame("shared-buffer"))
        firstTransport.receive()
        secondTransport.receive()
        withTimeout(2.seconds) { bothConnected.await() }
        val direct = server.openDirectConnection("shared-buffer", Unit)

        direct.transactYks { it.getText("body").insert(0, "shared") }

        val firstFrame = firstTransport.receive()
        val secondFrame = secondTransport.receive()
        assertSame(firstFrame, secondFrame)
        direct.disconnect()
        server.shutdown()
    }

    @Test
    fun `shutdown cancels only the server child scope`() = runBlocking {
        val parent = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
            parentScope = parent,
        )
        server.start()

        server.shutdown()

        assertTrue(parent.isActive)
        parent.coroutineContext[kotlinx.coroutines.Job]?.cancel()
    }

    private suspend fun server(extension: HocuspocusExtension<Unit>): Fixture {
        val transport = FakeTransport()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = testAuthenticator,
                extensions = listOf(extension),
            ),
        )
        val session = server.openSession(transport, HocuspocusRequest("ws://test/collab"), Unit)
        return Fixture(server, session, transport)
    }

    private fun authFrame(documentName: String): ByteArray = FrameCodec.encode(
        RoutingKey(documentName),
        MessageType.Auth,
        AuthenticationCodec.encodeClient(ClientAuthentication("secret", "4.6.0")),
    )

    private val testAuthenticator: HocuspocusAuthenticator<Unit> = HocuspocusAuthenticator { payload ->
        if (payload.token != "secret") {
            throw ai.hocuspocus.core.HocuspocusAuthenticationException()
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

    private data class Fixture(
        val server: HocuspocusServer<Unit>,
        val session: ClientSession<Unit>,
        val transport: FakeTransport,
    )

    private class FakeTransport : SocketTransport {
        private val open = AtomicBoolean(true)
        val outgoing = Channel<ByteArray>(Channel.UNLIMITED)
        val closed = Channel<CloseEvent>(Channel.UNLIMITED)

        override val isOpen: Boolean
            get() = open.get()

        override fun send(bytes: ByteArray): Boolean = open.get() && outgoing.trySend(bytes).isSuccess

        override fun close(code: Int, reason: String) {
            if (open.compareAndSet(true, false)) closed.trySend(CloseEvent(code, reason))
        }

        suspend fun receive(): ByteArray = withTimeout(2.seconds) { outgoing.receive() }
    }
}
