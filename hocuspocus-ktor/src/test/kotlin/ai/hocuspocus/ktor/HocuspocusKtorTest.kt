package ai.hocuspocus.ktor

import ai.hocuspocus.core.AuthenticatePayload
import ai.hocuspocus.core.ConfigurePayload
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusAuthenticator
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.protocol.AuthMessageType
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.Lib0Reader
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.yks.YksDocumentFactory
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.server.application.install
import io.ktor.server.application.plugin
import io.ktor.server.testing.testApplication
import io.ktor.server.websocket.WebSockets as ServerWebSockets
import io.ktor.websocket.ChannelOverflow
import io.ktor.websocket.Frame
import io.ktor.websocket.readBytes
import io.ktor.websocket.send
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class HocuspocusKtorTest {
    @Test
    fun `starts the collaboration server and extensions with the Ktor application`() = testApplication {
        val configured = CompletableDeferred<Unit>()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(
                    object : HocuspocusExtension<Unit> {
                        override suspend fun onConfigure(payload: ConfigurePayload<Unit>) {
                            configured.complete(Unit)
                        }
                    },
                ),
            ),
        )
        application {
            install(HocuspocusKtor) { use(server) }
        }

        startApplication()

        assertTrue(server.isStarted)
        withTimeout(2.seconds) { configured.await() }
    }

    @Test
    fun `installs a Ktor websocket route and forwards request context`() = testApplication {
        val authenticatedHeader = CompletableDeferred<String?>()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                authenticator = HocuspocusAuthenticator { payload ->
                    if (payload.token != "token") error("invalid token")
                },
                extensions = listOf(
                    object : HocuspocusExtension<String> {
                        override suspend fun onAuthenticate(payload: AuthenticatePayload<String>) {
                            authenticatedHeader.complete(payload.attempt.context.value)
                        }
                    },
                ),
            ),
        )
        application {
            install(HocuspocusKtor) {
                path = "/collab"
                use(server) { request.headers["X-Tenant"] ?: "missing" }
            }
        }
        val websocketClient = createClient { install(WebSockets) }

        websocketClient.webSocket(
            path = "/collab",
            request = { headers.append("X-Tenant", "tenant-a") },
        ) {
            send(
                Frame.Binary(
                    fin = true,
                    data = FrameCodec.encode(
                        RoutingKey("ktor-doc"),
                        MessageType.Auth,
                        AuthenticationCodec.encodeClient(ClientAuthentication("token", "4.4.0")),
                    ),
                ),
            )
            val response = FrameCodec.decode((incoming.receive() as Frame.Binary).readBytes())
            assertEquals(MessageType.Auth, response.type)
            val reader = Lib0Reader(response.payload)
            assertEquals(AuthMessageType.Authenticated.wireValue, reader.readVarUint())
            assertEquals("read-write", reader.readVarString())
            assertEquals("tenant-a", withTimeout(2.seconds) { authenticatedHeader.await() })
        }
    }

    @Test
    fun `shuts down the collaboration server with the Ktor application`() {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        testApplication {
            application {
                install(HocuspocusKtor) { use(server) }
            }
            startApplication()
        }

        assertFailsWith<IllegalStateException> {
            runBlocking { server.start() }
        }
    }

    @Test
    fun `requires a finite application shutdown timeout`() {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        val configuration = HocuspocusKtorConfiguration().apply {
            use(server)
            shutdownTimeout = Duration.INFINITE
        }

        assertFailsWith<IllegalArgumentException> { configuration.validate() }
        runBlocking { server.shutdown() }
    }

    @Test
    fun `auto installed websocket channels are bounded end to end`() = testApplication {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        application {
            install(HocuspocusKtor) {
                webSocketIncomingQueueCapacity = 17
                webSocketOutgoingQueueCapacity = 19
                use(server)
            }

            val channels = plugin(ServerWebSockets).channelsConfig
            assertEquals(17, channels.incoming.capacity)
            assertEquals(19, channels.outgoing.capacity)
        }
    }

    @Test
    fun `requires positive websocket channel capacities`() {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        val configuration = HocuspocusKtorConfiguration().apply {
            use(server)
            webSocketIncomingQueueCapacity = 0
        }

        assertFailsWith<IllegalArgumentException> { configuration.validate() }
        runBlocking { server.shutdown() }
    }

    @Test
    fun `rejects preinstalled unlimited websocket channels by default`() {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )

        assertFailsWith<IllegalStateException> {
            testApplication {
                application {
                    install(ServerWebSockets)
                    install(HocuspocusKtor) {
                        installWebSockets = false
                        use(server)
                    }
                }
                startApplication()
            }
        }
        runBlocking { server.shutdown() }
    }

    @Test
    fun `accepts preinstalled bounded websocket channels`() = testApplication {
        val server = HocuspocusServer(
            HocuspocusConfiguration<Unit>(documentFactory = YksDocumentFactory()),
        )
        application {
            install(ServerWebSockets) {
                channels {
                    incoming = bounded(7, ChannelOverflow.CLOSE)
                    outgoing = bounded(11, ChannelOverflow.SUSPEND)
                }
            }
            install(HocuspocusKtor) {
                installWebSockets = false
                use(server)
            }
        }

        startApplication()
    }
}
