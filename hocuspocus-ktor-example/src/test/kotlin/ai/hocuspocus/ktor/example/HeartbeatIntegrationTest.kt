package ai.hocuspocus.ktor.example

import ai.hocuspocus.core.CloseEvents
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.ktor.HocuspocusKtor
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.yks.YksDocumentFactory
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.websocket.WebSockets
import io.ktor.websocket.ChannelOverflow
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class HeartbeatIntegrationTest {
    @Test
    fun `healthy pong responsive clients stay connected without application traffic`() = runBlocking {
        checkHeartbeat(authenticate = true, enablePings = true)
    }

    @Test
    fun `pongs cannot extend authentication and disabling pings retains the core idle timeout`() = runBlocking {
        checkHeartbeat(authenticate = false, enablePings = true)
        checkHeartbeat(authenticate = true, enablePings = false)
    }

    private suspend fun checkHeartbeat(authenticate: Boolean, enablePings: Boolean) {
        val server = HocuspocusServer(HocuspocusConfiguration<Unit>(
            documentFactory = YksDocumentFactory(), allowAnonymous = true, timeout = 500.milliseconds,
        ))
        val app = embeddedServer(Netty, host = "127.0.0.1", port = 0) {
            if (!enablePings) install(WebSockets) {
                pingPeriodMillis = 0
                timeoutMillis = 500
                channels {
                    incoming = bounded(256, ChannelOverflow.CLOSE)
                    outgoing = bounded(256, ChannelOverflow.SUSPEND)
                }
            }
            install(HocuspocusKtor) { use(server) }
        }.start(wait = false)
        val authenticated = CompletableDeferred<Unit>()
        val closed = CompletableDeferred<Int>()
        val pongs = AtomicInteger()
        val client = HttpClient.newHttpClient()
        var socket: WebSocket? = null
        try {
            val port = app.engine.resolvedConnectors().single().port
            socket = client.newWebSocketBuilder().buildAsync(
                URI("ws://127.0.0.1:$port/collab"),
                object : WebSocket.Listener {
                    override fun onOpen(ws: WebSocket) {
                        ws.request(1)
                        if (authenticate) ws.sendBinary(ByteBuffer.wrap(FrameCodec.encode(
                            RoutingKey("heartbeat"), MessageType.Auth,
                            AuthenticationCodec.encodeClient(ClientAuthentication("token", "4.7.1")),
                        )), true)
                    }
                    override fun onBinary(ws: WebSocket, data: ByteBuffer, last: Boolean): CompletionStage<*>? {
                        val bytes = ByteArray(data.remaining()).also(data::get)
                        if (FrameCodec.decode(bytes).type == MessageType.Auth) authenticated.complete(Unit)
                        ws.request(1)
                        return null
                    }
                    override fun onPing(ws: WebSocket, message: ByteBuffer): CompletionStage<*> {
                        pongs.incrementAndGet()
                        ws.request(1)
                        return ws.sendPong(message)
                    }
                    override fun onClose(ws: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
                        closed.complete(statusCode)
                        return null
                    }
                    override fun onError(ws: WebSocket, error: Throwable) {
                        closed.completeExceptionally(error)
                    }
                },
            ).get(5, TimeUnit.SECONDS)
            if (authenticate) withTimeout(3.seconds) { authenticated.await() }
            if (authenticate && enablePings) {
                delay(1600.milliseconds)
                assertTrue(pongs.get() >= 2)
                assertFalse(closed.isCompleted, "healthy silent clients must survive multiple idle checks")
            } else {
                assertEquals(CloseEvents.ConnectionTimeout.code, withTimeout(3.seconds) { closed.await() })
                if (enablePings) assertTrue(pongs.get() > 0)
            }
        } finally {
            socket?.abort()
            app.stop(0, 1000)
            client.close()
        }
    }
}
