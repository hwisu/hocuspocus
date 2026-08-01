package ai.hocuspocus.throttle

import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.SocketTransport
import ai.hocuspocus.protocol.AuthMessageType
import ai.hocuspocus.protocol.AuthenticationCodec
import ai.hocuspocus.protocol.ClientAuthentication
import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.Lib0Reader
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.yks.YksDocumentFactory
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds

class ThrottleExtensionTest {
    @Test
    fun `rejects the attempt beyond the sliding window limit`() = runBlocking {
        val extension = ThrottleExtension<Unit>(
            ThrottleConfiguration(attempts = 1),
        )
        val server = server(extension)
        val first = authenticate(server, "first", "203.0.113.4")
        assertEquals(AuthMessageType.Authenticated, authType(first.receive()))

        val second = authenticate(server, "second", "203.0.113.4")
        assertEquals(AuthMessageType.PermissionDenied, authType(second.receive()))
        assertEquals(1, extension.trackedAddresses())

        server.shutdown()
    }

    @Test
    fun `does not trust forwarded headers by default`() = runBlocking {
        val extension = ThrottleExtension<Unit>()
        val server = server(extension)
        val transport = TestTransport()
        val session = server.openSession(
            transport,
            HocuspocusRequest(
                uri = "ws://test/collab",
                headers = mapOf("x-forwarded-for" to listOf("203.0.113.9")),
            ),
            Unit,
        )
        session.handleBinary(authFrame())

        assertEquals(AuthMessageType.PermissionDenied, authType(transport.receive()))
        server.shutdown()
    }

    @Test
    fun `validates bounded configuration`() {
        assertFailsWith<IllegalArgumentException> {
            ThrottleConfiguration(attempts = 0)
        }
    }

    private fun server(extension: ThrottleExtension<Unit>): HocuspocusServer<Unit> =
        HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                allowAnonymous = true,
                extensions = listOf(extension),
            ),
        )

    private suspend fun authenticate(
        server: HocuspocusServer<Unit>,
        socketId: String,
        address: String,
    ): TestTransport {
        val transport = TestTransport()
        val session = server.openSession(
            transport,
            HocuspocusRequest("ws://test/collab", remoteAddress = address),
            Unit,
            socketId,
        )
        session.handleBinary(authFrame())
        return transport
    }

    private fun authFrame(): ByteArray = FrameCodec.encode(
        RoutingKey("document"),
        MessageType.Auth,
        AuthenticationCodec.encodeClient(ClientAuthentication("", "4.4.0")),
    )

    private fun authType(frame: ByteArray): AuthMessageType {
        val decoded = FrameCodec.decode(frame)
        assertEquals(MessageType.Auth, decoded.type)
        return checkNotNull(
            AuthMessageType.fromWireValue(Lib0Reader(decoded.payload).readVarUint()),
        )
    }

    private class TestTransport : SocketTransport {
        private val frames: Channel<ByteArray> = Channel(Channel.UNLIMITED)
        override val isOpen: Boolean = true

        override fun send(bytes: ByteArray): Boolean = frames.trySend(bytes.copyOf()).isSuccess

        override fun close(code: Int, reason: String) = Unit

        suspend fun receive(): ByteArray = withTimeout(2.seconds) { frames.receive() }
    }
}
