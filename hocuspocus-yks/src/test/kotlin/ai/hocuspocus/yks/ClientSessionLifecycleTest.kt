package ai.hocuspocus.yks

import ai.hocuspocus.core.*
import ai.hocuspocus.protocol.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ClientSessionLifecycleTest {
    @Test
    fun `closed sessions ignore completion and rejection of blocking authentication hooks`() = runBlocking {
        for (duringConnect in listOf(false, true)) {
            for (reject in listOf(false, true)) {
                val entered = CompletableDeferred<Job>()
                val release = CountDownLatch(1)
                val loads = AtomicInteger()
                val connected = AtomicInteger()
                val errors = AtomicInteger()
                suspend fun blockHook() {
                    entered.complete(checkNotNull(currentCoroutineContext()[Job]))
                    check(release.await(5, TimeUnit.SECONDS))
                    if (reject) error("late hook rejection")
                }
                val server = HocuspocusServer(HocuspocusConfiguration<Unit>(
                    documentFactory = YksDocumentFactory(),
                    authenticator = HocuspocusAuthenticator { if (!duringConnect) blockHook() },
                    onError = { errors.incrementAndGet() },
                    extensions = listOf(object : HocuspocusExtension<Unit> {
                        override suspend fun onConnect(payload: ConnectionAttempt<Unit>) {
                            if (duringConnect) blockHook()
                        }
                        override suspend fun onLoadDocument(payload: DocumentHookPayload<Unit>): ByteArray? {
                            loads.incrementAndGet()
                            return null
                        }
                        override suspend fun connected(payload: ConnectedPayload<Unit>) {
                            connected.incrementAndGet()
                        }
                    }),
                ))
                val transport = RecordingTransport()
                try {
                    val session = server.openSession(transport, HocuspocusRequest("ws://test"), Unit)
                    session.handleBinary(authFrame())
                    val authentication = withTimeout(5.seconds) { entered.await() }
                    session.close()
                    release.countDown()
                    withTimeout(5.seconds) { authentication.join() }
                    assertEquals(0, transport.sends.get())
                    assertEquals(0, loads.get())
                    assertEquals(0, connected.get())
                    assertEquals(0, errors.get())
                    assertEquals(0, server.documentsCount)
                } finally {
                    release.countDown()
                    server.shutdown()
                }
            }
        }
    }

    @Test
    fun `out of band activity keeps authenticated sessions alive but cannot extend authentication`() = runBlocking {
        for (authenticate in listOf(true, false)) {
            val established = CompletableDeferred<Unit>()
            val transport = RecordingTransport()
            val server = HocuspocusServer(HocuspocusConfiguration<Unit>(
                documentFactory = YksDocumentFactory(), allowAnonymous = true, timeout = 300.milliseconds,
                extensions = listOf(object : HocuspocusExtension<Unit> {
                    override suspend fun connected(payload: ConnectedPayload<Unit>) { established.complete(Unit) }
                }),
            ))
            try {
                val session = server.openSession(transport, HocuspocusRequest("ws://test"), Unit)
                if (authenticate) {
                    session.handleBinary(authFrame())
                    withTimeout(3.seconds) { established.await() }
                }
                repeat(12) { session.touch(); delay(50.milliseconds) }
                if (authenticate) {
                    assertTrue(!transport.closed.isCompleted)
                    assertEquals(CloseEvents.ConnectionTimeout.code, withTimeout(3.seconds) { transport.closed.await() })
                } else {
                    assertEquals(CloseEvents.ConnectionTimeout.code, withTimeout(1.seconds) { transport.closed.await() })
                }
            } finally { server.shutdown() }
        }
    }

    private fun authFrame(): ByteArray = FrameCodec.encode(
        RoutingKey("lifecycle"), MessageType.Auth,
        AuthenticationCodec.encodeClient(ClientAuthentication("token", "4.7.1")),
    )

    private class RecordingTransport : SocketTransport {
        val sends = AtomicInteger()
        val closed = CompletableDeferred<Int>()
        override val isOpen: Boolean get() = !closed.isCompleted
        override fun send(bytes: ByteArray): Boolean { sends.incrementAndGet(); return isOpen }
        override fun close(code: Int, reason: String) { closed.complete(code) }
    }
}
