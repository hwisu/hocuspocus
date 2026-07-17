package ai.hocuspocus.webhook

import ai.hocuspocus.core.ChangePayload
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.TransactionOrigin
import ai.hocuspocus.yks.YksDocumentFactory
import ai.hocuspocus.yks.transactYks
import com.sun.net.httpserver.HttpServer
import dev.yks.YDoc
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.InetSocketAddress
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WebhookExtensionTest {
    @Test
    fun `sends signed standard state changes`() = runBlocking {
        WebhookFixture().use { fixture ->
            val extension = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Change)),
            )
            val server = server(extension)
            val connection = server.openDirectConnection("signed", Unit)
            connection.transactYks { it.getText("body").insert(0, "webhook") }

            val request = fixture.receive()
            assertEquals(extension.signature(request.body), request.signature)
            val envelope = Json.parseToJsonElement(
                request.body.toString(StandardCharsets.UTF_8),
            ).jsonObject
            assertEquals("change", envelope.getValue("event").jsonPrimitive.content)
            assertTrue(
                envelope.getValue("payload").jsonObject
                    .getValue("state").jsonPrimitive.content.isNotBlank(),
            )

            connection.disconnect()
            server.shutdown()
        }
    }

    @Test
    fun `loads a standard state update from create webhook`() = runBlocking {
        val source = YDoc()
        source.getText("body").insert(0, "created remotely")
        val encoded = source.encodeStateAsUpdate()
        source.destroy()

        WebhookFixture(
            responseBody = """{"state":"${Base64.getEncoder().encodeToString(encoded)}"}""",
        ).use { fixture ->
            val extension = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Create)),
            )
            val server = server(extension)
            val connection = server.openDirectConnection("created", Unit)
            val restored = YDoc()
            restored.applyUpdate(connection.document.encodeStateAsUpdate())

            assertEquals("created remotely", restored.getText("body").toString())
            restored.destroy()
            fixture.receive()
            connection.disconnect()
            server.shutdown()
        }
    }

    @Test
    fun `sends the Node webhook document contract through a transformer`() = runBlocking {
        WebhookFixture().use { fixture ->
            val extension = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Change)).copy(
                    payloadMode = WebhookPayloadMode.NodeCompatible,
                    transformer = TextDocumentTransformer(),
                ),
            )
            val server = server(extension)
            val connection = server.openDirectConnection("node-change", Unit)

            connection.transactYks { it.getText("body").insert(0, "node compatible") }

            val envelope = Json.parseToJsonElement(
                fixture.receive().body.toString(StandardCharsets.UTF_8),
            ).jsonObject
            val payload = envelope.getValue("payload").jsonObject
            assertEquals("change", envelope.getValue("event").jsonPrimitive.content)
            assertEquals("node-change", payload.getValue("documentName").jsonPrimitive.content)
            assertEquals(
                "node compatible",
                payload.getValue("document").jsonObject.getValue("body").jsonPrimitive.content,
            )
            assertTrue("state" !in payload)
            assertTrue("update" !in payload)
            assertTrue("requestHeaders" in payload)
            assertTrue("requestParameters" in payload)

            connection.disconnect()
            server.shutdown()
        }
    }

    @Test
    fun `loads Node webhook JSON fields through a transformer`() = runBlocking {
        WebhookFixture(
            responseBody = """{"body":"created from node JSON"}""",
        ).use { fixture ->
            val extension = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Create)).copy(
                    payloadMode = WebhookPayloadMode.NodeCompatible,
                    transformer = TextDocumentTransformer(),
                ),
            )
            val server = server(extension)
            val connection = server.openDirectConnection("node-create", Unit)
            val restored = YDoc()
            restored.applyUpdate(connection.document.encodeStateAsUpdate())

            assertEquals("created from node JSON", restored.getText("body").toString())
            val request = Json.parseToJsonElement(
                fixture.receive().body.toString(StandardCharsets.UTF_8),
            ).jsonObject.getValue("payload").jsonObject
            assertEquals("node-create", request.getValue("documentName").jsonPrimitive.content)
            assertTrue("requestHeaders" in request)
            assertTrue("requestParameters" in request)
            assertTrue("request" !in request)

            restored.destroy()
            connection.disconnect()
            server.shutdown()
        }
    }

    @Test
    fun `requires https unless explicitly enabled`() {
        assertFailsWith<IllegalArgumentException> {
            WebhookConfiguration<Unit>(
                endpoint = URI("http://127.0.0.1/webhook"),
                secret = TEST_SECRET,
            )
        }
    }

    @Test
    fun `requires a transformer for Node-compatible payloads`() {
        assertFailsWith<IllegalArgumentException> {
            WebhookConfiguration<Unit>(
                endpoint = URI("https://example.com/webhook"),
                secret = TEST_SECRET,
                payloadMode = WebhookPayloadMode.NodeCompatible,
            )
        }
    }

    @Test
    fun `bounds raw change payload before base64 encoding and network delivery`() = runBlocking {
        val errors = Channel<Throwable>(Channel.UNLIMITED)
        WebhookFixture().use { fixture ->
            val extension = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Change)).copy(maxChangePayloadBytes = 1),
            )
            val server = server(extension) { error ->
                errors.trySend(error)
                Unit
            }
            val connection = server.openDirectConnection("bounded", Unit)

            connection.transactYks { it.getText("body").insert(0, "too large") }

            val error = withTimeout(2.seconds) { errors.receive() }
            assertTrue(error is WebhookException)
            assertEquals(null, fixture.receiveOrNull())
            connection.disconnect()
            server.shutdown()
        }
    }

    @Test
    fun `bounds debounced changes by document count and total bytes`() = runBlocking {
        WebhookFixture().use { fixture ->
            val countBounded = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Change)).copy(
                    debounce = 1.seconds,
                    maxDebounce = 2.seconds,
                    maxPendingChanges = 1,
                ),
            )
            val countServer = server(countBounded)
            val first = countServer.openDirectConnection("first-pending", Unit)
            val second = countServer.openDirectConnection("second-pending", Unit)
            countBounded.onChange(change(first))

            assertFailsWith<WebhookException> {
                countBounded.onChange(change(second))
            }

            countServer.shutdown()
            fixture.receive()

            val bytesBounded = WebhookExtension<Unit>(
                fixture.configuration(setOf(WebhookEvent.Change)).copy(
                    debounce = 1.seconds,
                    maxDebounce = 2.seconds,
                    maxPendingChangeBytes = 1,
                ),
            )
            val bytesServer = server(bytesBounded)
            val connection = bytesServer.openDirectConnection("bytes-pending", Unit)

            assertFailsWith<WebhookException> {
                bytesBounded.onChange(change(connection))
            }

            bytesServer.shutdown()
            assertEquals(null, fixture.receiveOrNull())
        }
    }

    private fun change(
        connection: ai.hocuspocus.core.DirectConnection<Unit>,
    ): ChangePayload<Unit> = ChangePayload(
        document = connection.document,
        connection = null,
        context = Unit,
        update = ByteArray(0),
        transactionOrigin = TransactionOrigin.Local(Unit),
    )

    private fun server(
        extension: WebhookExtension<Unit>,
        onError: (Throwable) -> Unit = {},
    ): HocuspocusServer<Unit> =
        HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(extension),
                onError = onError,
            ),
        )

    private class WebhookFixture(
        private val responseBody: String = "{}",
    ) : AutoCloseable {
        private val requests: Channel<RecordedRequest> = Channel(Channel.UNLIMITED)
        private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            .apply {
                createContext("/hook") { exchange ->
                    val body = exchange.requestBody.use { it.readAllBytes() }
                    requests.trySend(
                        RecordedRequest(
                            body,
                            exchange.requestHeaders.getFirst("X-Hocuspocus-Signature-256"),
                        ),
                    )
                    val response = responseBody.toByteArray(StandardCharsets.UTF_8)
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.sendResponseHeaders(200, response.size.toLong())
                    exchange.responseBody.use { it.write(response) }
                }
                start()
            }

        fun configuration(events: Set<WebhookEvent>): WebhookConfiguration<Unit> =
            WebhookConfiguration(
                endpoint = URI("http://127.0.0.1:${server.address.port}/hook"),
                secret = TEST_SECRET,
                events = events,
                debounce = Duration.ZERO,
                allowInsecureHttp = true,
            )

        suspend fun receive(): RecordedRequest = withTimeout(2.seconds) { requests.receive() }

        suspend fun receiveOrNull(): RecordedRequest? =
            withTimeoutOrNull(100.milliseconds) { requests.receive() }

        override fun close() {
            server.stop(0)
        }
    }

    private data class RecordedRequest(
        val body: ByteArray,
        val signature: String?,
    )

    private class TextDocumentTransformer : WebhookDocumentTransformer<Unit> {
        override suspend fun fromDocument(
            document: ai.hocuspocus.core.HocuspocusDocument<Unit>,
        ): JsonElement {
            val snapshot = YDoc()
            return try {
                snapshot.applyUpdate(document.encodeStateAsUpdate())
                buildJsonObject {
                    put("body", snapshot.getText("body").toString())
                }
            } finally {
                snapshot.destroy()
            }
        }

        override suspend fun toUpdate(document: JsonObject): ByteArray? {
            val body = document["body"]?.jsonPrimitive?.content ?: return null
            val source = YDoc()
            return try {
                source.getText("body").insert(0, body)
                source.encodeStateAsUpdate()
            } finally {
                source.destroy()
            }
        }
    }

    private companion object {
        private const val TEST_SECRET: String = "0123456789abcdef0123456789abcdef"
    }
}
