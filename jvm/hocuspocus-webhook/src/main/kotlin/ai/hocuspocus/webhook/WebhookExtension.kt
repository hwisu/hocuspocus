package ai.hocuspocus.webhook

import ai.hocuspocus.core.ChangePayload
import ai.hocuspocus.core.CloseEvents
import ai.hocuspocus.core.ConfigurePayload
import ai.hocuspocus.core.ConnectionAttempt
import ai.hocuspocus.core.DisconnectPayload
import ai.hocuspocus.core.DocumentHookPayload
import ai.hocuspocus.core.HocuspocusAuthenticationException
import ai.hocuspocus.core.HocuspocusDocument
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration as JavaDuration
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

public enum class WebhookEvent(public val wireName: String) {
    Change("change"),
    Connect("connect"),
    Create("create"),
    Disconnect("disconnect"),
}

public enum class WebhookPayloadMode {
    StandardUpdate,
    NodeCompatible,
}

/**
 * Converts between a managed JVM document and the JSON document contract used
 * by the Node `@hocuspocus/extension-webhook` package.
 *
 * [toUpdate] receives only fields that are empty in the document being loaded.
 * The returned bytes must be a genuine standard Yjs V1 update.
 */
public interface WebhookDocumentTransformer<C : Any> {
    public suspend fun fromDocument(document: HocuspocusDocument<C>): JsonElement

    public suspend fun toUpdate(document: JsonObject): ByteArray?
}

public data class WebhookConfiguration<C : Any>(
    val endpoint: URI,
    val secret: String,
    val events: Set<WebhookEvent> = setOf(WebhookEvent.Change),
    val payloadMode: WebhookPayloadMode = WebhookPayloadMode.StandardUpdate,
    val transformer: WebhookDocumentTransformer<C>? = null,
    val debounce: Duration? = 2.seconds,
    val maxDebounce: Duration = 10.seconds,
    val requestTimeout: Duration = 10.seconds,
    val maxRequestBytes: Int = 10 * 1024 * 1024,
    val maxResponseBytes: Int = 1024 * 1024,
    val maxLoadedStateBytes: Int = 5 * 1024 * 1024,
    val maxChangePayloadBytes: Int = 6 * 1024 * 1024,
    val maxPendingChanges: Int = 1_000,
    val maxPendingChangeBytes: Long = 64L * 1024 * 1024,
    val allowInsecureHttp: Boolean = false,
    val forwardedHeaders: Set<String> = emptySet(),
    val forwardedParameters: Set<String> = emptySet(),
    val forwardRemoteAddress: Boolean = false,
    val contextEncoder: (C?) -> JsonElement = { JsonNull },
) {
    init {
        require(secret.toByteArray(StandardCharsets.UTF_8).size >= 16) {
            "secret must contain at least 16 UTF-8 bytes"
        }
        require(requestTimeout.isPositive() && requestTimeout.isFinite()) {
            "requestTimeout must be positive and finite"
        }
        require(maxRequestBytes > 0) { "maxRequestBytes must be positive" }
        require(maxResponseBytes in 1 until Int.MAX_VALUE) {
            "maxResponseBytes must be positive and smaller than Int.MAX_VALUE"
        }
        require(maxLoadedStateBytes > 0) { "maxLoadedStateBytes must be positive" }
        require(maxChangePayloadBytes > 0) { "maxChangePayloadBytes must be positive" }
        require(maxPendingChanges > 0) { "maxPendingChanges must be positive" }
        require(maxPendingChangeBytes > 0) { "maxPendingChangeBytes must be positive" }
        require(debounce == null || (!debounce.isNegative() && debounce.isFinite())) {
            "debounce must be null or finite and not negative"
        }
        require(maxDebounce.isPositive() && maxDebounce.isFinite()) {
            "maxDebounce must be positive and finite"
        }
        require(debounce == null || maxDebounce >= debounce) {
            "maxDebounce must be greater than or equal to debounce"
        }
        require(endpoint.rawUserInfo == null) { "webhook endpoint must not contain user info" }
        require(endpoint.fragment == null) { "webhook endpoint must not contain a fragment" }
        require(!endpoint.host.isNullOrBlank()) { "webhook endpoint must have a host" }
        val allowedSchemes = if (allowInsecureHttp) setOf("https", "http") else setOf("https")
        require(endpoint.scheme?.lowercase() in allowedSchemes) {
            "webhook endpoint must use ${allowedSchemes.joinToString(" or ")}"
        }
        require(payloadMode != WebhookPayloadMode.NodeCompatible || transformer != null) {
            "NodeCompatible payload mode requires a transformer"
        }
    }
}

public class WebhookException(
    message: String,
    cause: Throwable? = null,
) : IllegalStateException(message, cause)

/**
 * Signed, bounded webhook integration using standard Yjs V1 state updates.
 *
 * Request headers and parameters are omitted unless explicitly allow-listed.
 * Redirects are disabled so a configured HTTPS endpoint cannot redirect into
 * an internal network.
 */
public class WebhookExtension<C : Any>(
    public val configuration: WebhookConfiguration<C>,
    private val client: HttpClient = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NEVER)
        .build(),
    private val nanoTime: () -> Long = System::nanoTime,
) : HocuspocusExtension<C> {
    override val name: String = "webhook"

    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val pendingMonitor: Any = Any()
    private val pendingChanges: MutableMap<String, PendingChange> = linkedMapOf()
    private var pendingChangeBytes: Long = 0
    private lateinit var server: HocuspocusServer<C>

    override suspend fun onConfigure(payload: ConfigurePayload<C>) {
        server = payload.server
    }

    override suspend fun onConnect(payload: ConnectionAttempt<C>) {
        if (WebhookEvent.Connect !in configuration.events) return
        try {
            post(
                envelope(
                    WebhookEvent.Connect,
                    when (configuration.payloadMode) {
                        WebhookPayloadMode.StandardUpdate -> connectionPayload(
                            payload.routingKey.documentName,
                            payload.context.value,
                            payload.request,
                        )
                        WebhookPayloadMode.NodeCompatible -> nodeRequestPayload(
                            payload.routingKey.documentName,
                            payload.request,
                        )
                    },
                ),
            )
        } catch (error: Throwable) {
            throw HocuspocusAuthenticationException(
                CloseEvents.Forbidden,
                "webhook rejected the connection",
            ).also { it.addSuppressed(error) }
        }
    }

    override suspend fun onLoadDocument(payload: DocumentHookPayload<C>): ByteArray? {
        if (WebhookEvent.Create !in configuration.events) return null
        val response = post(
            envelope(
                WebhookEvent.Create,
                when (configuration.payloadMode) {
                    WebhookPayloadMode.StandardUpdate -> connectionPayload(
                        payload.document.name,
                        payload.attempt.context.value,
                        payload.attempt.request,
                    )
                    WebhookPayloadMode.NodeCompatible -> nodeRequestPayload(
                        payload.document.name,
                        payload.attempt.request,
                    )
                },
            ),
        )
        if (response.isEmpty()) return null
        return when (configuration.payloadMode) {
            WebhookPayloadMode.StandardUpdate -> decodeStandardCreateResponse(response)
            WebhookPayloadMode.NodeCompatible -> decodeNodeCreateResponse(payload, response)
        }
    }

    override suspend fun onChange(payload: ChangePayload<C>) {
        if (WebhookEvent.Change !in configuration.events) return
        val body = when (configuration.payloadMode) {
            WebhookPayloadMode.StandardUpdate -> standardChangeEnvelope(payload)
            WebhookPayloadMode.NodeCompatible -> nodeChangeEnvelope(payload)
        }
        val debounce = configuration.debounce
        if (debounce == null || debounce == Duration.ZERO) {
            post(body)
        } else {
            scheduleChange(payload.document.name, body, debounce)
        }
    }

    override suspend fun onDisconnect(payload: DisconnectPayload<C>) {
        if (WebhookEvent.Disconnect !in configuration.events) return
        runCatching {
            post(
                envelope(
                    WebhookEvent.Disconnect,
                    when (configuration.payloadMode) {
                        WebhookPayloadMode.StandardUpdate -> connectionPayload(
                            payload.document.name,
                            payload.context,
                            payload.request,
                        )
                        WebhookPayloadMode.NodeCompatible -> buildJsonObject {
                            nodeRequestPayload(payload.document.name, payload.request)
                                .forEach { (key, value) -> put(key, value) }
                            put("context", configuration.contextEncoder(payload.context))
                        }
                    },
                ),
            )
        }.onFailure(server.configuration.onError)
    }

    override suspend fun onDestroy(server: HocuspocusServer<C>) {
        val pending = synchronized(pendingMonitor) {
            pendingChanges.values.toList().also {
                pendingChanges.clear()
                pendingChangeBytes = 0
            }
        }
        pending.forEach { change ->
            change.job.cancelAndJoin()
            runCatching { post(change.body) }.onFailure(server.configuration.onError)
        }
        scope.cancel()
    }

    public fun signature(body: ByteArray): String {
        val mac = Mac.getInstance(HMAC_ALGORITHM)
        mac.init(SecretKeySpec(configuration.secret.toByteArray(StandardCharsets.UTF_8), HMAC_ALGORITHM))
        return "sha256=${mac.doFinal(body).toHex()}"
    }

    private fun scheduleChange(documentName: String, body: ByteArray, debounce: Duration) {
        synchronized(pendingMonitor) {
            val previous = pendingChanges[documentName]
            if (previous == null && pendingChanges.size >= configuration.maxPendingChanges) {
                throw WebhookException("webhook pending change count exceeds maxPendingChanges")
            }
            val projectedBytes = Math.addExact(
                pendingChangeBytes - (previous?.body?.size?.toLong() ?: 0L),
                body.size.toLong(),
            )
            if (projectedBytes > configuration.maxPendingChangeBytes) {
                throw WebhookException("webhook pending change bytes exceed maxPendingChangeBytes")
            }
            val started = previous?.startedNanos ?: nanoTime()
            previous?.job?.cancel()
            val elapsed = (nanoTime() - started).coerceAtLeast(0)
            val wait = if (elapsed >= configuration.maxDebounce.inWholeNanoseconds) {
                Duration.ZERO
            } else {
                minOf(debounce, (configuration.maxDebounce.inWholeNanoseconds - elapsed).toDurationNanos())
            }
            lateinit var scheduled: PendingChange
            val job = scope.launch(start = CoroutineStart.LAZY) {
                delay(wait)
                val shouldSend = synchronized(pendingMonitor) {
                    pendingChanges.remove(documentName, scheduled).also { removed ->
                        if (removed) pendingChangeBytes -= scheduled.body.size.toLong()
                    }
                }
                if (shouldSend) {
                    runCatching { post(body) }.onFailure(server.configuration.onError)
                }
            }
            scheduled = PendingChange(started, body, job)
            pendingChanges[documentName] = scheduled
            pendingChangeBytes = projectedBytes
            job.start()
        }
    }

    private suspend fun post(body: ByteArray): ByteArray {
        if (body.size > configuration.maxRequestBytes) {
            throw WebhookException("webhook request exceeds maxRequestBytes")
        }
        val request = HttpRequest.newBuilder(configuration.endpoint)
            .timeout(JavaDuration.ofMillis(configuration.requestTimeout.inWholeMilliseconds))
            .header("Content-Type", "application/json")
            .header(SIGNATURE_HEADER, signature(body))
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build()
        val response = try {
            client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream()).await()
        } catch (error: Throwable) {
            throw WebhookException("webhook request failed", error)
        }
        val responseBytes = withContext(Dispatchers.IO) {
            response.body().use { stream ->
                stream.readNBytes(configuration.maxResponseBytes + 1)
            }
        }
        if (responseBytes.size > configuration.maxResponseBytes) {
            throw WebhookException("webhook response exceeds maxResponseBytes")
        }
        if (response.statusCode() !in 200..299) {
            throw WebhookException("webhook returned HTTP ${response.statusCode()}")
        }
        return responseBytes
    }

    private fun envelope(event: WebhookEvent, payload: JsonObject): ByteArray =
        Json.encodeToString(
            buildJsonObject {
                put("event", event.wireName)
                put("payload", payload)
            },
        ).toByteArray(StandardCharsets.UTF_8)

    private fun connectionPayload(
        documentName: String,
        context: C?,
        request: HocuspocusRequest,
    ): JsonObject = buildJsonObject {
        put("documentName", documentName)
        put("context", configuration.contextEncoder(context))
        put("request", requestJson(request))
    }

    private fun nodeRequestPayload(
        documentName: String,
        request: HocuspocusRequest,
    ): JsonObject = buildJsonObject {
        put("documentName", documentName)
        put("requestHeaders", filteredValues(request.headers, configuration.forwardedHeaders))
        put("requestParameters", filteredValues(request.parameters, configuration.forwardedParameters))
    }

    private fun decodeStandardCreateResponse(response: ByteArray): ByteArray? {
        val state = runCatching {
            Json.parseToJsonElement(response.toString(StandardCharsets.UTF_8))
                .jsonObject["state"]
                ?.jsonPrimitive
                ?.content
        }.getOrElse { error ->
            throw WebhookException("webhook create response is not valid JSON", error)
        } ?: return null
        val decoded = runCatching { Base64.getDecoder().decode(state) }
            .getOrElse { error -> throw WebhookException("webhook state is not valid base64", error) }
        if (decoded.size > configuration.maxLoadedStateBytes) {
            throw WebhookException("webhook state exceeds maxLoadedStateBytes")
        }
        return decoded
    }

    private suspend fun decodeNodeCreateResponse(
        payload: DocumentHookPayload<C>,
        response: ByteArray,
    ): ByteArray? {
        val responseDocument = runCatching {
            Json.parseToJsonElement(response.toString(StandardCharsets.UTF_8))
        }.getOrElse { error ->
            throw WebhookException("webhook create response is not valid JSON", error)
        }
        if (responseDocument == JsonNull) return null
        val fields = responseDocument as? JsonObject
            ?: throw WebhookException("Node-compatible webhook create response must be a JSON object")
        val emptyFields = JsonObject(fields.filterKeys(payload.document::isEmpty))
        if (emptyFields.isEmpty()) return null
        val update = checkNotNull(configuration.transformer)
            .toUpdate(emptyFields)
            ?: return null
        if (update.size > configuration.maxLoadedStateBytes) {
            throw WebhookException("webhook transformed state exceeds maxLoadedStateBytes")
        }
        return update
    }

    private fun standardChangeEnvelope(payload: ChangePayload<C>): ByteArray {
        val state = payload.document.encodeStateAsUpdate()
        val rawPayloadBytes = state.size.toLong() + payload.update.size.toLong()
        if (rawPayloadBytes > configuration.maxChangePayloadBytes.toLong()) {
            throw WebhookException("webhook change state and update exceed maxChangePayloadBytes")
        }
        return envelope(
            WebhookEvent.Change,
            buildJsonObject {
                put("documentName", payload.document.name)
                put("state", Base64.getEncoder().encodeToString(state))
                put("update", Base64.getEncoder().encodeToString(payload.update))
                put("context", configuration.contextEncoder(payload.context))
                payload.connection?.attempt?.request?.let { put("request", requestJson(it)) }
            },
        )
    }

    private suspend fun nodeChangeEnvelope(payload: ChangePayload<C>): ByteArray {
        val request = payload.connection?.attempt?.request
        return envelope(
            WebhookEvent.Change,
            buildJsonObject {
                put(
                    "document",
                    checkNotNull(configuration.transformer).fromDocument(payload.document),
                )
                put("documentName", payload.document.name)
                put("context", configuration.contextEncoder(payload.context))
                put(
                    "requestHeaders",
                    filteredValues(request?.headers.orEmpty(), configuration.forwardedHeaders),
                )
                put(
                    "requestParameters",
                    filteredValues(request?.parameters.orEmpty(), configuration.forwardedParameters),
                )
            },
        )
    }

    private fun requestJson(request: HocuspocusRequest): JsonObject = buildJsonObject {
        if (configuration.forwardRemoteAddress) {
            request.remoteAddress?.let { put("remoteAddress", it) }
        }
        put("headers", filteredValues(request.headers, configuration.forwardedHeaders))
        put("parameters", filteredValues(request.parameters, configuration.forwardedParameters))
    }

    private fun filteredValues(
        values: Map<String, List<String>>,
        allowList: Set<String>,
    ): JsonObject {
        val normalized = allowList.map(String::lowercase).toSet()
        return buildJsonObject {
            values.toSortedMap(String.CASE_INSENSITIVE_ORDER).forEach { (key, entries) ->
                if (key.lowercase() in normalized) {
                    put(
                        key,
                        buildJsonArray {
                            entries.forEach { entry -> add(JsonPrimitive(entry)) }
                        },
                    )
                }
            }
        }
    }

    private data class PendingChange(
        val startedNanos: Long,
        val body: ByteArray,
        val job: Job,
    )

    private companion object {
        private const val HMAC_ALGORITHM: String = "HmacSHA256"
        private const val SIGNATURE_HEADER: String = "X-Hocuspocus-Signature-256"
    }
}

private fun ByteArray.toHex(): String = joinToString(separator = "") { byte ->
    "%02x".format(byte.toInt() and 0xff)
}

private fun Long.toDurationNanos(): Duration = Duration.parse("${this}ns")
