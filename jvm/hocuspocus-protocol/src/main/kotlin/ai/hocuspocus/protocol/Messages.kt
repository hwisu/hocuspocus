package ai.hocuspocus.protocol

import java.nio.charset.StandardCharsets

public enum class MessageType(public val wireValue: Long) {
    Sync(0),
    Awareness(1),
    Auth(2),
    QueryAwareness(3),
    SyncReply(4),
    Stateless(5),
    BroadcastStateless(6),
    Close(7),
    SyncStatus(8),
    Ping(9),
    Pong(10),
    ;

    public companion object {
        public fun fromWireValue(value: Long): MessageType? = entries.firstOrNull { it.wireValue == value }
    }
}

public enum class AuthMessageType(public val wireValue: Long) {
    Token(0),
    PermissionDenied(1),
    Authenticated(2),
    ;

    public companion object {
        public fun fromWireValue(value: Long): AuthMessageType? = entries.firstOrNull { it.wireValue == value }
    }
}

public enum class SyncMessageType(public val wireValue: Long) {
    StepOne(0),
    StepTwo(1),
    Update(2),
    ;

    public companion object {
        public fun fromWireValue(value: Long): SyncMessageType? = entries.firstOrNull { it.wireValue == value }
    }
}

public enum class AuthorizedScope(public val wireValue: String) {
    ReadOnly("readonly"),
    ReadWrite("read-write"),
}

public data class RoutingKey(
    val documentName: String,
    val sessionId: String? = null,
) {
    init {
        require(documentName.isNotBlank()) { "documentName must not be blank" }
        require('\u0000' !in documentName) { "documentName must not contain a NUL separator" }
        require(sessionId == null || '\u0000' !in sessionId) { "sessionId must not contain a NUL separator" }
    }

    public fun encode(): String = sessionId?.let { "$documentName\u0000$it" } ?: documentName

    public companion object {
        public fun parse(raw: String): RoutingKey {
            val separator = raw.indexOf('\u0000')
            return if (separator < 0) {
                RoutingKey(raw)
            } else {
                RoutingKey(raw.substring(0, separator), raw.substring(separator + 1))
            }
        }
    }
}

public data class HocuspocusFrame(
    val routingKey: RoutingKey,
    val type: MessageType,
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean =
        other is HocuspocusFrame &&
            routingKey == other.routingKey &&
            type == other.type &&
            payload.contentEquals(other.payload)

    override fun hashCode(): Int = 31 * (31 * routingKey.hashCode() + type.hashCode()) + payload.contentHashCode()
}

/**
 * A bounded zero-copy view over a decoded Hocuspocus frame.
 *
 * The input array must remain immutable while the view or a reader returned by
 * [payloadReader] is in use. Transport adapters that own their receive arrays
 * can use this path to avoid copying the outer payload before a message-specific
 * codec reads it.
 */
public class HocuspocusFrameView internal constructor(
    public val routingKey: RoutingKey,
    public val rawRoutingKey: String,
    public val type: MessageType,
    private val input: ByteArray,
    private val payloadOffset: Int,
    public val payloadSize: Int,
) {
    public fun payloadReader(limits: DecodeLimits = DecodeLimits()): Lib0Reader =
        Lib0Reader(input, limits, payloadOffset, payloadSize)

    public fun copyPayload(): ByteArray = payloadReader(
        DecodeLimits(maxByteArraySize = payloadSize),
    ).readRemainingBytes()

    public fun toFrame(): HocuspocusFrame = HocuspocusFrame(routingKey, type, copyPayload())
}

public object FrameCodec {
    public fun decode(bytes: ByteArray, limits: DecodeLimits = DecodeLimits()): HocuspocusFrame =
        decodeView(bytes, limits).toFrame()

    public fun decodeView(
        bytes: ByteArray,
        limits: DecodeLimits = DecodeLimits(),
    ): HocuspocusFrameView {
        val reader = Lib0Reader(bytes, limits)
        val rawRoutingKey = reader.readVarString()
        val typeValue = reader.readVarUint()
        val type = MessageType.fromWireValue(typeValue)
            ?: throw ProtocolException("unknown Hocuspocus message type $typeValue")
        if (reader.remaining > limits.maxByteArraySize) {
            throw ProtocolException(
                "remaining byte array length ${reader.remaining} exceeds configured limit " +
                    "${limits.maxByteArraySize}",
            )
        }
        return HocuspocusFrameView(
            routingKey = try {
                RoutingKey.parse(rawRoutingKey)
            } catch (error: IllegalArgumentException) {
                throw ProtocolException("invalid routing key", error)
            },
            rawRoutingKey = rawRoutingKey,
            type = type,
            input = bytes,
            payloadOffset = reader.position,
            payloadSize = reader.remaining,
        )
    }

    public fun encode(
        routingKey: RoutingKey,
        type: MessageType,
        payload: ByteArray = ByteArray(0),
    ): ByteArray = Lib0Writer()
        .writeVarString(routingKey.encode())
        .writeVarUint(type.wireValue)
        .writeBytes(payload)
        .toByteArray()

    /**
     * Encodes a complete Hocuspocus sync frame into one exact-size array.
     * This avoids the intermediate sync payload used by [SyncCodec.encode].
     */
    public fun encodeSync(
        routingKey: RoutingKey,
        syncType: SyncMessageType,
        updateOrStateVector: ByteArray,
    ): ByteArray {
        val encodedRoutingKey = routingKey.encode().toByteArray(StandardCharsets.UTF_8)
        val size = encodedSize(
            encodedRoutingKey.size,
            MessageType.Sync.wireValue,
            syncType.wireValue,
            updateOrStateVector.size,
        )
        val output = ByteArray(size)
        var offset = 0
        offset = writeVarUint(output, offset, encodedRoutingKey.size.toLong())
        encodedRoutingKey.copyInto(output, offset)
        offset += encodedRoutingKey.size
        offset = writeVarUint(output, offset, MessageType.Sync.wireValue)
        offset = writeVarUint(output, offset, syncType.wireValue)
        offset = writeVarUint(output, offset, updateOrStateVector.size.toLong())
        updateOrStateVector.copyInto(output, offset)
        return output
    }

    private fun encodedSize(
        routingKeySize: Int,
        messageType: Long,
        syncType: Long,
        payloadSize: Int,
    ): Int {
        val total = varUintSize(routingKeySize.toLong()).toLong() +
            routingKeySize +
            varUintSize(messageType) +
            varUintSize(syncType) +
            varUintSize(payloadSize.toLong()) +
            payloadSize
        require(total <= Int.MAX_VALUE) { "encoded frame exceeds the JVM array size limit" }
        return total.toInt()
    }

    private fun varUintSize(value: Long): Int {
        require(value in 0..MAX_SAFE_INTEGER)
        var remaining = value
        var size = 1
        while (remaining > 0x7f) {
            size += 1
            remaining = remaining ushr 7
        }
        return size
    }

    private fun writeVarUint(
        output: ByteArray,
        startOffset: Int,
        value: Long,
    ): Int {
        var offset = startOffset
        var remaining = value
        while (remaining > 0x7f) {
            output[offset++] = ((remaining and 0x7f) or 0x80).toByte()
            remaining = remaining ushr 7
        }
        output[offset++] = remaining.toByte()
        return offset
    }
}

public data class ClientAuthentication(
    val token: String,
    val providerVersion: String?,
)

public sealed interface ServerAuthentication {
    public data class PermissionDenied(val reason: String) : ServerAuthentication

    public data class Authenticated(val scope: AuthorizedScope) : ServerAuthentication

    public data object TokenRequest : ServerAuthentication
}

public object AuthenticationCodec {
    public fun encodeClient(message: ClientAuthentication): ByteArray = Lib0Writer()
        .writeVarUint(AuthMessageType.Token.wireValue)
        .writeVarString(message.token)
        .apply { message.providerVersion?.let(::writeVarString) }
        .toByteArray()

    public fun decodeClient(payload: ByteArray, limits: DecodeLimits = DecodeLimits()): ClientAuthentication {
        return decodeClient(Lib0Reader(payload, limits))
    }

    public fun decodeClient(reader: Lib0Reader): ClientAuthentication {
        val subtype = AuthMessageType.fromWireValue(reader.readVarUint())
            ?: throw ProtocolException("unknown authentication message type")
        if (subtype != AuthMessageType.Token) {
            throw ProtocolException("client authentication must use the token subtype")
        }
        val token = reader.readVarString()
        val providerVersion = if (reader.hasRemaining) reader.readVarString() else null
        reader.requireFullyConsumed("authentication message")
        return ClientAuthentication(token, providerVersion)
    }

    public fun encodeServer(message: ServerAuthentication): ByteArray {
        val writer = Lib0Writer()
        when (message) {
            is ServerAuthentication.PermissionDenied -> writer
                .writeVarUint(AuthMessageType.PermissionDenied.wireValue)
                .writeVarString(message.reason)
            is ServerAuthentication.Authenticated -> writer
                .writeVarUint(AuthMessageType.Authenticated.wireValue)
                .writeVarString(message.scope.wireValue)
            ServerAuthentication.TokenRequest -> writer
                .writeVarUint(AuthMessageType.Token.wireValue)
        }
        return writer.toByteArray()
    }
}

public data class SyncMessage(
    val type: SyncMessageType,
    val updateOrStateVector: ByteArray,
) {
    override fun equals(other: Any?): Boolean =
        other is SyncMessage && type == other.type && updateOrStateVector.contentEquals(other.updateOrStateVector)

    override fun hashCode(): Int = 31 * type.hashCode() + updateOrStateVector.contentHashCode()
}

public object SyncCodec {
    public fun decode(payload: ByteArray, limits: DecodeLimits = DecodeLimits()): SyncMessage {
        return decode(Lib0Reader(payload, limits))
    }

    public fun decode(reader: Lib0Reader): SyncMessage {
        val typeValue = reader.readVarUint()
        val type = SyncMessageType.fromWireValue(typeValue)
            ?: throw ProtocolException("unknown Yjs sync message type $typeValue")
        val updateOrStateVector = reader.readVarByteArray()
        reader.requireFullyConsumed("sync message")
        return SyncMessage(type, updateOrStateVector)
    }

    public fun encode(type: SyncMessageType, updateOrStateVector: ByteArray): ByteArray = Lib0Writer()
        .writeVarUint(type.wireValue)
        .writeVarByteArray(updateOrStateVector)
        .toByteArray()
}
