package ai.hocuspocus.protocol

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

/** The largest integer JavaScript can represent without losing precision. */
public const val MAX_SAFE_INTEGER: Long = 9_007_199_254_740_991L

public class ProtocolException(
    message: String,
    cause: Throwable? = null,
) : IllegalArgumentException(message, cause)

/**
 * Bounds used while decoding untrusted WebSocket frames.
 *
 * The outer Ktor adapter should impose a frame limit as well. These limits are
 * intentionally repeated here so direct users of the protocol module cannot
 * accidentally allocate from an attacker-controlled length prefix.
 */
public data class DecodeLimits(
    val maxByteArraySize: Int = 5 * 1024 * 1024,
    val maxStringSize: Int = 1024 * 1024,
    val maxAwarenessEntries: Int = 1_024,
) {
    init {
        require(maxByteArraySize >= 0) { "maxByteArraySize must not be negative" }
        require(maxStringSize >= 0) { "maxStringSize must not be negative" }
        require(maxAwarenessEntries >= 0) { "maxAwarenessEntries must not be negative" }
    }
}

/** Minimal lib0-compatible writer used by Hocuspocus and y-protocols frames. */
public class Lib0Writer {
    private val output: ByteArrayOutputStream = ByteArrayOutputStream()

    public val size: Int
        get() = output.size()

    public fun writeByte(value: Int): Lib0Writer {
        require(value in 0..0xff) { "byte must be between 0 and 255" }
        output.write(value)
        return this
    }

    public fun writeVarUint(value: Long): Lib0Writer {
        require(value in 0..MAX_SAFE_INTEGER) {
            "varuint must be between 0 and JavaScript's MAX_SAFE_INTEGER"
        }
        var remaining = value
        while (remaining > 0x7f) {
            output.write(((remaining and 0x7f) or 0x80).toInt())
            remaining = remaining ushr 7
        }
        output.write(remaining.toInt())
        return this
    }

    public fun writeBytes(value: ByteArray): Lib0Writer {
        output.write(value)
        return this
    }

    public fun writeVarByteArray(value: ByteArray): Lib0Writer {
        writeVarUint(value.size.toLong())
        return writeBytes(value)
    }

    public fun writeVarString(value: String): Lib0Writer {
        val encoded = value.toByteArray(StandardCharsets.UTF_8)
        writeVarUint(encoded.size.toLong())
        return writeBytes(encoded)
    }

    public fun toByteArray(): ByteArray = output.toByteArray()
}

/** Minimal bounded lib0-compatible reader for untrusted frames. */
public class Lib0Reader(
    private val input: ByteArray,
    private val limits: DecodeLimits = DecodeLimits(),
) {
    public var position: Int = 0
        private set

    public val remaining: Int
        get() = input.size - position

    public val hasRemaining: Boolean
        get() = remaining > 0

    public fun readByte(): Int {
        if (position >= input.size) {
            throw ProtocolException("unexpected end of input")
        }
        return input[position++].toInt() and 0xff
    }

    public fun readVarUint(): Long {
        var result = 0L
        var shift = 0
        while (shift <= 49) {
            val current = readByte()
            result = result or ((current and 0x7f).toLong() shl shift)
            if ((current and 0x80) == 0) {
                if (result > MAX_SAFE_INTEGER) {
                    throw ProtocolException("varuint exceeds JavaScript's MAX_SAFE_INTEGER")
                }
                return result
            }
            shift += 7
        }
        throw ProtocolException("varuint is too long")
    }

    public fun readBytes(length: Int): ByteArray {
        if (length < 0 || length > remaining) {
            throw ProtocolException("requested $length bytes with only $remaining remaining")
        }
        val result = input.copyOfRange(position, position + length)
        position += length
        return result
    }

    public fun readVarByteArray(): ByteArray {
        val length = readBoundedLength(limits.maxByteArraySize, "byte array")
        return readBytes(length)
    }

    public fun peekVarByteArray(): ByteArray {
        val savedPosition = position
        return try {
            readVarByteArray()
        } finally {
            position = savedPosition
        }
    }

    public fun readVarString(): String {
        val length = readBoundedLength(limits.maxStringSize, "string")
        return String(readBytes(length), StandardCharsets.UTF_8)
    }

    public fun readRemainingBytes(): ByteArray {
        if (remaining > limits.maxByteArraySize) {
            throw ProtocolException(
                "remaining byte array length $remaining exceeds configured limit ${limits.maxByteArraySize}",
            )
        }
        return readBytes(remaining)
    }

    public fun requireFullyConsumed(context: String = "message") {
        if (hasRemaining) {
            throw ProtocolException("$context contains $remaining unexpected trailing bytes")
        }
    }

    private fun readBoundedLength(limit: Int, kind: String): Int {
        val length = readVarUint()
        if (length > limit.toLong()) {
            throw ProtocolException("$kind length $length exceeds configured limit $limit")
        }
        if (length > Int.MAX_VALUE) {
            throw ProtocolException("$kind length $length cannot be represented on the JVM")
        }
        return length.toInt()
    }
}
