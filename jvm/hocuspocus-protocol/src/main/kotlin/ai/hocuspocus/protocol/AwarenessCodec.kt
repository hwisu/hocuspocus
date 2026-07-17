package ai.hocuspocus.protocol

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull

public data class AwarenessEntry(
    val clientId: Long,
    val clock: Long,
    val state: JsonElement?,
) {
    init {
        require(clientId in 0..MAX_SAFE_INTEGER) { "clientId is outside the JavaScript safe integer range" }
        require(clock in 0..MAX_SAFE_INTEGER) { "clock is outside the JavaScript safe integer range" }
    }
}

public object AwarenessCodec {
    private val json: Json = Json {
        isLenient = false
        ignoreUnknownKeys = false
    }

    public fun decode(update: ByteArray, limits: DecodeLimits = DecodeLimits()): List<AwarenessEntry> {
        val reader = Lib0Reader(update, limits)
        val count = reader.readVarUint()
        if (count > limits.maxAwarenessEntries.toLong()) {
            throw ProtocolException(
                "awareness entry count $count exceeds configured limit ${limits.maxAwarenessEntries}",
            )
        }
        // Every entry needs at least a client id, clock, and string-length varuint.
        // Validate this before allocating the attacker-controlled collection.
        if (count * MIN_ENCODED_ENTRY_SIZE > reader.remaining.toLong()) {
            throw ProtocolException("awareness entry count $count is impossible for ${reader.remaining} bytes")
        }
        val entries = ArrayList<AwarenessEntry>(count.toInt())
        repeat(count.toInt()) {
            val clientId = reader.readVarUint()
            val clock = reader.readVarUint()
            val encodedState = reader.readVarString()
            val parsed = try {
                json.parseToJsonElement(encodedState)
            } catch (error: IllegalArgumentException) {
                throw ProtocolException("invalid awareness JSON for client $clientId", error)
            }
            entries += AwarenessEntry(clientId, clock, parsed.takeUnless { it is JsonNull })
        }
        reader.requireFullyConsumed("awareness update")
        return entries
    }

    public fun encode(entries: Collection<AwarenessEntry>): ByteArray {
        val writer = Lib0Writer().writeVarUint(entries.size.toLong())
        entries.forEach { entry ->
            writer
                .writeVarUint(entry.clientId)
                .writeVarUint(entry.clock)
                .writeVarString((entry.state ?: JsonNull).toString())
        }
        return writer.toByteArray()
    }

    private const val MIN_ENCODED_ENTRY_SIZE: Long = 3
}
