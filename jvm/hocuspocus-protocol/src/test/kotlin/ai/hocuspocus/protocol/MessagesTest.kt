package ai.hocuspocus.protocol

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class MessagesTest {
    @Test
    fun `decodes a v4 provider authentication frame including provider version`() {
        val expected = byteArrayOf(
            0x03,
            'd'.code.toByte(),
            'o'.code.toByte(),
            'c'.code.toByte(),
            MessageType.Auth.wireValue.toByte(),
            AuthMessageType.Token.wireValue.toByte(),
            0x06,
            's'.code.toByte(),
            'e'.code.toByte(),
            'c'.code.toByte(),
            'r'.code.toByte(),
            'e'.code.toByte(),
            't'.code.toByte(),
            0x05,
            '4'.code.toByte(),
            '.'.code.toByte(),
            '4'.code.toByte(),
            '.'.code.toByte(),
            '0'.code.toByte(),
        )

        val frame = FrameCodec.decode(expected)
        assertEquals(RoutingKey("doc"), frame.routingKey)
        assertEquals(MessageType.Auth, frame.type)
        assertEquals(ClientAuthentication("secret", "4.4.0"), AuthenticationCodec.decodeClient(frame.payload))
        assertContentEquals(expected, FrameCodec.encode(frame.routingKey, frame.type, frame.payload))
    }

    @Test
    fun `preserves session-aware routing keys`() {
        val routingKey = RoutingKey("document", "provider-session")
        val encoded = FrameCodec.encode(routingKey, MessageType.QueryAwareness)
        assertEquals(routingKey, FrameCodec.decode(encoded).routingKey)
    }

    @Test
    fun `fused sync encoding is byte identical to composed encoding`() {
        val routingKeys = listOf(
            RoutingKey("document"),
            RoutingKey("문서", "provider-session"),
        )
        val payloads = listOf(
            ByteArray(0),
            ByteArray(127) { it.toByte() },
            ByteArray(128) { it.toByte() },
            ByteArray(64 * 1_024) { it.toByte() },
        )

        routingKeys.forEach { routingKey ->
            SyncMessageType.entries.forEach { syncType ->
                payloads.forEach { payload ->
                    val expected = FrameCodec.encode(
                        routingKey,
                        MessageType.Sync,
                        SyncCodec.encode(syncType, payload),
                    )
                    assertContentEquals(
                        expected,
                        FrameCodec.encodeSync(routingKey, syncType, payload),
                    )
                }
            }
        }
    }

    @Test
    fun `encodes server authentication responses`() {
        val encoded = AuthenticationCodec.encodeServer(
            ServerAuthentication.Authenticated(AuthorizedScope.ReadOnly),
        )
        val reader = Lib0Reader(encoded)
        assertEquals(AuthMessageType.Authenticated.wireValue, reader.readVarUint())
        assertEquals("readonly", reader.readVarString())
        reader.requireFullyConsumed()
    }

    @Test
    fun `round trips awareness entries`() {
        val entries = listOf(
            AwarenessEntry(
                clientId = 42,
                clock = 7,
                state = buildJsonObject {
                    put("name", "Ada")
                    put("cursor", 3)
                },
            ),
            AwarenessEntry(clientId = 43, clock = 2, state = null),
        )

        val decoded = AwarenessCodec.decode(AwarenessCodec.encode(entries))
        assertEquals(entries, decoded)
        assertIs<kotlinx.serialization.json.JsonObject>(decoded.first().state)
    }

    @Test
    fun `rejects awareness entry count before attacker controlled allocation`() {
        val encoded = Lib0Writer().writeVarUint(1_025).toByteArray()

        assertFailsWith<ProtocolException> {
            AwarenessCodec.decode(encoded, DecodeLimits(maxAwarenessEntries = 1_024))
        }
    }

    @Test
    fun `rejects awareness entry count impossible for remaining bytes`() {
        val encoded = Lib0Writer()
            .writeVarUint(1_000)
            .writeByte(0)
            .toByteArray()

        assertFailsWith<ProtocolException> {
            AwarenessCodec.decode(encoded, DecodeLimits(maxAwarenessEntries = 1_000))
        }
    }
}
