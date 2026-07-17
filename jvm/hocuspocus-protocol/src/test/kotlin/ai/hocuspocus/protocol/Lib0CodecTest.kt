package ai.hocuspocus.protocol

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class Lib0CodecTest {
    @Test
    fun `bounds unprefixed remaining bytes before copying`() {
        val reader = Lib0Reader(
            ByteArray(3),
            DecodeLimits(maxByteArraySize = 2),
        )

        assertFailsWith<ProtocolException> {
            reader.readRemainingBytes()
        }
    }

    @Test
    fun `varuint matches lib0 byte layout`() {
        assertContentEquals(byteArrayOf(0), Lib0Writer().writeVarUint(0).toByteArray())
        assertContentEquals(byteArrayOf(0x7f), Lib0Writer().writeVarUint(127).toByteArray())
        assertContentEquals(byteArrayOf(0x80.toByte(), 0x01), Lib0Writer().writeVarUint(128).toByteArray())
        assertContentEquals(
            byteArrayOf(0x80.toByte(), 0x80.toByte(), 0x01),
            Lib0Writer().writeVarUint(16_384).toByteArray(),
        )
    }

    @Test
    fun `round trips JavaScript safe integers and unicode strings`() {
        val bytes = Lib0Writer()
            .writeVarUint(MAX_SAFE_INTEGER)
            .writeVarString("Ktor · 협업 · 😀")
            .toByteArray()
        val reader = Lib0Reader(bytes)

        assertEquals(MAX_SAFE_INTEGER, reader.readVarUint())
        assertEquals("Ktor · 협업 · 😀", reader.readVarString())
        reader.requireFullyConsumed()
    }

    @Test
    fun `rejects truncated and oversized values`() {
        assertFailsWith<ProtocolException> {
            Lib0Reader(byteArrayOf(0x80.toByte())).readVarUint()
        }
        assertFailsWith<ProtocolException> {
            Lib0Reader(byteArrayOf(0x02, 0x01), DecodeLimits(maxStringSize = 1)).readVarString()
        }
    }
}
