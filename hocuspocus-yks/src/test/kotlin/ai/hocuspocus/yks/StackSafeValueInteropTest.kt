package ai.hocuspocus.yks

import ai.hocuspocus.protocol.Lib0Writer
import dev.yks.YDoc
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StackSafeValueInteropTest {
    @Test
    fun `deep standard values survive update persistence and reload`() {
        val update = nestedUpdate()
        val original = YksCrdtDocument(YDoc(clientId = 2))
        val restored = YksCrdtDocument(YDoc(clientId = 3))
        try {
            original.applyUpdate(update)
            assertTrue(original.containsUpdate(update))
            restored.applyUpdate(original.encodeStateAsUpdate())
            assertTrue(restored.containsUpdate(update))
        } finally {
            original.close()
            restored.close()
        }
    }

    @Test
    fun `read only inspection handles deep values without changing the document`() {
        val document = YksCrdtDocument(YDoc(clientId = 2))
        try {
            assertFalse(document.containsUpdate(nestedUpdate()))
            assertTrue(document.isFieldEmpty("values"))
        } finally {
            document.close()
        }
    }

    private fun nestedUpdate(): ByteArray {
        val writer = Lib0Writer()
            .writeVarUint(1).writeVarUint(1).writeVarUint(1).writeVarUint(0)
            .writeByte(8).writeVarUint(1).writeVarString("values").writeVarUint(1)
        repeat(10_000) { writer.writeByte(117).writeVarUint(1) }
        return writer.writeByte(126).writeVarUint(0).toByteArray()
    }
}
