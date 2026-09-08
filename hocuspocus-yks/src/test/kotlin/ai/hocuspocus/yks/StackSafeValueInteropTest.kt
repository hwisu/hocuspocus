package ai.hocuspocus.yks

import ai.hocuspocus.protocol.Lib0Writer
import dev.yks.YDoc
import dev.yks.encodeStateAsUpdate
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

    @Test
    fun `adjacent deep formatting survives incoming updates and persistence`() {
        fun nested(): Any? {
            var value: Any? = "leaf"
            repeat(10_000) { value = mapOf("next" to value) }
            return value
        }
        val source = YDoc(clientId = 1)
        val original = YksCrdtDocument(source)
        val receiving = YksCrdtDocument(YDoc(clientId = 2))
        val restored = YksCrdtDocument(YDoc(clientId = 3))
        try {
            source.getText("body").insert(0, "xy")
            source.getText("body").format(0, 1, mapOf("deep" to nested()))
            receiving.applyUpdate(encodeStateAsUpdate(source))
            source.getText("body").format(1, 1, mapOf("deep" to nested()))
            val update = encodeStateAsUpdate(source, receiving.encodeStateVector())
            receiving.applyUpdate(update)
            restored.applyUpdate(receiving.encodeStateAsUpdate())
            assertTrue(restored.containsUpdate(update))
        } finally {
            original.close()
            receiving.close()
            restored.close()
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
