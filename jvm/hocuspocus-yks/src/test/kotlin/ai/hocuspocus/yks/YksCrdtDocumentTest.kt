package ai.hocuspocus.yks

import ai.hocuspocus.core.CrdtDocumentOptions
import ai.hocuspocus.core.TransactionOrigin
import dev.yks.YDoc
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class YksCrdtDocumentTest {
    @Test
    fun `applies and emits genuine standard V1 updates`() {
        val source = YDoc(clientId = 1, gc = false)
        source.getText("body").insert(0, "hello 😀")
        val update = source.encodeStateAsUpdate()
        val origin = TransactionOrigin.Connection("socket-1", "doc")
        val target = YksCrdtDocument(YDoc(clientId = 2, gc = false))

        val emitted = target.applyUpdate(update, origin)

        assertEquals(1, emitted.size)
        assertEquals(origin, emitted.single().origin)
        assertEquals("hello 😀", target.document.getText("body").toString())
        assertContentEquals(source.encodeStateVector(), target.encodeStateVector())
        assertTrue(target.containsUpdate(update))

        source.getText("body").insert(source.getText("body").length, "!")
        assertFalse(target.containsUpdate(source.encodeStateAsUpdate(target.encodeStateVector())))
    }

    @Test
    fun `captures local transaction update through a typed native document`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions(garbageCollection = false))

        val updates = target.transact(YDoc::class, TransactionOrigin.Local(context = "test")) { native ->
            native.getMap("root").set("ready", true)
        }

        assertEquals(1, updates.size)
        assertEquals(true, target.requireYDoc().getMap("root").get("ready"))
    }

    @Test
    fun `rejects access after close`() {
        val target = YksCrdtDocument(YDoc())
        target.close()
        target.close()

        assertFailsWith<IllegalStateException> { target.encodeStateVector() }
    }

}
