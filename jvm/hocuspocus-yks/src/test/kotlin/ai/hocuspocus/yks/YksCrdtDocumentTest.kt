package ai.hocuspocus.yks

import ai.hocuspocus.core.CrdtDocumentOptions
import ai.hocuspocus.core.CrdtStructKind
import ai.hocuspocus.core.TransactionOrigin
import dev.yks.GC
import dev.yks.Id
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
    fun `maps gc filter metadata without exposing YKS structs`() {
        val seen = mutableListOf<ai.hocuspocus.core.CrdtStructInfo>()
        val target = YksDocumentFactory().create(
            CrdtDocumentOptions(
                garbageCollectionFilter = { struct ->
                    seen += struct
                    false
                },
            ),
        )

        assertFalse(target.requireYDoc().gcFilter(GC(Id(7, 11), 3)))
        assertEquals(1, seen.size)
        assertEquals(7, seen.single().clientId)
        assertEquals(11, seen.single().clock)
        assertEquals(3, seen.single().length)
        assertEquals(CrdtStructKind.GarbageCollected, seen.single().kind)
    }

    @Test
    fun `reports named root emptiness like Hocuspocus document`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions())

        assertTrue(target.isFieldEmpty("body"))
        target.requireYDoc().getText("body").insert(0, "content")
        assertFalse(target.isFieldEmpty("body"))
        assertTrue(target.isFieldEmpty("metadata"))
        target.requireYDoc().getMap("metadata").set("ready", true)
        assertFalse(target.isFieldEmpty("metadata"))
    }

    @Test
    fun `reports structural emptiness for unopened and deleted remote roots`() {
        val source = YDoc(clientId = 41)
        val target = YksCrdtDocument(YDoc(clientId = 42))
        try {
            val body = source.getText("body")
            body.insert(0, "remote")
            target.applyUpdate(source.encodeStateAsUpdate())
            assertFalse(target.isFieldEmpty("body"))

            body.delete(0, body.length)
            target.applyUpdate(source.encodeStateAsUpdate(target.encodeStateVector()))
            assertFalse(target.isFieldEmpty("body"))
        } finally {
            source.destroy()
            target.close()
        }
    }

    @Test
    fun `rejects access after close`() {
        val target = YksCrdtDocument(YDoc())
        target.close()
        target.close()

        assertFailsWith<IllegalStateException> { target.encodeStateVector() }
    }

}
