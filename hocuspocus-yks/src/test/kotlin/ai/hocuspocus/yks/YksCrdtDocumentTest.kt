package ai.hocuspocus.yks

import ai.hocuspocus.core.CrdtDocumentOptions
import ai.hocuspocus.core.CrdtMutationException
import ai.hocuspocus.core.CrdtStructKind
import ai.hocuspocus.core.TransactionOrigin
import dev.yks.GC
import dev.yks.Id
import dev.yks.YDoc
import dev.yks.UnsupportedYjsStandardUpdateException
import dev.yks.YXmlElement
import dev.yks.YXmlElementType
import dev.yks.YXmlTextType
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class YksCrdtDocumentTest {
    @Test
    fun `failed local mutation preserves committed updates and resets capture`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions())
        val replica = YDoc(clientId = 2)
        val origin = TransactionOrigin.Local("actor")
        val expected = IllegalArgumentException("callback failed")
        try {
            val failure = assertFailsWith<CrdtMutationException> {
                target.transact(YDoc::class, origin) { native ->
                    native.getText("9253").insert(0, "committed")
                    throw expected
                }
            }
            assertSame(expected, failure.cause)
            val committed = failure.committedUpdates.single()
            assertSame(origin, committed.origin)
            assertEquals(setOf("9253"), committed.changedRootNames)
            replica.applyUpdate(committed.data)
            assertEquals("committed", replica.getText("9253").toString())
            assertContentEquals(target.encodeStateVector(), replica.encodeStateVector())

            val next = target.transact(YDoc::class, origin) { native ->
                native.getText("9253").insert(9, " next")
            }
            replica.applyUpdate(next.single().data)
            assertEquals("committed next", replica.getText("9253").toString())
        } finally {
            target.close()
            replica.destroy()
        }
    }

    @Test
    fun `failed remote observer preserves the emitted update and changed roots`() {
        val source = YDoc(clientId = 1)
        val target = YksDocumentFactory().create(CrdtDocumentOptions())
        val replica = YDoc(clientId = 3)
        val expected = IllegalStateException("observer failed")
        val origin = TransactionOrigin.Connection("socket", "document")
        try {
            source.getText("9253").insert(0, "remote")
            target.requireYDoc().getText("9253").observe { throw expected }
            val failure = assertFailsWith<CrdtMutationException> {
                target.applyUpdate(source.encodeStateAsUpdate(), origin)
            }
            assertSame(expected, failure.cause)
            val committed = failure.committedUpdates.single()
            assertSame(origin, committed.origin)
            assertEquals(setOf("9253"), committed.changedRootNames)
            replica.applyUpdate(committed.data)
            assertEquals("remote", replica.getText("9253").toString())
            assertContentEquals(target.encodeStateVector(), replica.encodeStateVector())
            assertTrue(target.applyUpdate(source.encodeStateAsUpdate(), origin).isEmpty())
        } finally {
            source.destroy()
            target.close()
            replica.destroy()
        }
    }

    @Test
    fun `failure before mutation keeps the original exception and no changes`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions())
        val expected = IllegalArgumentException("rejected before editing")
        try {
            val before = target.encodeStateAsUpdate()
            assertSame(expected, assertFailsWith<IllegalArgumentException> {
                target.transact(YDoc::class, null) { throw expected }
            })
            assertContentEquals(before, target.encodeStateAsUpdate())
        } finally {
            target.close()
        }
    }

    @Test
    fun `failed delete only mutation retains its delta despite an unchanged state vector`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions())
        val replica = YDoc(clientId = 3)
        val expected = IllegalStateException("failed after deleting")
        try {
            target.transact(YDoc::class, null) { it.getText("body").insert(0, "remove") }
                .forEach { replica.applyUpdate(it.data) }
            val before = target.encodeStateVector()
            val failure = assertFailsWith<CrdtMutationException> {
                target.transact(YDoc::class, null) {
                    it.getText("body").delete(0, 6)
                    throw expected
                }
            }
            assertSame(expected, failure.cause)
            assertContentEquals(before, target.encodeStateVector())
            replica.applyUpdate(failure.committedUpdates.single().data)
            assertEquals("", replica.getText("body").toString())
            assertContentEquals(target.encodeStateAsUpdate(), replica.encodeStateAsUpdate())
        } finally {
            target.close()
            replica.destroy()
        }
    }

    @Test
    fun `standard wire rejection still rolls back without a committed failure`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions())
        try {
            val before = target.encodeStateAsUpdate()
            assertFailsWith<UnsupportedYjsStandardUpdateException> {
                target.transact(YDoc::class, null) { native ->
                    native.getText("body").insert(0, "must rollback")
                    native.getXmlFragment("xml").push(YXmlElement("p"))
                }
            }
            assertContentEquals(before, target.encodeStateAsUpdate())
        } finally {
            target.close()
        }
    }

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
    fun `reports the numeric root for a nested xml edit`() {
        val target = YksDocumentFactory().create(CrdtDocumentOptions(garbageCollection = false))
        target.transact(YDoc::class, TransactionOrigin.Local(context = "setup")) { native ->
            native.getXmlFragment("9253").push(
                YXmlElementType("paragraph").also { paragraph ->
                    paragraph.push(YXmlTextType().also { text -> text.insert(0, "before") })
                },
            )
        }

        val updates = target.transact(YDoc::class, TransactionOrigin.Local(context = "edit")) { native ->
            val paragraph = native.getXmlFragment("9253").getType(0) as YXmlElementType
            val text = paragraph.getType(0) as YXmlTextType
            text.insert(text.length, " after")
        }

        assertEquals(setOf("9253"), updates.single().changedRootNames)
        target.close()
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
