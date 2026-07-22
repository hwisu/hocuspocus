package ai.hocuspocus.core

import ai.hocuspocus.protocol.AwarenessCodec
import ai.hocuspocus.protocol.AwarenessEntry
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AwarenessStoreTest {
    private val state = buildJsonObject { put("name", "Ada") }

    @Test
    fun `applies only newer clocks and classifies changes like y-protocols`() {
        val store = AwarenessStore()

        assertEquals(
            AwarenessChange(added = listOf(7), updated = emptyList(), removed = emptyList()),
            store.apply(listOf(AwarenessEntry(7, 1, state))),
        )
        assertTrue(store.apply(listOf(AwarenessEntry(7, 1, state))).isEmpty)
        assertEquals(
            AwarenessChange(added = emptyList(), updated = listOf(7), removed = emptyList()),
            store.apply(listOf(AwarenessEntry(7, 2, state))),
        )
        assertEquals(
            AwarenessChange(added = emptyList(), updated = emptyList(), removed = listOf(7)),
            store.apply(listOf(AwarenessEntry(7, 2, null))),
        )
        assertFalse(store.contains(7))
    }

    @Test
    fun `encodes removals with retained metadata clock`() {
        val store = AwarenessStore()
        store.apply(listOf(AwarenessEntry(9, 12, state)))
        store.remove(listOf(9))

        assertEquals(listOf(AwarenessEntry(9, 12, null)), AwarenessCodec.decode(store.encode(listOf(9))))
    }

    @Test
    fun `identifies stale active clients`() {
        val store = AwarenessStore(
            Clock.fixed(Instant.ofEpochMilli(50_000), ZoneOffset.UTC),
        )
        store.apply(listOf(AwarenessEntry(1, 1, state)))

        assertEquals(listOf(1L), store.staleClientIds(timeoutMillis = 0))
        assertEquals(emptyList(), store.staleClientIds(timeoutMillis = 1))
    }

    @Test
    fun `retains unknown removal clocks and rejects stale resurrection like y-protocols`() {
        val store = AwarenessStore()

        assertTrue(store.apply(listOf(AwarenessEntry(999, 10, null))).isEmpty)
        assertEquals(setOf(999L), store.knownClientIds())
        assertEquals(10, store.currentClock(999))

        assertTrue(store.apply(listOf(AwarenessEntry(999, 9, state))).isEmpty)
        assertFalse(store.contains(999))
        assertEquals(
            AwarenessChange(added = emptyList(), updated = listOf(999), removed = emptyList()),
            store.apply(listOf(AwarenessEntry(999, 11, state))),
        )
        assertTrue(store.contains(999))
    }

    @Test
    fun `prunes inactive awareness metadata after retention`() {
        val store = AwarenessStore(
            Clock.fixed(Instant.ofEpochMilli(50_000), ZoneOffset.UTC),
        )
        store.apply(listOf(AwarenessEntry(7, 1, state)))
        store.remove(listOf(7))

        store.pruneInactiveMetadata(retentionMillis = 0)

        assertEquals(emptySet(), store.knownClientIds())
    }
}
