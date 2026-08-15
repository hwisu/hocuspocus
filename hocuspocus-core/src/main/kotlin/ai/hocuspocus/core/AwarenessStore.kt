package ai.hocuspocus.core

import ai.hocuspocus.protocol.AwarenessCodec
import ai.hocuspocus.protocol.AwarenessEntry
import kotlinx.serialization.json.JsonElement
import java.time.Clock

public data class AwarenessChange(
    val added: List<Long>,
    val updated: List<Long>,
    val removed: List<Long>,
) {
    public val changedClients: List<Long>
        get() = added + updated + removed

    public val isEmpty: Boolean
        get() = added.isEmpty() && updated.isEmpty() && removed.isEmpty()
}

private data class AwarenessMeta(
    val clock: Long,
    val lastUpdatedMillis: Long,
)

/** Server-side implementation of the y-protocols awareness clock semantics. */
public class AwarenessStore(
    private val clock: Clock = Clock.systemUTC(),
) {
    private val states: MutableMap<Long, JsonElement> = linkedMapOf()
    private val metadata: MutableMap<Long, AwarenessMeta> = linkedMapOf()

    @Synchronized
    public fun states(): Map<Long, JsonElement> = states.toMap()

    @Synchronized
    public fun contains(clientId: Long): Boolean = states.containsKey(clientId)

    @Synchronized
    public fun currentClock(clientId: Long): Long? = metadata[clientId]?.clock

    @Synchronized
    public fun lastUpdatedMillis(clientId: Long): Long? = metadata[clientId]?.lastUpdatedMillis

    @Synchronized
    internal fun knownClientIds(): Set<Long> = metadata.keys.toSet()

    @Synchronized
    internal fun projectedKnownClientIds(entries: Collection<AwarenessEntry>): Set<Long> {
        val projected = metadata.keys.toMutableSet()
        entries.forEach { entry ->
            if (shouldApply(entry)) projected += entry.clientId
        }
        return projected
    }

    @Synchronized
    internal fun projectedActiveClientIds(entries: Collection<AwarenessEntry>): Set<Long> {
        val projected = states.keys.toMutableSet()
        entries.forEach { entry ->
            if (!shouldApply(entry)) return@forEach
            if (entry.state == null) projected.remove(entry.clientId) else projected.add(entry.clientId)
        }
        return projected
    }

    @Synchronized
    public fun apply(entries: Collection<AwarenessEntry>): AwarenessChange {
        val now = clock.millis()
        val added = mutableListOf<Long>()
        val updated = mutableListOf<Long>()
        val removed = mutableListOf<Long>()

        entries.forEach { entry ->
            if (!shouldApply(entry)) return@forEach
            val nextState = entry.state
            val currentMeta = metadata[entry.clientId]

            if (nextState == null) {
                states.remove(entry.clientId)
            } else {
                states[entry.clientId] = nextState
            }
            metadata[entry.clientId] = AwarenessMeta(entry.clock, now)

            when {
                currentMeta == null && nextState != null -> added += entry.clientId
                currentMeta != null && nextState == null -> removed += entry.clientId
                nextState != null -> updated += entry.clientId
            }
        }

        return AwarenessChange(added, updated, removed)
    }

    /**
     * Removes connection-owned states using their current clocks, matching
     * y-protocols `removeAwarenessStates` for remote clients.
     */
    @Synchronized
    public fun remove(clientIds: Collection<Long>): AwarenessChange {
        val removed = clientIds.filter { states.remove(it) != null }
        return AwarenessChange(emptyList(), emptyList(), removed)
    }

    @Synchronized
    public fun entries(clientIds: Collection<Long> = states.keys): List<AwarenessEntry> =
        clientIds.mapNotNull { clientId ->
            val meta = metadata[clientId] ?: return@mapNotNull null
            AwarenessEntry(clientId, meta.clock, states[clientId])
        }

    @Synchronized
    public fun encode(clientIds: Collection<Long> = states.keys): ByteArray =
        AwarenessCodec.encode(entries(clientIds))

    @Synchronized
    public fun staleClientIds(timeoutMillis: Long): List<Long> {
        require(timeoutMillis >= 0) { "timeoutMillis must not be negative" }
        val cutoff = clock.millis() - timeoutMillis
        return states.keys.filter { clientId ->
            val updated = metadata[clientId]?.lastUpdatedMillis ?: Long.MIN_VALUE
            updated <= cutoff
        }
    }

    /** Bounds inactive clock metadata after enough time has passed for stale presence to expire. */
    @Synchronized
    internal fun pruneInactiveMetadata(retentionMillis: Long) {
        require(retentionMillis >= 0) { "retentionMillis must not be negative" }
        val cutoff = clock.millis() - retentionMillis
        metadata.entries.removeIf { (clientId, meta) ->
            clientId !in states && meta.lastUpdatedMillis <= cutoff
        }
    }

    /**
     * The y-protocols clock rule shared by [apply] and the projected-limit
     * queries, so enforcement can never disagree with what [apply] does.
     */
    private fun shouldApply(entry: AwarenessEntry): Boolean {
        val currentMeta = metadata[entry.clientId]
        val currentClock = currentMeta?.clock ?: 0
        val hasCurrentState = states.containsKey(entry.clientId)
        return currentClock < entry.clock ||
            (currentClock == entry.clock && entry.state == null && hasCurrentState)
    }
}
