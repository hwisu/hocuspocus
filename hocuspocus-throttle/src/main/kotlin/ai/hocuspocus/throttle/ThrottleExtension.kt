package ai.hocuspocus.throttle

import ai.hocuspocus.core.CloseEvents
import ai.hocuspocus.core.ConfigurePayload
import ai.hocuspocus.core.ConnectionAttempt
import ai.hocuspocus.core.HocuspocusAuthenticationException
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusRequest
import ai.hocuspocus.core.HocuspocusServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.ArrayDeque
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

public enum class UnknownAddressPolicy {
    Reject,
    SharedBucket,
    Allow,
}

public data class ThrottleConfiguration(
    val attempts: Int = 15,
    val window: Duration = 60.seconds,
    val banDuration: Duration = 5.minutes,
    val cleanupInterval: Duration = 90.seconds,
    val maxTrackedAddresses: Int = 100_000,
    val maxAddressLength: Int = 256,
    val unknownAddressPolicy: UnknownAddressPolicy = UnknownAddressPolicy.Reject,
    val addressResolver: (HocuspocusRequest) -> String? = HocuspocusRequest::remoteAddress,
) {
    init {
        require(attempts > 0) { "attempts must be positive" }
        require(window.isPositive() && window.isFinite()) { "window must be positive and finite" }
        require(banDuration.isPositive() && banDuration.isFinite()) {
            "banDuration must be positive and finite"
        }
        require(cleanupInterval.isPositive() && cleanupInterval.isFinite()) {
            "cleanupInterval must be positive and finite"
        }
        require(maxTrackedAddresses > 0) { "maxTrackedAddresses must be positive" }
        require(maxAddressLength > 0) { "maxAddressLength must be positive" }
    }
}

/**
 * Per-process sliding-window connection limiter.
 *
 * The default resolver trusts the socket address supplied by Ktor, not
 * spoofable forwarding headers. Deployments behind a trusted proxy can inject
 * an explicit resolver after normalizing their proxy chain.
 */
public class ThrottleExtension<C : Any>(
    public val configuration: ThrottleConfiguration = ThrottleConfiguration(),
    private val clockMillis: () -> Long = System::currentTimeMillis,
) : HocuspocusExtension<C> {
    override val priority: Int = 10_000
    override val name: String = "throttle"

    private val monitor: Any = Any()
    private val attemptsByAddress: MutableMap<String, ArrayDeque<Long>> = linkedMapOf()
    private val bannedUntil: MutableMap<String, Long> = linkedMapOf()
    private var scope: CoroutineScope? = null
    private var cleanupJob: Job? = null

    override suspend fun onConfigure(payload: ConfigurePayload<C>) {
        if (scope != null) return
        scope = CoroutineScope(SupervisorJob() + Dispatchers.Default).also { extensionScope ->
            cleanupJob = extensionScope.launch {
                while (isActive) {
                    delay(configuration.cleanupInterval)
                    clearExpired()
                }
            }
        }
    }

    override suspend fun onConnect(payload: ConnectionAttempt<C>) {
        val address = resolveAddress(payload.request) ?: return
        if (isLimited(address)) {
            throw HocuspocusAuthenticationException(
                CloseEvents.Forbidden,
                "connection rate limit exceeded",
            )
        }
    }

    override suspend fun onDestroy(server: HocuspocusServer<C>) {
        cleanupJob?.cancel()
        scope?.cancel()
        cleanupJob = null
        scope = null
        synchronized(monitor) {
            attemptsByAddress.clear()
            bannedUntil.clear()
        }
    }

    public fun clearExpired() {
        val now = clockMillis()
        synchronized(monitor) {
            attemptsByAddress.entries.removeIf { entry ->
                prune(entry.value, now)
                entry.value.isEmpty() && (bannedUntil[entry.key] ?: Long.MIN_VALUE) <= now
            }
            bannedUntil.entries.removeIf { (_, until) -> until <= now }
        }
    }

    public fun trackedAddresses(): Int = synchronized(monitor) {
        attemptsByAddress.size
    }

    private fun resolveAddress(request: HocuspocusRequest): String? {
        val resolved = configuration.addressResolver(request)?.trim()?.takeIf(String::isNotEmpty)
        if (resolved != null) {
            if (resolved.length > configuration.maxAddressLength) {
                throw HocuspocusAuthenticationException(
                    CloseEvents.Forbidden,
                    "client address exceeds configured length",
                )
            }
            return resolved
        }
        return when (configuration.unknownAddressPolicy) {
            UnknownAddressPolicy.Reject -> throw HocuspocusAuthenticationException(
                CloseEvents.Forbidden,
                "client address is unavailable",
            )
            UnknownAddressPolicy.SharedBucket -> UNKNOWN_ADDRESS
            UnknownAddressPolicy.Allow -> null
        }
    }

    private fun isLimited(address: String): Boolean {
        val now = clockMillis()
        synchronized(monitor) {
            bannedUntil[address]?.let { until ->
                if (until > now) return true
                bannedUntil.remove(address)
            }
            val attempts = attemptsByAddress[address] ?: run {
                if (attemptsByAddress.size >= configuration.maxTrackedAddresses) {
                    return true
                }
                ArrayDeque<Long>().also { attemptsByAddress[address] = it }
            }
            prune(attempts, now)
            attempts.addLast(now)
            if (attempts.size > configuration.attempts) {
                bannedUntil[address] = saturatedAdd(now, configuration.banDuration.inWholeMilliseconds)
                return true
            }
            return false
        }
    }

    private fun prune(attempts: ArrayDeque<Long>, now: Long) {
        val oldestAllowed = now - configuration.window.inWholeMilliseconds
        while (attempts.firstOrNull()?.let { it <= oldestAllowed } == true) {
            attempts.removeFirst()
        }
    }

    private fun saturatedAdd(left: Long, right: Long): Long =
        if (Long.MAX_VALUE - left < right) Long.MAX_VALUE else left + right

    private companion object {
        private const val UNKNOWN_ADDRESS: String = "<unknown>"
    }
}
