package ai.hocuspocus.redis

import ai.hocuspocus.core.DatabaseExtension
import ai.hocuspocus.core.DocumentStorage
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.yks.YksDocumentFactory
import ai.hocuspocus.yks.transactYks
import dev.yks.YDoc
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RedisExtensionTest {
    @Test
    fun `rejects identifiers that cannot interoperate with the upstream Redis envelope`() {
        assertFailsWith<IllegalArgumentException> {
            RedisExtensionConfiguration(identifier = "x".repeat(128))
        }
    }

    @Test
    fun `shutdown before start does not create or close a Redis bus`() = runBlocking {
        var creates = 0
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(
                    RedisExtension<Unit>(
                        RedisBusFactory {
                            creates += 1
                            InMemoryRedisBroker().newBus()
                        },
                    ),
                ),
            ),
        )

        server.shutdown()

        assertEquals(0, creates)
    }

    @Test
    fun `initial sync and live updates cross server boundaries`() = runBlocking {
        val broker = InMemoryRedisBroker()
        verifyMultiNodeSync(RedisBusFactory(broker::newBus))
    }

    @Test
    fun `conflated rapid updates converge without dropping the final state`() = runBlocking {
        val broker = InMemoryRedisBroker()
        val busFactory = RedisBusFactory(broker::newBus)
        val prefix = "test-${UUID.randomUUID()}"
        val first = newServer(busFactory, prefix, "first")
        val second = newServer(busFactory, prefix, "second")
        val firstConnection = first.openDirectConnection("burst", Unit)
        val secondConnection = second.openDirectConnection("burst", Unit)

        repeat(500) {
            firstConnection.transactYks { document ->
                val text = document.getText("body")
                text.insert(text.length, "x")
            }
        }
        eventually(5.seconds) {
            textValue(secondConnection.document.encodeStateAsUpdate()) == "x".repeat(500)
        }

        firstConnection.disconnect()
        secondConnection.disconnect()
        first.shutdown()
        second.shutdown()
    }

    @Test
    fun `failed change publication blocks unload and retries without losing state`() = runBlocking {
        val broker = InMemoryRedisBroker()
        val flakyBus = FlakyRedisBus(broker.newBus())
        val errors = CopyOnWriteArrayList<Throwable>()
        val prefix = "test-${UUID.randomUUID()}"
        val first = newServer(
            RedisBusFactory { flakyBus },
            prefix,
            "first",
            changeFlushTimeout = 50.milliseconds,
            changeRetryDelay = 5.milliseconds,
            onError = errors::add,
        )
        val second = newServer(RedisBusFactory(broker::newBus), prefix, "second")
        val firstConnection = first.openDirectConnection("retry", Unit)
        val secondConnection = second.openDirectConnection("retry", Unit)

        flakyBus.failPublications.set(true)
        firstConnection.transactYks { it.getText("body").insert(0, "retained") }
        val failure = assertFailsWith<IllegalStateException> { firstConnection.disconnect() }

        assertTrue(first.document("retry") != null)
        assertTrue(failure.message?.contains("did not flush") == true)
        assertTrue(errors.any { it.message?.contains("publication failure") == true })

        flakyBus.failPublications.set(false)
        eventually {
            textValue(secondConnection.document.encodeStateAsUpdate()) == "retained"
        }

        secondConnection.disconnect()
        first.shutdown()
        second.shutdown()
    }

    @Test
    fun `distributed lock has one owner and validates the release token`() = runBlocking {
        val broker = InMemoryRedisBroker()
        val first = broker.newBus()
        val second = broker.newBus()

        assertTrue(first.tryAcquireLock("document:lock", "first", 1.seconds))
        assertFalse(second.tryAcquireLock("document:lock", "second", 1.seconds))
        second.releaseLock("document:lock", "second")
        assertFalse(second.tryAcquireLock("document:lock", "second", 1.seconds))
        first.releaseLock("document:lock", "first")
        assertTrue(second.tryAcquireLock("document:lock", "second", 1.seconds))

        first.close()
        second.close()
    }

    @Test
    fun `store waits for a contended lock instead of marking the document durable`() = runBlocking {
        val broker = InMemoryRedisBroker()
        val blocker = broker.newBus()
        val prefix = "test-${UUID.randomUUID()}"
        val documentName = "contended"
        val lockKey = "$prefix:$documentName:lock"
        val storage = RecordingStorage()
        assertTrue(blocker.tryAcquireLock(lockKey, "blocker", 5.seconds))
        val server = newServer(
            RedisBusFactory(broker::newBus),
            prefix,
            "server",
            storage = storage,
            lockAcquireTimeout = 2.seconds,
            lockRetryDelay = 5.milliseconds,
        )
        val connection = server.openDirectConnection(documentName, Unit)
        connection.transactYks { it.getText("body").insert(0, "retained") }

        coroutineScope {
            val disconnect = async { connection.disconnect() }
            delay(75.milliseconds)
            assertFalse(disconnect.isCompleted)
            assertNull(storage.state.get())
            blocker.releaseLock(lockKey, "blocker")
            withTimeout(2.seconds) { disconnect.await() }
        }

        assertEquals("retained", textValue(checkNotNull(storage.state.get())))
        server.shutdown()
        blocker.close()
    }

    @Test
    fun `store lock lease is renewed while persistence is in progress`() = runBlocking {
        val broker = InMemoryRedisBroker()
        val contender = broker.newBus()
        val prefix = "test-${UUID.randomUUID()}"
        val documentName = "slow-store"
        val lockKey = "$prefix:$documentName:lock"
        val storeStarted = CompletableDeferred<Unit>()
        val finishStore = CompletableDeferred<Unit>()
        val storage = object : DocumentStorage {
            override suspend fun load(documentName: String): ByteArray? = null

            override suspend fun store(documentName: String, state: ByteArray) {
                storeStarted.complete(Unit)
                finishStore.await()
            }
        }
        val server = newServer(
            RedisBusFactory(broker::newBus),
            prefix,
            "server",
            storage = storage,
            lockTimeout = 60.milliseconds,
            lockRetryDelay = 5.milliseconds,
        )
        val connection = server.openDirectConnection(documentName, Unit)
        connection.transactYks { it.getText("body").insert(0, "slow") }

        coroutineScope {
            val disconnect = async { connection.disconnect() }
            storeStarted.await()
            delay(150.milliseconds)
            assertFalse(contender.tryAcquireLock(lockKey, "contender", 1.seconds))
            finishStore.complete(Unit)
            withTimeout(2.seconds) { disconnect.await() }
        }

        assertTrue(contender.tryAcquireLock(lockKey, "contender", 1.seconds))
        contender.releaseLock(lockKey, "contender")
        server.shutdown()
        contender.close()
    }

    @Test
    fun `failed persistence releases the store lock and keeps the document dirty`() = runBlocking {
        val broker = InMemoryRedisBroker()
        val contender = broker.newBus()
        val prefix = "test-${UUID.randomUUID()}"
        val documentName = "failed-store"
        val lockKey = "$prefix:$documentName:lock"
        val failStore = AtomicBoolean(true)
        val storage = object : DocumentStorage {
            override suspend fun load(documentName: String): ByteArray? = null

            override suspend fun store(documentName: String, state: ByteArray) {
                check(!failStore.get()) { "simulated persistence failure" }
            }
        }
        val server = newServer(
            RedisBusFactory(broker::newBus),
            prefix,
            "server",
            storage = storage,
            lockRetryDelay = 5.milliseconds,
        )
        val connection = server.openDirectConnection(documentName, Unit)
        connection.transactYks { it.getText("body").insert(0, "retry") }

        assertFailsWith<IllegalStateException> { connection.disconnect() }
        assertTrue(server.document(documentName) != null)
        assertTrue(contender.tryAcquireLock(lockKey, "contender", 1.seconds))
        contender.releaseLock(lockKey, "contender")

        failStore.set(false)
        server.flushPendingStores()
        assertNull(server.document(documentName))
        server.shutdown()
        contender.close()
    }

    @Test
    fun `lettuce bus synchronizes two servers through Redis`() = runBlocking {
        val redisUrl = System.getenv("REDIS_URL")
        assumeTrue(!redisUrl.isNullOrBlank(), "REDIS_URL is required for the Redis integration test")
        verifyMultiNodeSync(RedisBusFactory { LettuceRedisBus.connect(redisUrl) })
    }

    @Test
    fun `lettuce bus renews only the matching lock token`() = runBlocking {
        val redisUrl = System.getenv("REDIS_URL")
        assumeTrue(!redisUrl.isNullOrBlank(), "REDIS_URL is required for the Redis integration test")
        val first = LettuceRedisBus.connect(redisUrl)
        val second = LettuceRedisBus.connect(redisUrl)
        val key = "test-${UUID.randomUUID()}:lock"

        try {
            assertTrue(first.tryAcquireLock(key, "first", 200.milliseconds))
            assertFalse(second.renewLock(key, "second", 1.seconds))
            delay(75.milliseconds)
            assertTrue(first.renewLock(key, "first", 1.seconds))
            delay(250.milliseconds)
            assertFalse(second.tryAcquireLock(key, "second", 1.seconds))
            first.releaseLock(key, "first")
            assertTrue(second.tryAcquireLock(key, "second", 1.seconds))
        } finally {
            second.releaseLock(key, "second")
            first.close()
            second.close()
        }
    }

    private suspend fun verifyMultiNodeSync(busFactory: RedisBusFactory) {
        val prefix = "test-${UUID.randomUUID()}"
        val first = newServer(busFactory, prefix, "first")
        val second = newServer(busFactory, prefix, "second")
        val firstConnection = first.openDirectConnection("shared", Unit)
        firstConnection.transactYks { it.getText("body").insert(0, "before peer") }

        val secondConnection = second.openDirectConnection("shared", Unit)
        assertEquals("before peer", textValue(secondConnection.document.encodeStateAsUpdate()))

        secondConnection.transactYks { it.getText("body").insert(11, " + live") }
        eventually {
            textValue(firstConnection.document.encodeStateAsUpdate()) == "before peer + live"
        }

        firstConnection.transactYks { it.getText("body").insert(18, " + back") }
        eventually {
            textValue(secondConnection.document.encodeStateAsUpdate()) == "before peer + live + back"
        }

        firstConnection.disconnect()
        secondConnection.disconnect()
        first.shutdown()
        second.shutdown()
    }

    private fun newServer(
        busFactory: RedisBusFactory,
        prefix: String,
        identifier: String,
        changeFlushTimeout: Duration = 5.seconds,
        changeRetryDelay: Duration = 100.milliseconds,
        storage: DocumentStorage? = null,
        lockTimeout: Duration = 30.seconds,
        lockAcquireTimeout: Duration = 10.seconds,
        lockRetryDelay: Duration = 25.milliseconds,
        onError: (Throwable) -> Unit = {},
    ): HocuspocusServer<Unit> {
        val extensions = buildList {
            add(
                RedisExtension(
                    busFactory,
                    RedisExtensionConfiguration(
                        prefix = prefix,
                        identifier = identifier,
                        lockTimeout = lockTimeout,
                        lockAcquireTimeout = lockAcquireTimeout,
                        lockRetryDelay = lockRetryDelay,
                        disconnectDelay = Duration.ZERO,
                        initialSyncTimeout = 2.seconds,
                        changeFlushTimeout = changeFlushTimeout,
                        changeRetryDelay = changeRetryDelay,
                    ),
                ),
            )
            storage?.let { add(DatabaseExtension<Unit>(it)) }
        }
        return HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = extensions,
                debounce = 10.seconds,
                maxDebounce = 10.seconds,
                onError = onError,
            ),
        )
    }

    private suspend fun eventually(
        timeout: Duration = 2.seconds,
        assertion: () -> Boolean,
    ) {
        withTimeout(timeout) {
            while (!assertion()) delay(5.milliseconds)
        }
    }

    private fun textValue(update: ByteArray): String {
        val document = YDoc()
        return try {
            document.applyUpdate(update)
            document.getText("body").toString()
        } finally {
            document.destroy()
        }
    }
}

private class InMemoryRedisBroker {
    private val monitor: Any = Any()
    private val subscriptions:
        MutableMap<String, MutableMap<InMemoryRedisBus, (ByteArray) -> Unit>> = linkedMapOf()
    private val locks: MutableMap<String, Lock> = linkedMapOf()

    fun newBus(): RedisBus = InMemoryRedisBus(this)

    fun publish(channel: String, message: ByteArray) {
        val listeners = synchronized(monitor) {
            subscriptions[channel]?.values?.toList().orEmpty()
        }
        listeners.forEach { listener -> listener(message.copyOf()) }
    }

    fun subscribe(
        bus: InMemoryRedisBus,
        channel: String,
        listener: (ByteArray) -> Unit,
    ) {
        synchronized(monitor) {
            val channelSubscriptions = subscriptions.getOrPut(channel, ::linkedMapOf)
            check(channelSubscriptions.putIfAbsent(bus, listener) == null)
        }
    }

    fun unsubscribe(bus: InMemoryRedisBus, channel: String) {
        synchronized(monitor) {
            subscriptions[channel]?.let { channelSubscriptions ->
                channelSubscriptions.remove(bus)
                if (channelSubscriptions.isEmpty()) subscriptions.remove(channel)
            }
        }
    }

    fun unsubscribeAll(bus: InMemoryRedisBus) {
        synchronized(monitor) {
            subscriptions.keys.toList().forEach { unsubscribe(bus, it) }
        }
    }

    fun subscriberCount(channel: String): Long = synchronized(monitor) {
        subscriptions[channel]?.size?.toLong() ?: 0L
    }

    fun tryAcquireLock(
        key: String,
        token: String,
        timeout: Duration,
    ): Boolean = synchronized(monitor) {
        val now = System.nanoTime()
        locks[key]?.takeIf { it.expiresAtNanos > now }?.let { return false }
        locks[key] = Lock(token, now + timeout.inWholeNanoseconds)
        true
    }

    fun releaseLock(key: String, token: String) {
        synchronized(monitor) {
            if (locks[key]?.token == token) locks.remove(key)
        }
    }

    fun renewLock(key: String, token: String, timeout: Duration): Boolean = synchronized(monitor) {
        val now = System.nanoTime()
        val current = locks[key]
        if (current?.token != token || current.expiresAtNanos <= now) return false
        locks[key] = Lock(token, now + timeout.inWholeNanoseconds)
        true
    }

    private data class Lock(
        val token: String,
        val expiresAtNanos: Long,
    )
}

private class InMemoryRedisBus(
    private val broker: InMemoryRedisBroker,
) : RedisBus {
    private val subscriptions: MutableSet<String> = ConcurrentHashMap.newKeySet()

    override suspend fun publish(channel: String, message: ByteArray) {
        broker.publish(channel, message)
    }

    override suspend fun subscribe(channel: String, listener: (ByteArray) -> Unit) {
        check(subscriptions.add(channel)) { "already subscribed to $channel" }
        try {
            broker.subscribe(this, channel, listener)
        } catch (error: Throwable) {
            subscriptions.remove(channel)
            throw error
        }
    }

    override suspend fun unsubscribe(channel: String) {
        subscriptions.remove(channel)
        broker.unsubscribe(this, channel)
    }

    override suspend fun subscriberCount(channel: String): Long =
        broker.subscriberCount(channel)

    override suspend fun tryAcquireLock(
        key: String,
        token: String,
        timeout: Duration,
    ): Boolean = broker.tryAcquireLock(key, token, timeout)

    override suspend fun releaseLock(key: String, token: String) {
        broker.releaseLock(key, token)
    }

    override suspend fun renewLock(
        key: String,
        token: String,
        timeout: Duration,
    ): Boolean = broker.renewLock(key, token, timeout)

    override suspend fun close() {
        subscriptions.clear()
        broker.unsubscribeAll(this)
    }
}

private class RecordingStorage : DocumentStorage {
    val state: AtomicReference<ByteArray?> = AtomicReference()

    override suspend fun load(documentName: String): ByteArray? = state.get()?.copyOf()

    override suspend fun store(documentName: String, state: ByteArray) {
        this.state.set(state.copyOf())
    }
}

private class FlakyRedisBus(
    private val delegate: RedisBus,
) : RedisBus by delegate {
    val failPublications: AtomicBoolean = AtomicBoolean()

    override suspend fun publish(channel: String, message: ByteArray) {
        check(!failPublications.get()) { "simulated Redis publication failure" }
        delegate.publish(channel, message)
    }
}
