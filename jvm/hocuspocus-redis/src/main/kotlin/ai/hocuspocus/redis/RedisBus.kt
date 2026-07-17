package ai.hocuspocus.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration

private object StringByteArrayRedisCodec : RedisCodec<String, ByteArray> {
    override fun decodeKey(bytes: ByteBuffer): String =
        StandardCharsets.UTF_8.decode(bytes).toString()

    override fun decodeValue(bytes: ByteBuffer): ByteArray =
        ByteArray(bytes.remaining()).also(bytes::get)

    override fun encodeKey(key: String): ByteBuffer =
        StandardCharsets.UTF_8.encode(key)

    override fun encodeValue(value: ByteArray): ByteBuffer =
        ByteBuffer.wrap(value)
}

public interface RedisBus {
    public suspend fun publish(channel: String, message: ByteArray)

    public suspend fun subscribe(channel: String, listener: (ByteArray) -> Unit)

    public suspend fun unsubscribe(channel: String)

    public suspend fun subscriberCount(channel: String): Long

    public suspend fun tryAcquireLock(
        key: String,
        token: String,
        timeout: Duration,
    ): Boolean

    public suspend fun releaseLock(key: String, token: String)

    public suspend fun close()
}

public fun interface RedisBusFactory {
    public suspend fun create(): RedisBus
}

public class LettuceRedisBus private constructor(
    private val client: RedisClient,
    private val publisher: StatefulRedisConnection<String, ByteArray>,
    private val subscriber: StatefulRedisPubSubConnection<String, ByteArray>,
) : RedisBus {
    private val listeners: ConcurrentHashMap<String, (ByteArray) -> Unit> = ConcurrentHashMap()

    init {
        subscriber.addListener(
            object : RedisPubSubAdapter<String, ByteArray>() {
                override fun message(channel: String, message: ByteArray) {
                    listeners[channel]?.invoke(message.copyOf())
                }
            },
        )
    }

    override suspend fun publish(channel: String, message: ByteArray) {
        publisher.async().publish(channel, message).await()
    }

    override suspend fun subscribe(channel: String, listener: (ByteArray) -> Unit) {
        check(listeners.putIfAbsent(channel, listener) == null) {
            "already subscribed to Redis channel $channel"
        }
        try {
            subscriber.async().subscribe(channel).await()
        } catch (error: Throwable) {
            listeners.remove(channel, listener)
            throw error
        }
    }

    override suspend fun unsubscribe(channel: String) {
        listeners.remove(channel)
        subscriber.async().unsubscribe(channel).await()
    }

    override suspend fun subscriberCount(channel: String): Long =
        publisher.async().pubsubNumsub(channel).await()[channel] ?: 0L

    override suspend fun tryAcquireLock(
        key: String,
        token: String,
        timeout: Duration,
    ): Boolean {
        val result = publisher.async().set(
            key,
            token.toByteArray(StandardCharsets.UTF_8),
            SetArgs().nx().px(timeout.inWholeMilliseconds),
        ).await()
        return result == "OK"
    }

    override suspend fun releaseLock(key: String, token: String) {
        publisher.async().eval<Long>(
            RELEASE_LOCK_SCRIPT,
            ScriptOutputType.INTEGER,
            arrayOf(key),
            token.toByteArray(StandardCharsets.UTF_8),
        ).await()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) {
            subscriber.close()
            publisher.close()
            client.shutdown()
        }
    }

    public companion object {
        public suspend fun connect(uri: String): LettuceRedisBus {
            val client = RedisClient.create(RedisURI.create(uri))
            try {
                val redisUri = RedisURI.create(uri)
                val publisher: StatefulRedisConnection<String, ByteArray> =
                    client.connectAsync(StringByteArrayRedisCodec, redisUri).await()
                try {
                    val subscriber: StatefulRedisPubSubConnection<String, ByteArray> =
                        client.connectPubSubAsync(StringByteArrayRedisCodec, redisUri).await()
                    return LettuceRedisBus(client, publisher, subscriber)
                } catch (error: Throwable) {
                    publisher.close()
                    throw error
                }
            } catch (error: Throwable) {
                client.shutdown()
                throw error
            }
        }

        private const val RELEASE_LOCK_SCRIPT: String =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]) else return 0 end"
    }
}
