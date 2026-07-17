package ai.hocuspocus.benchmark

import ai.hocuspocus.protocol.FrameCodec
import ai.hocuspocus.protocol.MessageType
import ai.hocuspocus.protocol.RoutingKey
import ai.hocuspocus.protocol.SyncCodec
import ai.hocuspocus.protocol.SyncMessageType
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
public open class FrameEncodingBenchmark {
    @Param("1", "10", "100", "1000")
    public var recipients: Int = 1

    @Param("1024", "65536", "524288")
    public var updateBytes: Int = 1_024

    private lateinit var update: ByteArray
    private lateinit var routingKeys: List<RoutingKey>

    @Setup
    public fun setup() {
        update = ByteArray(updateBytes) { index -> index.toByte() }
        routingKeys = List(recipients) { RoutingKey("document") }
    }

    @Benchmark
    public fun encodePerRecipient(blackhole: Blackhole) {
        routingKeys.forEach { routingKey ->
            val sync = SyncCodec.encode(SyncMessageType.Update, update)
            blackhole.consume(FrameCodec.encode(routingKey, MessageType.Sync, sync))
        }
    }

    @Benchmark
    public fun encodeOncePerRoutingKey(blackhole: Blackhole) {
        val sync = SyncCodec.encode(SyncMessageType.Update, update)
        routingKeys.distinct().forEach { routingKey ->
            blackhole.consume(FrameCodec.encode(routingKey, MessageType.Sync, sync))
        }
    }

    @Benchmark
    public fun encodeFusedOncePerRoutingKey(blackhole: Blackhole) {
        routingKeys.distinct().forEach { routingKey ->
            blackhole.consume(FrameCodec.encodeSync(routingKey, SyncMessageType.Update, update))
        }
    }

    @Benchmark
    public fun retainSharedFramePerRecipient(blackhole: Blackhole) {
        val frame = FrameCodec.encodeSync(routingKeys.first(), SyncMessageType.Update, update)
        repeat(recipients) {
            blackhole.consume(frame)
        }
    }

    @Benchmark
    public fun copyFramePerRecipient(blackhole: Blackhole) {
        val frame = FrameCodec.encodeSync(routingKeys.first(), SyncMessageType.Update, update)
        repeat(recipients) {
            blackhole.consume(frame.copyOf())
        }
    }
}
