package ai.hocuspocus.benchmark

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown

@State(Scope.Benchmark)
public open class ServerFanoutBenchmark {
    @Param("1", "10", "100", "1000")
    public var recipients: Int = 1

    private lateinit var harness: BenchmarkHarness

    @Setup(Level.Trial)
    public fun setup() {
        harness = BenchmarkHarness(recipients)
    }

    @Benchmark
    public fun fanout(): Long = harness.broadcast(1_024).bytes

    @TearDown(Level.Trial)
    public fun tearDown() {
        harness.close()
    }
}
