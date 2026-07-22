package ai.hocuspocus.benchmark

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds

public object PerformanceSuite {
    @JvmStatic
    public fun main(args: Array<String>) {
        val duration = args.firstOrNull { it.startsWith("--duration=") }
            ?.substringAfter('=')
            ?.let(Duration::parse)
            ?: Duration.parse("30m")
        val soakOnly = "--soak-only" in args

        if (!soakOnly) {
            runFanoutMatrix()
            runPersistenceMatrix()
            runSlowConsumer()
            val heap = PerformanceScenarios.retainedDocumentProbe(1_000)
            check(heap.retainedReferences <= 10) {
                "loaded documents retained after unload: ${heap.retainedReferences}/${heap.documents}"
            }
            println(
                "retained-heap documents=${heap.documents} retained=${heap.retainedReferences} " +
                    "deltaBytes=${heap.heapDeltaBytes}",
            )
        }
        runSoak(duration)
    }

    private fun runFanoutMatrix() {
        listOf(1, 10, 100, 1_000).forEach { clients ->
            BenchmarkHarness(clients).use { harness ->
                listOf(1_024, 64 * 1_024, 512 * 1_024).forEach { updateBytes ->
                    val result = harness.broadcast(updateBytes)
                    check(result.frames == clients.toLong()) {
                        "fanout delivered ${result.frames}/$clients frames"
                    }
                    println(
                        "fanout clients=$clients updateBytes=$updateBytes " +
                            "elapsedUs=${result.elapsedNanos.nanoseconds.inWholeMicroseconds} " +
                            "wireBytes=${result.bytes}",
                    )
                }
            }
        }
    }

    private fun runPersistenceMatrix() {
        listOf(Duration.ZERO, 5.milliseconds, 25.milliseconds).forEach { latency ->
            val elapsed = PerformanceScenarios.persistenceLatency(latency)
            check(elapsed >= latency) { "persistence returned before storage completed" }
            println("persistence storageLatency=$latency elapsed=$elapsed")
        }
    }

    private fun runSlowConsumer() {
        BenchmarkHarness(clientCount = 10, rejectAfterFrames = 0).use { harness ->
            harness.broadcast(1_024)
            val deadline = System.nanoTime() + Duration.parse("5s").inWholeNanoseconds
            while (harness.activeClients() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(1)
            }
            check(harness.activeClients() == 0) { "slow consumers were not disconnected" }
            println("slow-consumer clients=10 disconnected=10")
        }
    }

    private fun runSoak(duration: Duration) {
        require(duration.isPositive() && duration.isFinite()) { "duration must be positive and finite" }
        val deadline = System.nanoTime() + duration.inWholeNanoseconds
        var operations = 0L
        var cycles = 0L
        while (System.nanoTime() < deadline) {
            BenchmarkHarness(100).use { harness ->
                repeat(30) {
                    if (System.nanoTime() >= deadline) return@repeat
                    val updateSize = when (operations % 3L) {
                        0L -> 1_024
                        1L -> 64 * 1_024
                        else -> 512 * 1_024
                    }
                    val result = harness.broadcast(updateSize)
                    check(result.frames == 100L) { "soak fanout lost recipients" }
                    check(harness.failures().isEmpty()) {
                        "soak observed ${harness.failures().size} server failures"
                    }
                    operations += 1
                }
                check(harness.activeClients() == 100) { "soak lost active clients" }
            }
            cycles += 1
        }
        println("soak duration=$duration operations=$operations cycles=$cycles clients=100")
    }
}
