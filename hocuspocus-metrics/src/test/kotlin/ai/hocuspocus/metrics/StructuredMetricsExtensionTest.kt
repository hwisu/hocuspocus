package ai.hocuspocus.metrics

import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.yks.YksDocumentFactory
import ai.hocuspocus.yks.transactYks
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class StructuredMetricsExtensionTest {
    @Test
    fun `records low-cardinality lifecycle and change metrics`() = runBlocking {
        val sink = InMemoryMetricsSink()
        val server = HocuspocusServer(
            HocuspocusConfiguration(
                documentFactory = YksDocumentFactory(),
                extensions = listOf(StructuredMetricsExtension<Unit>(sink)),
            ),
        )
        val connection = server.openDirectConnection("private-document-name", Unit)
        connection.transactYks { it.getText("body").insert(0, "metric") }
        connection.disconnect()
        eventually { sink.snapshot().any { it.name == "hocuspocus.change.bytes" } }
        server.shutdown()

        val points = sink.snapshot()
        assertTrue(points.any { it.name == "hocuspocus.document.load.duration" })
        assertTrue(points.any { it.name == "hocuspocus.document.store.duration" })
        assertTrue(points.any { it.name == "hocuspocus.server.stop" })
        assertFalse(points.any { "document" in it.attributes })
    }

    private suspend fun eventually(assertion: () -> Boolean) {
        withTimeout(2.seconds) {
            while (!assertion()) delay(5.milliseconds)
        }
    }
}
