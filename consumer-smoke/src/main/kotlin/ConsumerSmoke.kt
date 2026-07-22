import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.yks.YksDocumentFactory
import ai.hocuspocus.yks.transactYks
import kotlinx.coroutines.runBlocking

fun main(): Unit = runBlocking {
    val server = HocuspocusServer(
        HocuspocusConfiguration<Unit>(
            documentFactory = YksDocumentFactory(),
        ),
    )
    val connection = server.openDirectConnection("consumer-smoke", Unit)
    connection.transactYks { document ->
        document.getText("body").insert(0, "ok")
    }
    check(connection.document.encodeStateAsUpdate().isNotEmpty())
    connection.disconnect()
    server.shutdown()
}
