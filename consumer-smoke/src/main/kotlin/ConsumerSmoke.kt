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
        val question = document.createMap()
        document.getMap("questions").set("42", question)
        question.set("id", 42)
        question.set("assignUser", listOf("user-1", "사용자-😀"))
        val paragraph = document.createXmlElement("paragraph")
        document.getXmlFragment("42").push(paragraph)
        val answer = document.createXmlText()
        paragraph.push(answer)
        answer.insert(0, "저장된 답변 😀")
    }
    check(connection.document.encodeStateAsUpdate().isNotEmpty())
    connection.disconnect()
    server.shutdown()
}
