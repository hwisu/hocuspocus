package ai.hocuspocus.ktor.example

import ai.hocuspocus.core.ConnectedPayload
import ai.hocuspocus.core.DatabaseExtension
import ai.hocuspocus.core.DocumentStorage
import ai.hocuspocus.core.HocuspocusConfiguration
import ai.hocuspocus.core.HocuspocusAuthenticationException
import ai.hocuspocus.core.HocuspocusAuthenticator
import ai.hocuspocus.core.HocuspocusExtension
import ai.hocuspocus.core.HocuspocusServer
import ai.hocuspocus.core.StatelessPayload
import ai.hocuspocus.core.TokenSyncPayload
import ai.hocuspocus.ktor.HocuspocusKtor
import ai.hocuspocus.yks.YksDocumentFactory
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import java.util.concurrent.ConcurrentHashMap

public fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    val server = HocuspocusServer(
        HocuspocusConfiguration(
            documentFactory = YksDocumentFactory(),
            authenticator = HocuspocusAuthenticator { payload ->
                if (payload.token != "oracle-token") throw HocuspocusAuthenticationException()
            },
            extensions = listOf(
                OracleExtension,
                DatabaseExtension<Unit>(OracleStorage),
            ),
            onError = Throwable::printStackTrace,
        ),
    )

    embeddedServer(Netty, host = "0.0.0.0", port = port) {
        install(HocuspocusKtor) {
            path = "/collab"
            use(server)
        }
        routing {
            get("/health") {
                call.respondText("ok", ContentType.Text.Plain)
            }
            get("/documents-count") {
                call.respondText(server.documentsCount.toString(), ContentType.Text.Plain)
            }
        }
    }.start(wait = true)
}

private object OracleStorage : DocumentStorage {
    private val states: ConcurrentHashMap<String, ByteArray> = ConcurrentHashMap()

    override suspend fun load(documentName: String): ByteArray? = states[documentName]?.copyOf()

    override suspend fun store(documentName: String, state: ByteArray) {
        states[documentName] = state.copyOf()
    }
}

private object OracleExtension : HocuspocusExtension<Unit> {
    override suspend fun connected(payload: ConnectedPayload<Unit>) {
        payload.connection.requestToken()
    }

    override suspend fun onTokenSync(payload: TokenSyncPayload<Unit>) {
        payload.connection.sendStateless("token:${payload.token}")
    }

    override suspend fun onStateless(payload: StatelessPayload<Unit>) {
        payload.connection.document.broadcastStateless(payload.payload)
    }
}
