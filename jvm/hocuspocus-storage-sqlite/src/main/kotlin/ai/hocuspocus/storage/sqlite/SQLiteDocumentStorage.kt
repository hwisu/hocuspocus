package ai.hocuspocus.storage.sqlite

import ai.hocuspocus.core.DocumentStorage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

public data class SQLiteStorageConfiguration(
    val database: String = ":memory:",
    val maxDocumentBytes: Int = 5 * 1024 * 1024,
    val maxDocumentNameBytes: Int = 4 * 1024,
    val busyTimeout: Duration = 5.seconds,
    val writeAheadLog: Boolean = database != ":memory:",
) {
    init {
        require(database.isNotBlank()) { "database must not be blank" }
        require(maxDocumentBytes > 0) { "maxDocumentBytes must be positive" }
        require(maxDocumentNameBytes > 0) { "maxDocumentNameBytes must be positive" }
        require(!busyTimeout.isNegative() && busyTimeout.isFinite()) {
            "busyTimeout must be finite and not negative"
        }
        require(busyTimeout.inWholeMilliseconds <= Int.MAX_VALUE) {
            "busyTimeout is too large for SQLite"
        }
    }
}

/**
 * Parameterized, serialized SQLite storage with WAL enabled for file databases.
 */
public class SQLiteDocumentStorage(
    public val configuration: SQLiteStorageConfiguration = SQLiteStorageConfiguration(),
) : DocumentStorage, AutoCloseable {
    private val mutex: Mutex = Mutex()
    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:${configuration.database}")
    private val loadStatement: PreparedStatement
    private val storeStatement: PreparedStatement
    @Volatile
    private var closed: Boolean = false

    init {
        connection.createStatement().use { statement ->
            statement.execute("PRAGMA busy_timeout = ${configuration.busyTimeout.inWholeMilliseconds}")
            statement.execute("PRAGMA foreign_keys = ON")
            statement.execute("PRAGMA trusted_schema = OFF")
            if (configuration.writeAheadLog) statement.execute("PRAGMA journal_mode = WAL")
            statement.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    name TEXT PRIMARY KEY NOT NULL,
                    data BLOB NOT NULL
                )
                """.trimIndent(),
            )
        }
        loadStatement = connection.prepareStatement(
            "SELECT data, length(data) FROM documents WHERE name = ? LIMIT 1",
        )
        storeStatement = connection.prepareStatement(
            """
            INSERT INTO documents (name, data) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET data = excluded.data
            """.trimIndent(),
        )
    }

    override suspend fun load(documentName: String): ByteArray? = withConnection {
        validateDocumentName(documentName)
        loadStatement.setString(1, documentName)
        loadStatement.executeQuery().use { result ->
            if (!result.next()) return@withConnection null
            require(result.getLong(2) <= configuration.maxDocumentBytes.toLong()) {
                "stored document exceeds configured maxDocumentBytes"
            }
            result.getBytes(1)
        }
    }

    override suspend fun store(documentName: String, state: ByteArray) {
        validateDocumentName(documentName)
        require(state.size <= configuration.maxDocumentBytes) {
            "document state exceeds configured maxDocumentBytes"
        }
        withConnection {
            storeStatement.setString(1, documentName)
            storeStatement.setBytes(2, state)
            check(storeStatement.executeUpdate() == 1) { "SQLite document upsert affected no row" }
        }
    }

    override fun close() {
        runBlocking {
            mutex.withLock {
                if (closed) return@withLock
                closed = true
                withContext(Dispatchers.IO) {
                    var failure: Throwable? = null
                    listOf(loadStatement, storeStatement, connection).forEach { resource ->
                        try {
                            resource.close()
                        } catch (error: Throwable) {
                            failure?.addSuppressed(error) ?: run { failure = error }
                        }
                    }
                    failure?.let { throw it }
                }
            }
        }
    }

    private suspend fun <T> withConnection(block: () -> T): T = mutex.withLock {
        check(!closed) { "SQLite storage is closed" }
        withContext(Dispatchers.IO) { block() }
    }

    private fun validateDocumentName(documentName: String) {
        require(documentName.isNotBlank()) { "documentName must not be blank" }
        require(documentName.toByteArray(StandardCharsets.UTF_8).size <= configuration.maxDocumentNameBytes) {
            "documentName exceeds configured maxDocumentNameBytes"
        }
    }
}
