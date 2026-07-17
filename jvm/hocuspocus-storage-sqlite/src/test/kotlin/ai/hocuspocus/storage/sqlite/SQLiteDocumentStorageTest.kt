package ai.hocuspocus.storage.sqlite

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.io.path.absolutePathString
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith

class SQLiteDocumentStorageTest {
    @Test
    fun `persists parameterized document names across reopen`() = runBlocking {
        val path = Files.createTempFile("hocuspocus-", ".sqlite")
        try {
            SQLiteDocumentStorage(
                SQLiteStorageConfiguration(path.absolutePathString()),
            ).use { storage ->
                storage.store("tenant'); DROP TABLE documents; --", byteArrayOf(1, 2, 3))
            }
            SQLiteDocumentStorage(
                SQLiteStorageConfiguration(path.absolutePathString()),
            ).use { storage ->
                assertContentEquals(
                    byteArrayOf(1, 2, 3),
                    storage.load("tenant'); DROP TABLE documents; --"),
                )
            }
        } finally {
            Files.deleteIfExists(path)
            Files.deleteIfExists(path.resolveSibling("${path.fileName}-wal"))
            Files.deleteIfExists(path.resolveSibling("${path.fileName}-shm"))
        }
    }

    @Test
    fun `serializes concurrent upserts on one connection`() = runBlocking {
        SQLiteDocumentStorage().use { storage ->
            (0 until 50).map { value ->
                async { storage.store("same", byteArrayOf(value.toByte())) }
            }.awaitAll()
            storage.store("same", byteArrayOf(49))
            val loaded = storage.load("same")
            requireNotNull(loaded)
            assertContentEquals(byteArrayOf(49), loaded)
        }
    }

    @Test
    fun `rejects oversized documents`() = runBlocking {
        SQLiteDocumentStorage(
            SQLiteStorageConfiguration(maxDocumentBytes = 2),
        ).use { storage ->
            assertFailsWith<IllegalArgumentException> {
                storage.store("oversized", ByteArray(3))
            }
        }
    }

    @Test
    fun `close is idempotent and rejects further operations`() = runBlocking {
        val storage = SQLiteDocumentStorage()
        storage.close()
        storage.close()

        assertFailsWith<IllegalStateException> {
            storage.load("closed")
        }
    }
}
