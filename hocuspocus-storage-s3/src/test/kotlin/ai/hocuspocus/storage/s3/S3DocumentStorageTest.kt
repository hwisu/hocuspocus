package ai.hocuspocus.storage.s3

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.net.URI
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull

class S3DocumentStorageTest {
    @Test
    fun `round trips through an encoded non traversable object key`() = runBlocking {
        val client = FakeS3Client()
        val storage = S3DocumentStorage(
            S3StorageConfiguration(bucket = "documents", maxDocumentBytes = 16),
            client,
        )

        storage.store("../tenant/private", byteArrayOf(1, 2, 3))

        val key = storage.objectKey("../tenant/private")
        assertFalse("../tenant/private" in key)
        assertFalse(".." in key)
        assertContentEquals(byteArrayOf(1, 2, 3), storage.load("../tenant/private"))
        assertEquals("documents", client.lastBucket)
    }

    @Test
    fun `rejects oversized state before upload`() = runBlocking {
        val client = FakeS3Client()
        val storage = S3DocumentStorage(
            S3StorageConfiguration(bucket = "documents", maxDocumentBytes = 2),
            client,
        )

        assertFailsWith<IllegalArgumentException> {
            storage.store("document", ByteArray(3))
        }
        assertEquals(0, client.values.size)
    }

    @Test
    fun `rejects insecure custom endpoints by default`() {
        assertFailsWith<IllegalArgumentException> {
            S3StorageConfiguration(
                bucket = "documents",
                endpointOverride = java.net.URI("http://127.0.0.1:9000"),
            )
        }
    }

    @Test
    fun `Node-compatible key mode reads and writes the upstream object layout`() = runBlocking {
        val client = FakeS3Client().apply {
            values["hocuspocus-documents/tenant/existing.bin"] = byteArrayOf(1, 2, 3)
        }
        val storage = S3DocumentStorage(
            S3StorageConfiguration(
                bucket = "documents",
                maxDocumentBytes = 16,
                keyEncoder = ::nodeCompatibleDocumentKey,
            ),
            client,
        )

        assertEquals(
            "hocuspocus-documents/tenant/existing.bin",
            storage.objectKey("tenant/existing"),
        )
        assertContentEquals(byteArrayOf(1, 2, 3), storage.load("tenant/existing"))

        storage.store("tenant/from-jvm", byteArrayOf(4, 5, 6))

        assertContentEquals(
            byteArrayOf(4, 5, 6),
            client.values["hocuspocus-documents/tenant/from-jvm.bin"],
        )
    }

    @Test
    fun `AWS SDK client round trips against an S3-compatible endpoint`() = runBlocking {
        val endpoint = System.getenv("S3_ENDPOINT")
        assumeTrue(!endpoint.isNullOrBlank(), "S3_ENDPOINT is required for the S3 integration test")
        val storage = S3DocumentStorage(
            S3StorageConfiguration(
                bucket = "documents",
                endpointOverride = URI(endpoint),
                forcePathStyle = true,
                allowInsecureEndpoint = true,
                maxDocumentBytes = 1_024,
            ),
        )
        val documentName = "integration-${UUID.randomUUID()}"
        val state = ByteArray(512) { index -> (index % 251).toByte() }

        try {
            assertNull(storage.load(documentName))
            storage.store(documentName, state)
            assertContentEquals(state, storage.load(documentName))
        } finally {
            storage.close()
        }
    }

    private class FakeS3Client : S3ObjectClient {
        val values: MutableMap<String, ByteArray> = linkedMapOf()
        var lastBucket: String? = null

        override suspend fun get(bucket: String, key: String, maxBytes: Int): ByteArray? {
            lastBucket = bucket
            return values[key]?.copyOf()
        }

        override suspend fun put(bucket: String, key: String, value: ByteArray) {
            lastBucket = bucket
            values[key] = value.copyOf()
        }
    }
}
