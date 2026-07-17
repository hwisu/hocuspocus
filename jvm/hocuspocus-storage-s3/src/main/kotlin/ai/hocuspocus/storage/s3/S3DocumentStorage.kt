package ai.hocuspocus.storage.s3

import ai.hocuspocus.core.DocumentStorage
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64

public data class S3StorageConfiguration(
    val bucket: String,
    val prefix: String = "hocuspocus-documents/",
    val region: String = "us-east-1",
    val endpointOverride: URI? = null,
    val forcePathStyle: Boolean = false,
    val allowInsecureEndpoint: Boolean = false,
    val maxDocumentBytes: Int = 5 * 1024 * 1024,
    val maxDocumentNameBytes: Int = 4 * 1024,
    val closeInjectedClient: Boolean = false,
    val keyEncoder: (String) -> String = ::base64DocumentKey,
) {
    init {
        require(bucket.isNotBlank()) { "bucket must not be blank" }
        require(region.isNotBlank()) { "region must not be blank" }
        require(maxDocumentBytes in 1 until Int.MAX_VALUE) {
            "maxDocumentBytes must be positive and smaller than Int.MAX_VALUE"
        }
        require(maxDocumentNameBytes > 0) { "maxDocumentNameBytes must be positive" }
        require('\u0000' !in prefix) { "prefix must not contain NUL" }
        endpointOverride?.let { endpoint ->
            require(!endpoint.host.isNullOrBlank()) { "endpointOverride must have a host" }
            require(endpoint.rawUserInfo == null) { "endpointOverride must not contain user info" }
            require(endpoint.fragment == null) { "endpointOverride must not contain a fragment" }
            val allowed = endpoint.scheme.equals("https", ignoreCase = true) ||
                (allowInsecureEndpoint && endpoint.scheme.equals("http", ignoreCase = true))
            require(allowed) { "endpointOverride must use HTTPS" }
        }
    }
}

public interface S3ObjectClient : AutoCloseable {
    public suspend fun get(
        bucket: String,
        key: String,
        maxBytes: Int,
    ): ByteArray?

    public suspend fun put(
        bucket: String,
        key: String,
        value: ByteArray,
    )

    override fun close() {}
}

public class AwsSdkS3ObjectClient(
    private val client: S3AsyncClient,
    private val closeDelegate: Boolean = true,
) : S3ObjectClient {
    override suspend fun get(
        bucket: String,
        key: String,
        maxBytes: Int,
    ): ByteArray? {
        val stream = try {
            client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                AsyncResponseTransformer.toBlockingInputStream(),
            ).await()
        } catch (error: Throwable) {
            if (error is CancellationException) throw error
            if (error.findS3Status() == 404) return null
            throw error
        }
        val bytes = withContext(Dispatchers.IO) {
            stream.use { it.readNBytes(maxBytes + 1) }
        }
        require(bytes.size <= maxBytes) { "S3 object exceeds configured maxDocumentBytes" }
        return bytes
    }

    override suspend fun put(
        bucket: String,
        key: String,
        value: ByteArray,
    ) {
        client.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/octet-stream")
                .build(),
            AsyncRequestBody.fromBytes(value),
        ).await()
    }

    override fun close() {
        if (closeDelegate) client.close()
    }

    public companion object {
        public fun create(configuration: S3StorageConfiguration): AwsSdkS3ObjectClient {
            val builder = S3AsyncClient.builder()
                .region(Region.of(configuration.region))
                .forcePathStyle(configuration.forcePathStyle)
            configuration.endpointOverride?.let(builder::endpointOverride)
            return AwsSdkS3ObjectClient(builder.build())
        }
    }
}

/**
 * Standard-update object storage adapter.
 *
 * Document names are base64url encoded by default, preventing slash traversal
 * and ambiguous object keys. Credentials come from the AWS default provider
 * chain; inject a preconfigured [S3ObjectClient] for custom credential sources.
 */
public class S3DocumentStorage(
    public val configuration: S3StorageConfiguration,
    client: S3ObjectClient? = null,
) : DocumentStorage, AutoCloseable {
    private val ownsClient: Boolean = client == null
    private val client: S3ObjectClient = client ?: AwsSdkS3ObjectClient.create(configuration)

    override suspend fun load(documentName: String): ByteArray? =
        client.get(configuration.bucket, objectKey(documentName), configuration.maxDocumentBytes)

    override suspend fun store(documentName: String, state: ByteArray) {
        require(state.size <= configuration.maxDocumentBytes) {
            "document state exceeds configured maxDocumentBytes"
        }
        client.put(configuration.bucket, objectKey(documentName), state.copyOf())
    }

    public fun objectKey(documentName: String): String {
        require(documentName.isNotBlank()) { "documentName must not be blank" }
        require(documentName.toByteArray(StandardCharsets.UTF_8).size <= configuration.maxDocumentNameBytes) {
            "documentName exceeds configured maxDocumentNameBytes"
        }
        val encoded = configuration.keyEncoder(documentName)
        require(encoded.isNotBlank()) { "keyEncoder returned a blank key" }
        require('\u0000' !in encoded) { "encoded document key must not contain NUL" }
        return configuration.prefix + encoded + ".bin"
    }

    override fun close() {
        if (ownsClient || configuration.closeInjectedClient) client.close()
    }
}

public fun base64DocumentKey(documentName: String): String =
    Base64.getUrlEncoder().withoutPadding()
        .encodeToString(documentName.toByteArray(StandardCharsets.UTF_8))

private fun Throwable.findS3Status(): Int? {
    var current: Throwable? = this
    while (current != null) {
        if (current is S3Exception) return current.statusCode()
        current = current.cause
    }
    return null
}
