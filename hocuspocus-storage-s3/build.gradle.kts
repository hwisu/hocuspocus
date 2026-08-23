dependencies {
    api(project(":hocuspocus-core"))
    implementation(platform(libs.netty41.bom)) {
        because(
            "GHSA-8c42-7qj2-3j46: the AWS SDK resolves Netty 4.1.136.Final; " +
                "CorsHandler is fixed in 4.1.137.Final.",
        )
    }
    implementation(libs.aws.s3)
    implementation(libs.kotlinx.coroutines.jdk8)

    constraints {
        implementation(libs.httpclient5) {
            because(
                "GHSA-hjcp-jmpx-g3qm: the AWS SDK's apache5-client resolves httpclient5 5.6.2, " +
                    "whose Content-Encoding decode error leaks pooled connections. Fixed in 5.6.3.",
            )
        }
    }
}
