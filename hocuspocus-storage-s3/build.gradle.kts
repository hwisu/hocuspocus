dependencies {
    api(project(":hocuspocus-core"))
    implementation(platform(libs.netty41.bom)) {
        because(
            "The AWS SDK resolves Netty 4.1.136.Final; keep its HTTP client on " +
                "the 4.1.139.Final security release (HTTP parsing, HTTP/2 resource limits, SNI).",
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
