dependencies {
    api(project(":hocuspocus-core"))
    api(project(":hocuspocus-yks"))
    api(libs.ktor.server.core)
    api(libs.ktor.server.websockets)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.websockets)

    constraints {
        testImplementation(libs.httpclient5) {
            because(
                "Ktor's test host brings httpclient5 5.5.1 and httpcore5 5.3.6; " +
                    "5.6.4 also raises HttpCore to 5.4.3, fixing GHSA-hjcp-jmpx-g3qm, " +
                    "GHSA-v3jc-474w-2wm6, and GHSA-hf6x-8p5f-cgmf.",
            )
        }
    }
}
