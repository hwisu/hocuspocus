dependencies {
    api(project(":hocuspocus-core"))
    api(project(":hocuspocus-yks"))
    api(libs.ktor.server.core)
    api(libs.ktor.server.websockets)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.websockets)
}
