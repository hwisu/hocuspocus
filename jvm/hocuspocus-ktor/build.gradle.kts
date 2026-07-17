dependencies {
    api(project(":hocuspocus-core"))
    api(project(":hocuspocus-yks"))
    api("io.ktor:ktor-server-core:3.5.1")
    api("io.ktor:ktor-server-websockets:3.5.1")

    testImplementation("io.ktor:ktor-server-test-host:3.5.1")
    testImplementation("io.ktor:ktor-client-websockets:3.5.1")
}
