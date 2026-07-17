plugins {
    application
}

dependencies {
    implementation(project(":hocuspocus-ktor"))
    implementation("io.ktor:ktor-server-netty:3.5.1")
    implementation("io.ktor:ktor-server-status-pages:3.5.1")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.35")
}

application {
    mainClass.set("ai.hocuspocus.ktor.example.MainKt")
}
