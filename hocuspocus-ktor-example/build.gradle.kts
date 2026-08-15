plugins {
    application
}

dependencies {
    implementation(platform(libs.netty.bom))
    implementation(project(":hocuspocus-ktor"))
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.status.pages)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass.set("ai.hocuspocus.ktor.example.MainKt")
}
