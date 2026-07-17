plugins {
    application
}

val nettyNativeTransport = when {
    System.getProperty("os.name").lowercase().contains("mac") -> {
        val classifier = when (System.getProperty("os.arch").lowercase()) {
            "aarch64", "arm64" -> "osx-aarch_64"
            "amd64", "x86_64" -> "osx-x86_64"
            else -> null
        }
        classifier?.let { "io.netty:netty-transport-native-kqueue:4.2.15.Final:$it" }
    }
    System.getProperty("os.name").lowercase().contains("linux") -> {
        val classifier = when (System.getProperty("os.arch").lowercase()) {
            "aarch64", "arm64" -> "linux-aarch_64"
            "amd64", "x86_64" -> "linux-x86_64"
            else -> null
        }
        classifier?.let { "io.netty:netty-transport-native-epoll:4.2.15.Final:$it" }
    }
    else -> null
}

dependencies {
    implementation(project(":hocuspocus-ktor"))
    implementation("io.ktor:ktor-server-netty:3.5.1")
    implementation("io.ktor:ktor-server-status-pages:3.5.1")
    nettyNativeTransport?.let { runtimeOnly(it) }
    runtimeOnly("ch.qos.logback:logback-classic:1.5.35")
}

application {
    mainClass.set("ai.hocuspocus.ktor.example.MainKt")
}
