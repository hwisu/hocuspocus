plugins {
    id("org.jetbrains.kotlin.jvm")
    application
}

val hocuspocusVersion = providers.gradleProperty("hocuspocusVersion")
    .getOrElse("0.1.3-SNAPSHOT")

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation("ai.hocuspocus:hocuspocus-ktor:$hocuspocusVersion")
}

application {
    mainClass.set("ConsumerSmokeKt")
}
