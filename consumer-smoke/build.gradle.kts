plugins {
    kotlin("jvm") version "2.2.20"
    application
}

val hocuspocusVersion = providers.gradleProperty("hocuspocusVersion")
    .getOrElse("0.1.0-SNAPSHOT")

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation("ai.hocuspocus:hocuspocus-ktor:$hocuspocusVersion")
}

application {
    mainClass.set("ConsumerSmokeKt")
}
