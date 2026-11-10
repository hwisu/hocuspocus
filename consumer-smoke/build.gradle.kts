plugins {
    id("org.jetbrains.kotlin.jvm")
    application
}

val hocuspocusVersion = providers.gradleProperty("hocuspocusVersion")
    .getOrElse("0.1.11-SNAPSHOT")
val consumerKotlinVersion = providers.gradleProperty("consumerKotlinVersion").getOrElse("2.4.21")

kotlin {
    jvmToolchain(21)
    // Exercise the older compiler without loading its vulnerable Gradle plugin.
    @OptIn(
        org.jetbrains.kotlin.buildtools.api.ExperimentalBuildToolsApi::class,
        org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi::class,
    )
    compilerVersion.set(consumerKotlinVersion)
}

// Compiler plugins must follow the selected compiler, not the Gradle plugin.
configurations.matching { it.name.startsWith("kotlinCompilerPluginClasspath") }.configureEach {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.jetbrains.kotlin" && requested.name.startsWith("kotlin-scripting-")) {
            useVersion(consumerKotlinVersion)
        }
    }
}

dependencies {
    implementation("ai.hocuspocus:hocuspocus-ktor:$hocuspocusVersion")
}

application {
    mainClass.set("ConsumerSmokeKt")
}
