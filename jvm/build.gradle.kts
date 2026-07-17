import org.gradle.api.plugins.ExtensionAware
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.jvm.tasks.Jar
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import org.jetbrains.kotlin.gradle.dsl.abi.AbiValidationExtension
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation

plugins {
    kotlin("jvm") version "2.2.20" apply false
    id("me.champeau.jmh") version "0.7.3" apply false
    `maven-publish`
}

allprojects {
    group = "ai.hocuspocus"
    version = providers.gradleProperty("releaseVersion").getOrElse("0.1.0-SNAPSHOT")
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")

    dependencyLocking {
        lockAllConfigurations()
        lockMode.set(org.gradle.api.artifacts.dsl.LockMode.STRICT)
    }

    extensions.configure<KotlinJvmProjectExtension> {
        jvmToolchain(21)
        explicitApi()
        compilerOptions {
            allWarningsAsErrors.set(true)
            freeCompilerArgs.add("-Xjsr305=strict")
        }
        if (project.name !in setOf("hocuspocus-benchmark", "hocuspocus-ktor-example")) {
            @OptIn(ExperimentalAbiValidation::class)
            (this as ExtensionAware).extensions.configure<AbiValidationExtension>("abiValidation") {
                enabled.set(true)
            }
        }
    }

    extensions.configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<Jar>().configureEach {
        from(rootProject.file("../LICENSE.md")) {
            into("META-INF")
        }
        from(rootProject.file("THIRD_PARTY_NOTICES")) {
            into("META-INF")
        }
    }

    dependencies {
        "testImplementation"(kotlin("test"))
        "testImplementation"("org.junit.jupiter:junit-jupiter:5.13.4")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    if (name !in setOf("hocuspocus-benchmark", "hocuspocus-ktor-example")) {
        extensions.configure<PublishingExtension> {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(components["java"])
                    pom {
                        name.set(project.name)
                        description.set("Hocuspocus-compatible JVM runtime for Ktor")
                        url.set("https://github.com/hwisu/hocuspocus")
                        licenses {
                            license {
                                name.set("MIT License")
                                url.set("https://github.com/hwisu/hocuspocus/blob/main/LICENSE.md")
                                distribution.set("repo")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://github.com/hwisu/hocuspocus.git")
                            developerConnection.set("scm:git:ssh://git@github.com/hwisu/hocuspocus.git")
                            url.set("https://github.com/hwisu/hocuspocus")
                        }
                    }
                }
            }
        }
    }
}
