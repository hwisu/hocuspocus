import org.gradle.api.plugins.ExtensionAware
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.jvm.tasks.Jar
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import org.jetbrains.kotlin.gradle.dsl.abi.AbiValidationExtension
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation
import java.util.Locale
import java.util.zip.ZipFile

abstract class VerifyPublicationMetadata : DefaultTask() {
    @get:InputFile
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val pomFile: RegularFileProperty

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val publicationArtifacts: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val legalNotices: ConfigurableFileCollection

    @get:Input
    abstract val expectedRevision: Property<String>

    @TaskAction
    fun verify() {
        val pomText = pomFile.get().asFile.readText()
        listOf(
            "<name>MIT License</name>",
            "<url>https://github.com/hwisu/hocuspocus/blob/main/LICENSE.md</url>",
            "<distribution>repo</distribution>",
            "<developerConnection>scm:git:ssh://git@github.com/hwisu/hocuspocus.git</developerConnection>",
            "<tag>${expectedRevision.get()}</tag>",
        ).forEach { fragment ->
            check(fragment in pomText) {
                "Generated Maven POM is missing metadata: $fragment"
            }
        }

        val expectedNotices = legalNotices.files.associateBy({ it.name }, { it.readBytes() })
        publicationArtifacts.files.forEach { artifact ->
            ZipFile(artifact).use { archive ->
                expectedNotices.forEach { (name, expectedBytes) ->
                    val path = "META-INF/$name"
                    val entry = checkNotNull(archive.getEntry(path)) {
                        "${artifact.name} is missing $path"
                    }
                    val actualBytes = archive.getInputStream(entry).use { it.readBytes() }
                    check(actualBytes.contentEquals(expectedBytes)) {
                        "${artifact.name} contains stale $path"
                    }
                }
            }
        }
    }
}

abstract class ValidateRelease : DefaultTask() {
    @get:Input
    abstract val releaseVersion: Property<String>

    @get:Input
    abstract val buildRevision: Property<String>

    @TaskAction
    fun validate() {
        val version = releaseVersion.get()
        val semanticVersionPattern = Regex(
            """^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$""",
        )
        check(semanticVersionPattern.matches(version)) {
            "Remote publication requires -PreleaseVersion=<SemVer>; got '$version'"
        }
        val revision = buildRevision.get()
        check(revision.matches(Regex("^[0-9a-f]{40}$"))) {
            "Remote publication requires -PbuildRevision=<40-character Git SHA>; got '$revision'"
        }
    }
}

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.jmh) apply false
    `maven-publish`
}

private fun nettyNativeRuntimeDependency(
    osName: String,
    architecture: String,
    nettyVersion: String,
): String? {
    val normalizedArchitecture = architecture.lowercase(Locale.ROOT)
    return when {
        osName.contains("mac", ignoreCase = true) -> {
            val classifier = when (normalizedArchitecture) {
                "aarch64", "arm64" -> "osx-aarch_64"
                "amd64", "x86_64" -> "osx-x86_64"
                else -> return null
            }
            "io.netty:netty-transport-native-kqueue:$nettyVersion:$classifier"
        }
        osName.contains("linux", ignoreCase = true) -> {
            val classifier = when (normalizedArchitecture) {
                "aarch64", "arm64" -> "linux-aarch_64"
                "amd64", "x86_64" -> "linux-x86_64"
                else -> return null
            }
            "io.netty:netty-transport-native-epoll:$nettyVersion:$classifier"
        }
        else -> null
    }
}

val nettyNativeRuntimeDependency = nettyNativeRuntimeDependency(
    providers.systemProperty("os.name").get(),
    providers.systemProperty("os.arch").get(),
    libs.versions.netty.get(),
)

allprojects {
    group = "ai.hocuspocus"
    version = providers.gradleProperty("releaseVersion").getOrElse("0.1.4-SNAPSHOT")
}

val buildRevision = providers.gradleProperty("buildRevision").getOrElse("uncommitted")
val legalNoticeFiles = listOf("LICENSE.md", "THIRD_PARTY_NOTICES")
val nonPublishedProjects = setOf("hocuspocus-benchmark", "hocuspocus-ktor-example")
val validateRelease = tasks.register<ValidateRelease>("validateRelease") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Validates the version and source revision used for remote publication."
    releaseVersion.set(providers.gradleProperty("releaseVersion").orElse("<missing>"))
    buildRevision.set(providers.gradleProperty("buildRevision").orElse("uncommitted"))
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
        if (project.name !in nonPublishedProjects) {
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

    tasks.withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }

    tasks.withType<Jar>().configureEach {
        manifest.attributes(
            "Implementation-Version" to project.version.toString(),
            "Hocuspocus-Revision" to buildRevision,
        )
        from(legalNoticeFiles.map(rootProject::file)) {
            into("META-INF")
        }
    }

    dependencies {
        "testImplementation"(kotlin("test"))
        "testImplementation"(rootProject.libs.junit.jupiter.get().toString())
        if (name in nonPublishedProjects) {
            nettyNativeRuntimeDependency?.let { "runtimeOnly"(it) }
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    if (name !in nonPublishedProjects) {
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
                        developers {
                            developer {
                                id.set("hwisu")
                                name.set("hwisu")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://github.com/hwisu/hocuspocus.git")
                            developerConnection.set("scm:git:ssh://git@github.com/hwisu/hocuspocus.git")
                            url.set("https://github.com/hwisu/hocuspocus")
                            tag.set(buildRevision)
                        }
                    }
                }
            }
            repositories {
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/hwisu/hocuspocus")
                    credentials {
                        username = providers.environmentVariable("GITHUB_ACTOR").orNull
                        password = providers.environmentVariable("GITHUB_TOKEN").orNull
                    }
                }
            }
        }

        val publicationMetadataTest = tasks.register<VerifyPublicationMetadata>("publicationMetadataTest") {
            description = "Verifies Maven metadata and legal notices in published artifacts."
            group = LifecycleBasePlugin.VERIFICATION_GROUP
            dependsOn("generatePomFileForMavenJavaPublication", "jar", "sourcesJar", "javadocJar")
            pomFile.set(layout.buildDirectory.file("publications/mavenJava/pom-default.xml"))
            publicationArtifacts.from(
                listOf("jar", "sourcesJar", "javadocJar").map { taskName ->
                    tasks.named<Jar>(taskName).flatMap { it.archiveFile }
                },
            )
            legalNotices.from(legalNoticeFiles.map(rootProject::file))
            expectedRevision.set(buildRevision)
        }
        tasks.named("check") {
            dependsOn(publicationMetadataTest)
        }

        tasks.withType<org.gradle.api.publish.maven.tasks.PublishToMavenRepository>().configureEach {
            dependsOn(validateRelease)
        }
    }
}

val consumerSmokeTest = tasks.register<GradleBuild>("consumerSmokeTest") {
    description = "Publishes the JVM modules and runs the baseline standalone consumer."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    dependsOn(
        subprojects
            .filter { it.name !in nonPublishedProjects }
            .map { "${it.path}:publishToMavenLocal" },
    )
    dir = file("consumer-smoke")
    tasks = listOf("clean", "run")
    startParameter.projectProperties = mapOf(
        "consumerKotlinVersion" to "2.2.20",
        "hocuspocusVersion" to project.version.toString(),
        "useMavenLocal" to "true",
    )
}

val consumerKotlinCompatibilityTest = tasks.register<Exec>("consumerKotlinCompatibilityTest") {
    description = "Consumes the published JVM modules with Norric's Kotlin compiler baseline."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    dependsOn(
        subprojects
            .filter { it.name !in nonPublishedProjects }
            .map { "${it.path}:publishToMavenLocal" },
    )
    mustRunAfter(consumerSmokeTest)
    workingDir = file("consumer-smoke")
    commandLine(
        rootProject.file("gradlew").absolutePath,
        "clean",
        "run",
        "--no-daemon",
        "-PconsumerKotlinVersion=2.3.21",
        "-PhocuspocusVersion=${project.version}",
        "-PuseMavenLocal=true",
    )
}
