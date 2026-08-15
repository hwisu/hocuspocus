plugins {
    alias(libs.plugins.jmh)
    application
}

dependencies {
    implementation(platform(libs.netty.bom))
    implementation(project(":hocuspocus-yks"))
    implementation(project(":hocuspocus-ktor"))
    implementation(project(":hocuspocus-redis"))
    implementation(project(":hocuspocus-storage-sqlite"))
    implementation(libs.ktor.server.netty)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass.set("ai.hocuspocus.benchmark.WebSocketBenchmarkServer")
}

jmh {
    val quick = providers.gradleProperty("jmhQuick").isPresent
    jmhVersion.set(libs.versions.jmh.get())
    benchmarkMode.set(listOf("avgt"))
    warmupIterations.set(if (quick) 1 else 3)
    iterations.set(if (quick) 2 else 5)
    fork.set(1)
    timeOnIteration.set(if (quick) "250ms" else "1s")
    warmup.set(if (quick) "250ms" else "1s")
    timeUnit.set("us")
    profilers.set(listOf("gc"))
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
    humanOutputFile.set(layout.buildDirectory.file("reports/jmh/human.txt"))
    failOnError.set(true)
    providers.gradleProperty("jmhInclude").orNull?.let { includes.set(listOf(it)) }
    providers.gradleProperty("jmhRecipients").orNull?.let { value ->
        benchmarkParameters.put(
            "recipients",
            objects.listProperty(String::class.java).value(listOf(value)),
        )
    }
    providers.gradleProperty("jmhUpdateBytes").orNull?.let { value ->
        benchmarkParameters.put(
            "updateBytes",
            objects.listProperty(String::class.java).value(listOf(value)),
        )
    }
    if (providers.gradleProperty("jmhJfr").isPresent) {
        val recording = layout.buildDirectory.file("reports/jmh/profile.jfr").get().asFile.absolutePath
        jvmArgsAppend.set(
            listOf(
                "-XX:StartFlightRecording=filename=$recording,settings=profile,dumponexit=true",
            ),
        )
    }
}

tasks.register<JavaExec>("performanceSmoke") {
    group = "verification"
    description = "Runs bounded fanout, update-size, persistence, slow-consumer, and heap scenarios."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("ai.hocuspocus.benchmark.PerformanceSuite")
    args("--duration=5s")
}

tasks.register<JavaExec>("soak") {
    group = "verification"
    description = "Runs the collaboration soak test (30 minutes by default)."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("ai.hocuspocus.benchmark.PerformanceSuite")
    args("--soak-only", "--duration=${providers.gradleProperty("soakDuration").getOrElse("30m")}")
}
