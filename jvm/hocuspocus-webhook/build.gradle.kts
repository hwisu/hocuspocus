dependencies {
    api(project(":hocuspocus-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")

    testImplementation(project(":hocuspocus-yks"))
}
