dependencies {
    api(project(":hocuspocus-core"))
    implementation(platform("io.netty:netty-bom:4.2.15.Final"))
    implementation("io.lettuce:lettuce-core:7.6.0.RELEASE")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.11.0")

    testImplementation(project(":hocuspocus-yks"))
}
