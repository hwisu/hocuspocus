dependencies {
    api(project(":hocuspocus-core"))
    implementation(libs.kotlinx.coroutines.jdk8)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(project(":hocuspocus-yks"))
}
