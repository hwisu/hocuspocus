dependencies {
    api(project(":hocuspocus-core"))
    implementation(libs.kotlinx.serialization.json)

    testImplementation(project(":hocuspocus-yks"))
}
