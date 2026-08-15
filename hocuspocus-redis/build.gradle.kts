dependencies {
    api(project(":hocuspocus-core"))
    implementation(platform(libs.netty.bom))
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.coroutines.jdk8)

    testImplementation(project(":hocuspocus-yks"))
}
