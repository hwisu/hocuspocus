dependencies {
    api(project(":hocuspocus-core"))
    implementation(libs.aws.s3)
    implementation(libs.kotlinx.coroutines.jdk8)
}
