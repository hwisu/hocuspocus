pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()

        val githubUser = providers.gradleProperty("gpr.user")
            .orElse(providers.environmentVariable("GITHUB_ACTOR"))
            .orNull
        val githubToken = providers.gradleProperty("gpr.key")
            .orElse(providers.environmentVariable("GITHUB_TOKEN"))
            .orNull
        if (githubUser != null && githubToken != null) {
            maven {
                name = "YksGitHubPackages"
                url = uri("https://maven.pkg.github.com/hwisu/yks")
                credentials {
                    username = githubUser
                    password = githubToken
                }
                content { includeGroup("dev.yks") }
            }
        }
    }
}

rootProject.name = "hocuspocus-jvm"

include(":hocuspocus-protocol")
include(":hocuspocus-core")
include(":hocuspocus-yks")
include(":hocuspocus-ktor")
include(":hocuspocus-redis")
include(":hocuspocus-throttle")
include(":hocuspocus-metrics")
include(":hocuspocus-webhook")
include(":hocuspocus-storage-s3")
include(":hocuspocus-storage-sqlite")
include(":hocuspocus-benchmark")
include(":hocuspocus-ktor-example")

providers.gradleProperty("yks.localPath").orNull?.let { localPath ->
    val yksBuild = file(localPath)
    require(yksBuild.resolve("settings.gradle.kts").isFile) {
        "yks.localPath does not point to a Gradle build: $localPath"
    }
    includeBuild(yksBuild)
}
