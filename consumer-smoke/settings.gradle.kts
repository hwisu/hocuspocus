pluginManagement {
    plugins {
        id("org.jetbrains.kotlin.jvm") version providers.gradleProperty("consumerKotlinVersion")
            .getOrElse("2.2.20")
    }
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        if (providers.gradleProperty("useMavenLocal").orNull == "true") {
            mavenLocal()
        }

        val githubUser = providers.gradleProperty("gpr.user")
            .orElse(providers.environmentVariable("GITHUB_ACTOR"))
            .orNull
        val githubToken = providers.gradleProperty("gpr.key")
            .orElse(providers.environmentVariable("GITHUB_TOKEN"))
            .orNull
        listOfNotNull(
            providers.gradleProperty("hocuspocusRepositoryUrl").orNull,
            providers.gradleProperty("yksRepositoryUrl").orNull,
        ).forEachIndexed { index, repositoryUrl ->
            maven {
                name = "GitHubPackages$index"
                url = uri(repositoryUrl)
                credentials {
                    username = githubUser
                    password = githubToken
                }
            }
        }
    }
}

rootProject.name = "hocuspocus-consumer-smoke"
