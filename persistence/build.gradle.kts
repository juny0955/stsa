import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    `java-library`
}

group = "dev.junyoung.stsa"
version = rootProject.version
description = "Shared database schema and jOOQ types"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(27)
    }
}

repositories {
    mavenCentral()
}

sourceSets.main {
    java.srcDir("src/generated/java")
}

val tooling = sourceSets.create("tooling") {
    resources.srcDir("src/main/resources")
}

configurations.testImplementation {
    extendsFrom(configurations[tooling.implementationConfigurationName])
}

dependencies {
    api(platform(SpringBootPlugin.BOM_COORDINATES))
    api("org.jooq:jooq")
    runtimeOnly("org.postgresql:postgresql")

    add(tooling.implementationConfigurationName, platform(SpringBootPlugin.BOM_COORDINATES))
    add(tooling.implementationConfigurationName, "org.flywaydb:flyway-core")
    add(tooling.implementationConfigurationName, "org.flywaydb:flyway-database-postgresql")
    add(tooling.implementationConfigurationName, "org.jooq:jooq-codegen")
    add(tooling.runtimeOnlyConfigurationName, "org.postgresql:postgresql")

    testImplementation(tooling.output)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.register<JavaExec>("flywayMigrate") {
    group = "database"
    description = "Migrates the database configured by MIGRATION_DB_URL, USER and PASSWORD."
    classpath = tooling.runtimeClasspath
    mainClass = "dev.junyoung.stsa.persistence.tooling.MigrateDatabase"
}

tasks.register<JavaExec>("jooqCodegen") {
    group = "database"
    description = "Migrates the CODEGEN_DB database and regenerates the committed jOOQ sources."
    classpath = tooling.runtimeClasspath
    mainClass = "dev.junyoung.stsa.persistence.tooling.GenerateJooq"
    args(layout.projectDirectory.dir("src/generated/java").asFile.absolutePath)
}

tasks.withType<Test> {
    useJUnitPlatform()
    inputs.dir(layout.projectDirectory.dir("src/generated/java"))
    systemProperty("persistence.generatedSources", layout.projectDirectory.dir("src/generated/java").asFile.absolutePath)
}
