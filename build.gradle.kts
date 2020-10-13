import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.jvm.tasks.Jar

plugins {
    kotlin("jvm") version "1.4.10"
    application
}
group = "me.ueli"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}
dependencies {
    testImplementation(kotlin("test-junit5"))
    implementation(group = "io.github.rybalkinsd", name = "kohttp-jackson", version = "0.12.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.11.+")
    compile("io.github.cdimascio:dotenv-kotlin:6.2.1")
}
tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}
application {
    mainClassName = "MainKt"
}

tasks {
    register<Jar>("fatJar") {
        group = "build"
        manifest {
            attributes["Implementation-Title"] = "Build fat jar"
            attributes["Implementation-Version"] = archiveVersion
            attributes["Main-Class"] = application.mainClassName
        }
        archiveBaseName.set("${project.name}-fat")
        from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
        with(jar.get() as CopySpec)
    }
}
