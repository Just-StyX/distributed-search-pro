import com.google.protobuf.gradle.*
import org.gradle.api.file.FileCollection
import org.gradle.api.provider.Property
import org.gradle.api.tasks.*
import org.gradle.process.ExecOperations
import javax.inject.Inject
import kotlin.concurrent.thread

plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
//    java
    application
    id("com.google.protobuf") version "0.9.4" // For defining Raft messages
}

repositories {
    mavenCentral()
}

dependencies {
    /// 1. gRPC for ultra-fast networking
    implementation("io.grpc:grpc-netty-shaded:1.62.2")
    implementation("io.grpc:grpc-protobuf:1.62.2")
    implementation("io.grpc:grpc-stub:1.62.2")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    // 2. Jackson for JSON logs
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")

    // 3. Your TST logic (Import your previous code here)
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

//application {
//    mainClass = "org.example.RaftLauncher"
//}

tasks.named<Test>("test") {
    useJUnitPlatform()
}



abstract class RaftClusterTask @Inject constructor(
    private val execOps: ExecOperations
) : DefaultTask() {

    @get:InputFiles
    abstract val clusterClasspath: Property<FileCollection>

    @get:Input
    abstract val mainClassName: Property<String>

    @TaskAction
    fun launch() {
        val nodes = listOf(
            "node1 50051 node2:localhost:50052,node3:localhost:50053",
            "node2 50052 node1:localhost:50051,node3:localhost:50053",
            "node3 50053 node1:localhost:50051,node2:localhost:50052"
        )

        val cp = clusterClasspath.get()
        val mc = mainClassName.get()

        nodes.forEach { nodeArgs ->
            thread(isDaemon = true) {
                execOps.javaexec {
                    mainClass.set(mc)
                    classpath = cp
                    args = nodeArgs.split(" ")
                    standardOutput = System.out
                    errorOutput = System.err
                }
            }
        }

        println(">>> Raft Cluster is starting. Press Ctrl+C to terminate.")
        while (true) { Thread.sleep(1000) }
    }
}

// Registering the task correctly
tasks.register<RaftClusterTask>("launchCluster") {
    group = "raft"
    clusterClasspath.set(project.sourceSets.main.get().runtimeClasspath)
    mainClassName.set("org.example.RaftLauncher")
}

// Register a simple task to run the Test Client
tasks.register<JavaExec>("runTestClient") {
    group = "raft"
    description = "Runs the RaftTestClient to send a boost and verify replication."

    mainClass.set("org.example.RaftTestClient")
    classpath = sourceSets.main.get().runtimeClasspath

    // Standard I/O so you see the "✅ Boost confirmed" messages
    standardInput = System.`in`
    standardOutput = System.out
    errorOutput = System.err
}



protobuf {
    protoc {
        // Download the protoc compiler
        artifact = "com.google.protobuf:protoc:3.25.1"
    }
    plugins {
        // Define the gRPC plugin
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.62.2"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                // Apply the "grpc" plugin to the task
                id("grpc") { }
            }
        }
    }
}