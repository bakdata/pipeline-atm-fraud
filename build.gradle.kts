description = "ATM fraud detection with Common Kafka Streams"
plugins {
    java
    idea
    id("com.bakdata.release") version "1.7.1"
    id("com.bakdata.sonar") version "1.7.1"
    id("com.bakdata.sonatype") version "1.7.1"
    id("com.bakdata.jib") version "1.7.1"
    id("io.freefair.lombok") version "8.11"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.0"
}

group = "com.bakdata.kafka"

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Salomon Popp")
            id.set("disrupted")
        }
    }
}

tasks {
    compileJava {
        options.encoding = "UTF-8"
    }
    compileTestJava {
        options.encoding = "UTF-8"
    }
    test {
        maxParallelForks = 1
        useJUnitPlatform()
    }
}

dependencies {
    val streamsBootstrapVersion = "3.1.0"
    implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli", version = streamsBootstrapVersion)
    val confluentVersion = "7.6.0"
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    val log4jVersion = "2.24.2"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    implementation(group = "org.elasticsearch", name = "elasticsearch", version = "7.17.26")
    implementation(group = "com.opencsv", name = "opencsv", version = "5.9")
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.18.2")

    val junitVersion = "5.11.3"
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    val assertJVersion = "3.26.3"
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)

    testImplementation(group = "com.bakdata.kafka", name = "streams-bootstrap-test", version = streamsBootstrapVersion)
    val fluentKafkaVersion = "2.14.0"
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = fluentKafkaVersion
    )
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = fluentKafkaVersion
    )
    val kafkaJunitVersion = "3.6.0"
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaJunitVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
}

jibImage {
    name.set(providers.systemProperty("jib.container.mainClass").map { mainClass ->
        when (mainClass) {
            "com.bakdata.kafka.TransactionAvroProducer" -> "atm-demo-transactionavroproducer"
            "com.bakdata.kafka.AccountProducer" -> "atm-demo-accountproducer"
            "com.bakdata.kafka.TransactionJoiner" -> "atm-demo-transactionjoiner"
            "com.bakdata.kafka.FraudDetector" -> "atm-demo-frauddetector"
            "com.bakdata.kafka.AccountLinker" -> "atm-demo-accountlinker"
            else -> project.name
        }
    }.orElse(project.name))
}

jib {
    from {
        image = "eclipse-temurin:21.0.5_11-jre"
    }
}
