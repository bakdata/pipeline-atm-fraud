description = "ATM fraud detection with streams-bootstrap"
plugins {
    java
    alias(libs.plugins.release)
    alias(libs.plugins.sonar)
    alias(libs.plugins.sonatype)
    alias(libs.plugins.lombok)
    alias(libs.plugins.jib)
    alias(libs.plugins.avro)
}

group = "com.bakdata.kafka"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

publication {
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
    implementation(platform(libs.streamsBootstrap.bom))
    implementation(libs.streamsBootstrap.cli)
    implementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation(libs.log4j.slf4j2)
    implementation(libs.opencsv)
    implementation(libs.jackson.databind)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)

    testImplementation(libs.streamsBootstrap.cli.test)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
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
