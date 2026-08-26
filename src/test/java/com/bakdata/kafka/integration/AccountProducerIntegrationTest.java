package com.bakdata.kafka.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.Account;
import com.bakdata.kafka.AccountProducer;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.TestApplicationRunner;
import com.bakdata.kafka.producer.KafkaProducerApplication;
import com.bakdata.kafka.producer.SimpleKafkaProducerApplication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import java.time.Duration;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AccountProducerIntegrationTest {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10L);
    private static final String OUTPUT_TOPIC = "atm-fraud-accounts-topic";
    @Container
    private final KafkaContainer kafkaCluster = new KafkaContainer(DockerImageName.parse("apache/kafka")
            .withTag(AppInfoParser.getVersion()));

    private static KafkaProducerApplication<AccountProducer> setupApp() {
        final KafkaProducerApplication<AccountProducer> producerApp =
                new SimpleKafkaProducerApplication<>(AccountProducer::new);
        producerApp.setOutputTopic(OUTPUT_TOPIC);
        return producerApp;
    }

    @Test
    void shouldRunApp() {
        try (final KafkaProducerApplication<AccountProducer> accountProducer = setupApp()) {
            final TestApplicationRunner runner = TestApplicationRunner.create(this.kafkaCluster.getBootstrapServers())
                    .withSchemaRegistry()
                    .withSessionTimeout(Duration.ofSeconds(10L));
            final KafkaTestClient testClient = runner.newTestClient();
            testClient.createTopic(OUTPUT_TOPIC);
            runner.configure(accountProducer);
            accountProducer.run();
            assertThat(testClient.read()
                    .withKeyDeserializer(new StringDeserializer())
                    .withValueDeserializer(new SpecificAvroDeserializer<Account>())
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT))
                    .hasSize(999)
                    .allSatisfy(keyValue -> {
                        final String recordKey = keyValue.key();
                        final Account account = keyValue.value();
                        final String accountId = account.getAccountId();
                        final String regex = "^a([0-9]{1,3})";

                        assertThat(accountId).matches(regex);
                        assertThat(recordKey).isEqualTo(accountId);
                    });
        }
    }
}
