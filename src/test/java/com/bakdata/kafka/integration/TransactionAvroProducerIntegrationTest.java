package com.bakdata.kafka.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.TestApplicationRunner;
import com.bakdata.kafka.Transaction;
import com.bakdata.kafka.TransactionAvroProducer.TransactionAvroProducerApplication;
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
class TransactionAvroProducerIntegrationTest {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10L);
    private static final int BOUND = 4;
    private static final int ITERATIONS = 5;
    private static final int EXPECTED = (BOUND + 1) * ITERATIONS;
    private static final int KEY_SIZE = 36;
    private static final int FRAUD_KEY_SIZE = 39;
    private static final String OUTPUT_TOPIC = "atm-fraud-incoming-transactions-topic";
    @Container
    private final KafkaContainer kafkaCluster = new KafkaContainer(DockerImageName.parse("apache/kafka")
            .withTag(AppInfoParser.getVersion()));

    private static TransactionAvroProducerApplication setupApp() {
        final TransactionAvroProducerApplication producerApp = new TransactionAvroProducerApplication();
        producerApp.setBound(BOUND);
        producerApp.setIterations(ITERATIONS);
        producerApp.setOutputTopic(OUTPUT_TOPIC);
        return producerApp;
    }

    @Test
    void shouldRunApp() {
        try (final TransactionAvroProducerApplication producerApp = setupApp()) {
            final TestApplicationRunner runner = TestApplicationRunner.create(this.kafkaCluster.getBootstrapServers())
                    .withSchemaRegistry()
                    .withSessionTimeout(Duration.ofSeconds(10L));
            final KafkaTestClient testClient = runner.newTestClient();
            testClient.createTopic(OUTPUT_TOPIC);
            runner.configure(producerApp);
            producerApp.run();
            assertThat(testClient.read()
                    .withKeyDeserializer(new StringDeserializer())
                    .withValueDeserializer(new SpecificAvroDeserializer<Transaction>())
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT))
                    .hasSize(EXPECTED)
                    .allSatisfy(keyValue -> {
                        final String recordKey = keyValue.key();
                        final Transaction tx = keyValue.value();
                        final String txID = tx.getTransactionId();
                        final String fraudPrefix = "xxx";
                        final String regex = "^a([0-9]{1,3})";

                        assertThat(recordKey.length()).isIn(KEY_SIZE, FRAUD_KEY_SIZE);
                        assertThat(recordKey).isEqualTo(txID);
                        if (recordKey.length() > KEY_SIZE) {
                            assertThat(recordKey).contains(fraudPrefix);
                        }
                        assertThat(tx.getAccountId()).matches(regex);
                    });
        }
    }
}
