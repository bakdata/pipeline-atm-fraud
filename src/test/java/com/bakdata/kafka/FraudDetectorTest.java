package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.fluent_kafka_streams_tests.junitjupiter.TestTopologyExtension;
import com.bakdata.kafka.streams.ConfiguredStreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import com.bakdata.kafka.streams.TestTopologyFactory;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class FraudDetectorTest {

    private static final String INPUT_TOPIC = "INPUT";
    private static final String OUTPUT_TOPIC = "OUTPUT";

    @RegisterExtension
    final TestTopologyExtension<String, JoinedTransaction> testTopology =
            TestTopologyFactory.withSchemaRegistry().createTopologyExtension(createApp());

    private static ConfiguredStreamsApp<FraudDetector> createApp() {
        final StreamsTopicConfig topicConfig = StreamsTopicConfig.builder()
                .inputTopics(List.of(INPUT_TOPIC))
                .outputTopic(OUTPUT_TOPIC)
                .build();
        final StreamsAppConfiguration appConfig = new StreamsAppConfiguration(topicConfig);
        return new ConfiguredStreamsApp<>(new FraudDetector(), appConfig);
    }

    @Test
    void shouldFindFraudulent() {
        final Map<String, Transaction> transactions = TransactionBuilder.buildTestTransactionsMap();

        final JoinedTransaction genuineJoinedTransaction = JoinedTransaction
                .newBuilder()
                .setTransaction1(transactions.get("03"))
                .setTransaction2(transactions.get("02"))
                .build();
        this.testTopology.input().add("", genuineJoinedTransaction);

        final JoinedTransaction fraudulentJoinedTransaction = JoinedTransaction
                .newBuilder()
                .setTransaction1(transactions.get("X05"))
                .setTransaction2(transactions.get("02"))
                .build();
        this.testTopology.input().add("", fraudulentJoinedTransaction);

        final List<ProducerRecord<String, JoinedTransaction>> output = this.testTopology.streamOutput()
                .withValueType(JoinedTransaction.class)
                .toList();

        assertThat(output)
                .hasSize(1)
                .allSatisfy(record ->
                        assertThat(record.value()).isEqualTo(
                                JoinedTransaction
                                        .newBuilder()
                                        .setTransaction1(transactions.get("X05"))
                                        .setTransaction2(transactions.get("02"))
                                        .build()
                        ));
    }
}
