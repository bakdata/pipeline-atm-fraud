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

class TransactionJoinerTest {

    private static final String INPUT_TOPIC = "INPUT";
    private static final String OUTPUT_TOPIC = "OUTPUT";

    @RegisterExtension
    final TestTopologyExtension<String, Transaction> testTopology =
            TestTopologyFactory.withSchemaRegistry().createTopologyExtension(createApp());

    private static ConfiguredStreamsApp<TransactionJoiner> createApp() {
        final StreamsTopicConfig topicConfig = StreamsTopicConfig.builder()
                .inputTopics(List.of(INPUT_TOPIC))
                .outputTopic(OUTPUT_TOPIC)
                .build();
        final StreamsAppConfiguration appConfig = new StreamsAppConfiguration(topicConfig);
        return new ConfiguredStreamsApp<>(new TransactionJoiner(), appConfig);
    }

    @Test
    void shouldJoinTransactions() {
        final Map<String, Transaction> transactions = TransactionBuilder.buildTestTransactionsMap();

        for (final Map.Entry<String, Transaction> entry : transactions.entrySet()) {
            this.testTopology.input()
                    .add(entry.getKey(), entry.getValue());
        }

        final List<ProducerRecord<String, JoinedTransaction>> output = this.testTopology.streamOutput()
                .withValueType(JoinedTransaction.class)
                .toList();

        assertThat(output)
                .anySatisfy(record ->
                        assertThat(record.value()).isEqualTo(
                                JoinedTransaction
                                        .newBuilder()
                                        .setTransaction1(transactions.get("04"))
                                        .setTransaction2(transactions.get("X05"))
                                        .build()
                        ));
    }
}
