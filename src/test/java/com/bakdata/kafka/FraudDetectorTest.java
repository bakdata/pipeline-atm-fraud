package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class FraudDetectorTest {

  private static final String INPUT_TOPIC = "INPUT";
  private static final String OUTPUT_TOPIC = "OUTPUT";
  private final ConfiguredStreamsApp<FraudDetector> app = createApp();

  @RegisterExtension
  final TestTopologyExtension<String, JoinedTransaction> testTopology =
      TestTopologyFactory.createTopologyExtensionWithSchemaRegistry(this.app);

  private static ConfiguredStreamsApp<FraudDetector> createApp() {
    final StreamsTopicConfig topicConfig = StreamsTopicConfig.builder()
        .inputTopics(List.of(INPUT_TOPIC))
        .outputTopic(OUTPUT_TOPIC)
        .build();
    final AppConfiguration<StreamsTopicConfig> appConfig = new AppConfiguration<>(topicConfig);
    return new ConfiguredStreamsApp<>(new FraudDetector(), appConfig);
  }

  @AfterEach
  void tearDown() {
    this.app.close();
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

    final List<ProducerRecord<String, JoinedTransaction>> output = this.testTopology
        .streamOutput(this.app.getTopics().getOutputTopic())
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
