package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import java.util.List;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AccountLinkerTest {

  private static final String INPUT_TOPIC = "INPUT";
  private static final String OUTPUT_TOPIC = "OUTPUT";
  private static final String ACCOUNTS_TOPIC = "ACCOUNTS";
  private static final Map<String, List<String>> LABELED_INPUT_TOPICS = Map.of(
      "accounts", List.of(ACCOUNTS_TOPIC)
  );

  private final ConfiguredStreamsApp<AccountLinker> app = createApp();

  @RegisterExtension
  final TestTopologyExtension<String, SpecificRecord> testTopology =
      TestTopologyFactory.createTopologyExtensionWithSchemaRegistry(this.app);

  private static ConfiguredStreamsApp<AccountLinker> createApp() {
    final StreamsTopicConfig topicConfig = StreamsTopicConfig.builder()
        .inputTopics(List.of(INPUT_TOPIC))
        .labeledInputTopics(LABELED_INPUT_TOPICS)
        .outputTopic(OUTPUT_TOPIC)
        .build();
    final AppConfiguration<StreamsTopicConfig> appConfig = new AppConfiguration<>(topicConfig);
    return new ConfiguredStreamsApp<>(new AccountLinker(), appConfig);
  }

  @AfterEach
  void tearDown() {
    this.app.close();
  }

  @Test
  void shouldCreateJoinedAccountTransaction() {
    final Account account = Account.newBuilder()
        .setAccountId("ac_03")
        .setFirstName("Foo")
        .setLastName("Bar")
        .setEmail("foo@bar.io")
        .setPhone("+123456789")
        .setAddress("123 Town Road")
        .setCountry("Atlantis")
        .build();

    this.testTopology
        .input(ACCOUNTS_TOPIC)
        .add("", account);

    final Map<String, Transaction> transactions = TransactionBuilder.buildTestTransactionsMap();

    final JoinedTransaction joinedTransaction = JoinedTransaction
        .newBuilder()
        .setTransaction1(transactions.get("04"))
        .setTransaction2(transactions.get("X05"))
        .build();

    this.testTopology.input(INPUT_TOPIC).add("joinedTransaction", joinedTransaction);

    final List<ProducerRecord<String, JoinedAccountTransaction>> output = this.testTopology
        .streamOutput(this.app.getTopics().getOutputTopic())
        .withValueType(JoinedAccountTransaction.class)
        .toList();

    assertThat(output)
        .hasSize(1)
        .allSatisfy(record -> {
          assertThat(record.value().getAccountId()).isEqualTo("ac_03");
          assertThat(record.value().getMinutesDifference()).isEqualTo(3);
        });
  }
}
