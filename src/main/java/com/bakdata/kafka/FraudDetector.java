package com.bakdata.kafka;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.kstream.KStream;

public class FraudDetector implements StreamsApp {

  public static void main(final String[] args) {
    KafkaApplication.startApplication(
        new SimpleKafkaStreamsApplication<>(FraudDetector::new),
        args
    );
  }

  private static boolean isPotentiallyFraudulentTransaction(final String k, final JoinedTransaction joinedTransaction) {
    final Transaction t1 = joinedTransaction.getTransaction1();
    final Transaction t2 = joinedTransaction.getTransaction2();

    return (!t1.getTransactionId().equals(t2.getTransactionId()))
        && (!t1.getAtm().equals(t2.getAtm()))
        && (t1.getTimestamp().compareTo(t2.getTimestamp()) > 0)
        && (t1.getLocation().getLatitude() != t2.getLocation().getLatitude()
        || t1.getLocation().getLongitude() != t2.getLocation().getLongitude());
  }

  @Override
  public void buildTopology(final TopologyBuilder builder) {
    final KStream<String, JoinedTransaction> inputKStream = builder.streamInput();

    final KStream<String, JoinedTransaction> possibleFraudTransactions = inputKStream
        .filter(FraudDetector::isPotentiallyFraudulentTransaction);

    possibleFraudTransactions.to(builder.getTopics().getOutputTopic());
  }

  @Override
  public String getUniqueAppId(final StreamsTopicConfig topics) {
    return "streams-explorer-frauddetector-" + topics.getOutputTopic();
  }

  @Override
  public SerdeConfig defaultSerializationConfig() {
    return new SerdeConfig(StringSerde.class, SpecificAvroSerde.class);
  }
}
