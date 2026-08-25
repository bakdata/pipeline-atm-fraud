package com.bakdata.kafka;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;

public class AccountLinker implements StreamsApp {

  public static void main(final String[] args) {
    KafkaApplication.startApplication(
        new SimpleKafkaStreamsApplication<>(AccountLinker::new),
        args
    );
  }

  private static JoinedAccountTransaction join(final JoinedTransaction joinedTransaction, final Account account) {
    final Transaction t1 = joinedTransaction.getTransaction1();
    final Transaction t2 = joinedTransaction.getTransaction2();

    final Duration timeDifference =
        Duration.between(
            t1.getTimestamp(),
            t2.getTimestamp()).abs();

    final double distance = GeoDistance.ARC.calculate(
        t1.getLocation().getLatitude(),
        t1.getLocation().getLongitude(),
        t2.getLocation().getLatitude(),
        t2.getLocation().getLongitude(),
        DistanceUnit.KILOMETERS);

    final double hoursBetween = (double) timeDifference.toSeconds() / 3600;
    final double kmhRequired = distance / hoursBetween;

    return JoinedAccountTransaction
        .newBuilder()
        .setAccountId(account.getAccountId())
        .setCustomerName(
            String.format(
                "%s %s", account.getFirstName(), account.getLastName()
            )
        )
        .setCustomerEmail(account.getEmail())
        .setCustomerPhone(account.getPhone())
        .setCustomerAddress(account.getAddress())
        .setCustomerCountry(account.getCountry())
        .setTransaction1(t1)
        .setTransaction2(t2)
        .setDistanceBetweenTxnKm(distance)
        .setMinutesDifference(timeDifference.toMinutes())
        .setKmhRequired(kmhRequired)
        .build();
  }

  @Override
  public void buildTopology(final TopologyBuilder builder) {
    final KStream<String, JoinedTransaction> transactionsKStream = builder.streamInput();
    final KStream<String, JoinedTransaction> transactionsRekeyedKStream = transactionsKStream
        .map((k, v) -> KeyValue.pair(v.getTransaction1().getAccountId(), v));

    final KStream<String, Account> accountsKStream = builder.streamInput("accounts");
    final KStream<String, Account> accountsRekeyedKStream = accountsKStream
        .map((k, v) -> KeyValue.pair(v.getAccountId(), v));

    final KTable<String, Account> accountsKTable = accountsRekeyedKStream
        .groupByKey()
        .reduce((previousValue, newValue) -> newValue);

    final KStream<String, JoinedAccountTransaction> joined = transactionsRekeyedKStream
        .join(accountsKTable, AccountLinker::join);

    joined.to(builder.getTopics().getOutputTopic());
  }

  @Override
  public String getUniqueAppId(final StreamsTopicConfig topics) {
    return "streams-explorer-accountlinker-" + topics.getOutputTopic();
  }

  @Override
  public SerdeConfig defaultSerializationConfig() {
    return new SerdeConfig(StringSerde.class, SpecificAvroSerde.class);
  }
}
