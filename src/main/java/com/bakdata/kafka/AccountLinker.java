package com.bakdata.kafka;

import com.bakdata.kafka.streams.KafkaStreamsApplication;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.kstream.KTableX;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;

public class AccountLinker implements StreamsApp {

    public static void main(final String[] args) {
        try (final KafkaStreamsApplication<AccountLinker> app = new SimpleKafkaStreamsApplication<>(
                AccountLinker::new)) {
            app.startApplication(args);
        }
    }

    private static JoinedAccountTransaction join(final JoinedTransaction joinedTransaction, final Account account) {
        final Transaction t1 = joinedTransaction.getTransaction1();
        final Transaction t2 = joinedTransaction.getTransaction2();

        final Duration timeDifference = Duration.between(
                t1.getTimestamp(),
                t2.getTimestamp()
        ).abs();

        final double distance = DistanceUtil.calculateDistance(t1.getLocation(), t2.getLocation());

        final double hoursBetween = (double) timeDifference.toSeconds() / 3600;
        final double kmhRequired = distance / hoursBetween;

        return JoinedAccountTransaction.newBuilder()
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
    public void buildTopology(final StreamsBuilderX builder) {
        final KStreamX<String, JoinedTransaction> transactionsKStream = builder.streamInput();
        final KStreamX<String, JoinedTransaction> transactionsRekeyedKStream = transactionsKStream
                .selectKey(v -> v.getTransaction1().getAccountId());

        final KStreamX<String, Account> accountsKStream = builder.streamInput("accounts");
        final KStreamX<String, Account> accountsRekeyedKStream = accountsKStream
                .selectKey(Account::getAccountId);

        final KTableX<String, Account> accountsKTable =
                accountsRekeyedKStream.toTable(Named.as("accounts"), Materialized.as("accounts"));

        final KStreamX<String, JoinedAccountTransaction> joined = transactionsRekeyedKStream
                .join(accountsKTable, AccountLinker::join, Joined.as("joined"));

        joined.toOutputTopic();
    }

    @Override
    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
        return "streams-explorer-accountlinker-" + configuration.getTopics().getOutputTopic();
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(StringSerde.class, SpecificAvroSerde.class);
    }
}
