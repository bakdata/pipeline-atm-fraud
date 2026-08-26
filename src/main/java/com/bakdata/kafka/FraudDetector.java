package com.bakdata.kafka;

import com.bakdata.kafka.streams.KafkaStreamsApplication;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

public class FraudDetector implements StreamsApp {

    public static void main(final String[] args) {
        try (final KafkaStreamsApplication<FraudDetector> app = new SimpleKafkaStreamsApplication<>(
                FraudDetector::new)) {
            app.startApplication(args);
        }
    }

    private static boolean isPotentiallyFraudulentTransaction(final String k,
            final JoinedTransaction joinedTransaction) {
        final Transaction t1 = joinedTransaction.getTransaction1();
        final Transaction t2 = joinedTransaction.getTransaction2();

        return (!t1.getTransactionId().equals(t2.getTransactionId()))
                && (!t1.getAtm().equals(t2.getAtm()))
                && (t1.getTimestamp().compareTo(t2.getTimestamp()) > 0)
                && (t1.getLocation().getLatitude() != t2.getLocation().getLatitude()
                || t1.getLocation().getLongitude() != t2.getLocation().getLongitude());
    }

    @Override
    public void buildTopology(final StreamsBuilderX builder) {
        final KStreamX<String, JoinedTransaction> inputKStream = builder.streamInput();

        final KStreamX<String, JoinedTransaction> possibleFraudTransactions = inputKStream
                .filter(FraudDetector::isPotentiallyFraudulentTransaction);

        possibleFraudTransactions.toOutputTopic();
    }

    @Override
    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
        return "streams-explorer-frauddetector-" + configuration.getTopics().getOutputTopic();
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(StringSerde.class, SpecificAvroSerde.class);
    }
}
