package com.bakdata.kafka;

import com.bakdata.kafka.streams.KafkaStreamsApplication;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;

public class TransactionJoiner implements StreamsApp {

    public static void main(final String[] args) {
        try (final KafkaStreamsApplication<TransactionJoiner> app = new SimpleKafkaStreamsApplication<>(
                TransactionJoiner::new)) {
            app.startApplication(args);
        }
    }

    @Override
    public void buildTopology(final StreamsBuilderX builder) {
        final KStreamX<String, Transaction> input = builder.streamInput();
        final KStreamX<String, Transaction> mapped = input
                .selectKey(Transaction::getAccountId)
                .repartition(Repartitioned.as("accounts"));

        final KStreamX<String, JoinedTransaction> joined = mapped
                .join(mapped,
                        (t1, t2) -> JoinedTransaction
                                .newBuilder()
                                .setTransaction1(t1)
                                .setTransaction2(t2)
                                .build(),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)).before(Duration.ZERO),
                        StreamJoined.as("joined"));

        joined.toOutputTopic();
    }

    @Override
    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
        return "streams-explorer-transactionjoiner-" + configuration.getTopics().getOutputTopic();
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(StringSerde.class, SpecificAvroSerde.class);
    }
}
