package com.bakdata.kafka;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

public class TransactionJoiner implements StreamsApp {

    public static void main(final String[] args) {
        KafkaApplication.startApplication(
                new SimpleKafkaStreamsApplication<>(TransactionJoiner::new),
                args
        );
    }

    @Override
    public void buildTopology(final TopologyBuilder builder) {
        final KStream<String, Transaction> input = builder.streamInput();
        final KStream<String, Transaction> mapped = input
                .map((k, v) -> KeyValue.pair(v.getAccountId(), v));

        final KStream<String, JoinedTransaction> joined = mapped
                .join(mapped,
                        (t1, t2) -> JoinedTransaction
                                .newBuilder()
                                .setTransaction1(t1)
                                .setTransaction2(t2)
                                .build(),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)).before(Duration.ZERO));

        joined.to(builder.getTopics().getOutputTopic());
    }

    @Override
    public String getUniqueAppId(final StreamsTopicConfig topics) {
        return "streams-explorer-transactionjoiner-" + topics.getOutputTopic();
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(StringSerde.class, SpecificAvroSerde.class);
    }
}
