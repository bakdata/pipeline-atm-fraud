package com.bakdata.kafka;

import com.bakdata.kafka.producer.KafkaProducerApplication;
import com.bakdata.kafka.producer.ProducerApp;
import com.bakdata.kafka.producer.ProducerBuilder;
import com.bakdata.kafka.producer.ProducerRunnable;
import com.bakdata.kafka.producer.SerializerConfig;
import com.bakdata.kafka.producer.SimpleKafkaProducerApplication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

public class AccountProducer implements ProducerApp {
    private static final String FILE_NAME = "accounts.json";

    public static void main(final String[] args) {
        try (final KafkaProducerApplication<AccountProducer> app = new SimpleKafkaProducerApplication<>(
                AccountProducer::new)) {
            app.startApplication(args);
        }
    }

    public static List<Account> loadJSON(final String fileName) {
        final ClassLoader classLoader = AccountProducer.class.getClassLoader();
        final ObjectMapper objectMapper = JsonMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .build();
        try (final InputStream inputStream = classLoader.getResourceAsStream(fileName)) {
            return objectMapper.readValue(inputStream, new TypeReference<>() {});
        } catch (final IOException e) {
            throw new UncheckedIOException("Error occurred while reading the JSON file.", e);
        }
    }

    @Override
    public ProducerRunnable buildRunnable(final ProducerBuilder producerBuilder) {
        return () -> {
            final List<Account> accounts = loadJSON(FILE_NAME);
            try (final Producer<String, Account> producer = producerBuilder.createProducer()) {
                final String outputTopic = producerBuilder.getTopics().getOutputTopic();
                for (final Account accountObj : accounts) {
                    producer.send(new ProducerRecord<>(outputTopic, accountObj.getAccountId(), accountObj));
                }
            }
        };
    }

    @Override
    public SerializerConfig defaultSerializationConfig() {
        return new SerializerConfig(StringSerializer.class, SpecificAvroSerializer.class);
    }
}
