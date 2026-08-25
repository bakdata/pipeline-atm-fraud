package com.bakdata.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
@Setter
public class AccountProducer implements ProducerApp {
    private static final String FILE_NAME = "accounts.json";

    public static void main(final String[] args) {
        KafkaApplication.startApplication(
                new SimpleKafkaProducerApplication<>(AccountProducer::new),
                args
        );
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
                producer.flush();
            }
        };
    }

    @Override
    public SerializerConfig defaultSerializationConfig() {
        return new SerializerConfig(StringSerializer.class, SpecificAvroSerializer.class);
    }

    public static List<Account> loadJSON(final String fileName) {
        final ClassLoader classLoader = AccountProducer.class.getClassLoader();
        final ObjectMapper objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        try (final InputStream inputStream = classLoader.getResourceAsStream(fileName)) {
            return objectMapper.readValue(inputStream, new TypeReference<>() {});
        } catch (final IOException e) {
            throw new RuntimeException("Error occurred while reading the JSON file.", e);
        }
    }
}
