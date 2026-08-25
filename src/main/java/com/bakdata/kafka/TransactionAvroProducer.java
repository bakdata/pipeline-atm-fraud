package com.bakdata.kafka;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@Slf4j
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionAvroProducer implements ProducerApp {

    static final String FILE_NAME = "atm_locations.csv";
    private static final ClassLoader CLASS_LOADER = AccountProducer.class.getClassLoader();

    @Builder.Default
    private int bound = 1;
    @Builder.Default
    private int iterations = 1;

    public static void main(final String[] args) {
        KafkaApplication.startApplication(new TransactionAvroProducerApplication(), args);
    }

    @Override
    public ProducerRunnable buildRunnable(final ProducerBuilder producerBuilder) {
        return () -> {
            final TransactionFactory transactionFactory = new TransactionFactory(loadCsvData(FILE_NAME));
            try (final Producer<String, Transaction> producer = producerBuilder.createProducer()) {
                final String outputTopic = producerBuilder.getTopics().getOutputTopic();
                log.debug("Bound = {} and Iteration= {}", this.bound, this.iterations);
                log.debug("Expected amount of transactions: {}", (this.bound + 1) * this.iterations);
                log.info("Producing data into output topic <{}>...", outputTopic);
                for (int counter = 0; counter < this.iterations; counter++) {
                    final int fraudIndex = counter % this.bound;
                    Transaction oldTransaction = new Transaction();

                    for (int i = 0; i < this.bound; i++) {
                        final Transaction newRealTransaction = transactionFactory.createRealTimeTransaction();
                        producer.send(new ProducerRecord<>(outputTopic, newRealTransaction.getTransactionId(), newRealTransaction));
                        if (i == fraudIndex) {
                            oldTransaction = newRealTransaction;
                        }
                    }
                    final Transaction fraudTransaction = transactionFactory.createFraudTransaction(oldTransaction, fraudIndex);
                    producer.send(new ProducerRecord<>(outputTopic, fraudTransaction.getTransactionId(), fraudTransaction));
                    log.debug("Current iteration step: {}", counter);
                }
                producer.flush();
            }
        };
    }

    @Override
    public SerializerConfig defaultSerializationConfig() {
        return new SerializerConfig(StringSerializer.class, SpecificAvroSerializer.class);
    }

    public static List<AtmLocation> loadCsvData(final String fileName) {
        try (final InputStream inputStream = CLASS_LOADER.getResourceAsStream(fileName);
                final InputStreamReader streamReader = new InputStreamReader(Objects.requireNonNull(inputStream),
                        StandardCharsets.UTF_8);
                final CSVReader csvReader = new CSVReader(streamReader)) {

            final CsvToBean<AtmLocation> csvToBean = new CsvToBeanBuilder<AtmLocation>(csvReader)
                    .withType(AtmLocation.class)
                    .withSeparator(',')
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build();
            final List<AtmLocation> allLocations = csvToBean.parse();
            log.debug("Amount of locations information loaded from the csv file: {}", allLocations.size());
            return allLocations;
        } catch (final IOException e) {
            throw new RuntimeException("Error occurred while loading CSV file", e);
        }
    }

    @ToString(callSuper = true)
    @Getter
    @Setter
    public static class TransactionAvroProducerApplication extends KafkaProducerApplication<TransactionAvroProducer> {
        @CommandLine.Option(names = "--real-tx",
                description = "How many real transactions must be generated before a fraudulent transaction can be "
                        + "generated?")
        private int bound;
        @CommandLine.Option(names = "--iteration",
                description = "One iteration contains number of real transactions and one fraudulent transaction")
        private int iterations;

        @Override
        public TransactionAvroProducer createApp() {
            return TransactionAvroProducer.builder()
                    .bound(this.bound)
                    .iterations(this.iterations)
                    .build();
        }
    }
}
