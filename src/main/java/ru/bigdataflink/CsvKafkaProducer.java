package ru.bigdataflink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class CsvKafkaProducer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private CsvKafkaProducer() {
    }

    public static void main(String[] args) throws Exception {
        String inputDir = envOrDefault("INPUT_DIR", "исходные данные");
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");
        String topic = envOrDefault("KAFKA_TOPIC", "pet-sales");
        long sendDelayMs = longEnvOrDefault("SEND_DELAY_MS", 0L);

        List<Path> csvFiles = csvFiles(Path.of(inputDir));
        if (csvFiles.isEmpty()) {
            throw new IllegalStateException("CSV files were not found in " + inputDir);
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "csv-json-producer");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) Duration.ofMinutes(2).toMillis());

        long sent = 0L;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (Path csvFile : csvFiles) {
                sent += sendFile(producer, topic, csvFile, sendDelayMs);
            }
            producer.flush();
        }

        System.out.printf("Sent %d JSON messages to Kafka topic '%s'%n", sent, topic);
    }

    private static long sendFile(
            KafkaProducer<String, String> producer,
            String topic,
            Path csvFile,
            long sendDelayMs
    ) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        long count = 0L;
        String sourceFile = csvFile.getFileName().toString();
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true)
                .build();

        try (BufferedReader reader = Files.newBufferedReader(csvFile, StandardCharsets.UTF_8);
             CSVParser parser = csvFormat.parse(reader)) {
            for (CSVRecord record : parser) {
                Map<String, String> event = new LinkedHashMap<>(record.toMap());
                event.put("source_file", sourceFile);
                event.put("source_record_number", Long.toString(record.getRecordNumber()));
                event.put("event_id", sourceFile + "#" + record.getRecordNumber());

                String eventId = event.get("event_id");
                String json = OBJECT_MAPPER.writeValueAsString(event);
                producer.send(new ProducerRecord<>(topic, eventId, json)).get(30, TimeUnit.SECONDS);
                count++;

                if (sendDelayMs > 0) {
                    Thread.sleep(sendDelayMs);
                }
            }
        }

        System.out.printf("Sent %d records from %s%n", count, sourceFile);
        return count;
    }

    private static List<Path> csvFiles(Path inputDir) throws IOException {
        if (!Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("Input path is not a directory: " + inputDir.toAbsolutePath());
        }

        List<Path> result = new ArrayList<>();
        try (var stream = Files.list(inputDir)) {
            stream.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .forEach(result::add);
        }
        return result;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static long longEnvOrDefault(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }
}
