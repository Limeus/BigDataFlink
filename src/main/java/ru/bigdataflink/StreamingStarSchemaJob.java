package ru.bigdataflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public final class StreamingStarSchemaJob {
    private StreamingStarSchemaJob() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");
        String topic = envOrDefault("KAFKA_TOPIC", "pet-sales");
        String groupId = envOrDefault("KAFKA_GROUP_ID", "flink-star-schema");
        String postgresUrl = envOrDefault("POSTGRES_URL", "jdbc:postgresql://localhost:5432/petshop");
        String postgresUser = envOrDefault("POSTGRES_USER", "flink");
        String postgresPassword = envOrDefault("POSTGRES_PASSWORD", "flink");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(Duration.ofSeconds(10).toMillis());

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<SaleEvent> saleEvents = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka CSV JSON source")
                .map(new JsonToSaleEventMap())
                .name("Parse JSON to sale event");

        saleEvents
                .addSink(new StarSchemaJdbcSink(postgresUrl, postgresUser, postgresPassword))
                .name("Upsert star schema to PostgreSQL")
                .uid("postgres-star-schema-sink");

        env.execute("CSV sales streaming star schema");
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
