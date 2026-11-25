package io.github.himmireda.ridehailing.rideservice.support;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;

/**
 * Plain Kafka clients for the test side: publish the events the service consumes, and read back
 * the events it produces.
 *
 * <p>Avro without a Schema Registry: a registry URL starting with {@code mock://} makes the Confluent
 * serializers use an in-memory registry, shared by every client in the same JVM that uses the same
 * scope name. The application and the test both point at {@link #SCHEMA_REGISTRY_URL}, so a schema
 * registered by one side can be resolved by the other.
 */
public final class AvroKafkaTestClient {

    public static final String SCHEMA_REGISTRY_URL = "mock://ride-service-it";

    private AvroKafkaTestClient() {
    }

    public static void send(String bootstrapServers, String topic, String key, SpecificRecord value) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        try (KafkaProducer<String, SpecificRecord> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not send test event to " + topic, e);
        }
    }

    /**
     * Reads the topic from the beginning with a fresh consumer group until a record matches,
     * or fails after 30 seconds. A fresh group per call keeps tests independent of each other.
     */
    @SuppressWarnings("unchecked")
    public static <V extends SpecificRecord> ConsumerRecord<String, V> awaitRecord(
            String bootstrapServers, String topic, Predicate<ConsumerRecord<String, V>> matcher) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        try (KafkaConsumer<String, V> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, V>[] found = new ConsumerRecord[1];
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                for (ConsumerRecord<String, V> record : consumer.poll(Duration.ofMillis(500))) {
                    if (matcher.test(record)) {
                        found[0] = record;
                        return true;
                    }
                }
                return false;
            });
            return found[0];
        }
    }
}
