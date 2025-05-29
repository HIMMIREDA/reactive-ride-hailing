package fr.univ.artois.reda.rideservice.config.kafka;

import fr.univ.artois.reda.avro.RideProposalBookedEvent;
import fr.univ.artois.reda.avro.RideProposalProposedEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ReactiveKafkaConsumer {


    @Value("${kafka.topics.ride-proposal-proposed}")
    private String rideProposalProposedTopicName;

    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopicName;


    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.properties.auto.register.schemas}")
    private boolean autoRegisterSchema;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    public <U, T> ReceiverOptions<U, T> createKafkaReceiverOptions(KafkaProperties kafkaProperties, String topicName) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, autoRegisterSchema);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        ReceiverOptions<U, T> basicReceiverOptions = ReceiverOptions.create(config);
        return basicReceiverOptions.subscription(Collections.singletonList(topicName));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, RideProposalProposedEvent> reactiveRideProposalProposedKafkaConsumerTemplate(KafkaProperties kafkaProperties) {
        return new ReactiveKafkaConsumerTemplate<>(
                createKafkaReceiverOptions(kafkaProperties, rideProposalProposedTopicName)
        );
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, RideProposalBookedEvent> reactiveRideProposalBookedKafkaConsumerTemplate(KafkaProperties kafkaProperties) {
        return new ReactiveKafkaConsumerTemplate<>(
                createKafkaReceiverOptions(kafkaProperties, rideProposalBookedTopicName)
        );
    }
}
