package io.github.himmireda.ridehailing.driverservice.config.kafka;

import io.github.himmireda.ridehailing.avro.RideProposalBookedEvent;
import io.github.himmireda.ridehailing.avro.RideProposalProposedEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
public class ReactiveKafkaProducerConfig {


    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.properties.auto.register.schemas}")
    private boolean autoRegisterSchema;

    private <U, T> ReactiveKafkaProducerTemplate<U, T> createProducerTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, autoRegisterSchema);
        return new ReactiveKafkaProducerTemplate<U, T>(SenderOptions.create(props));
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, RideProposalProposedEvent> rideProposalProposedEventReactiveKafkaProducerTemplate(KafkaProperties kafkaProperties) {
        return createProducerTemplate(kafkaProperties);
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, RideProposalBookedEvent> rideProposalBookedEventReactiveKafkaProducerTemplate(KafkaProperties kafkaProperties) {
        return createProducerTemplate(kafkaProperties);
    }
}
