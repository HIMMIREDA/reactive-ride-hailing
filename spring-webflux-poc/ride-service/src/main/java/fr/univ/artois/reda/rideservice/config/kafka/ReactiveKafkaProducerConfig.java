package fr.univ.artois.reda.rideservice.config.kafka;

import fr.univ.artois.reda.avro.RideRequestedEvent;
import fr.univ.artois.reda.avro.RideToAcceptEvent;
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


    @Bean
    public ReactiveKafkaProducerTemplate<String, RideRequestedEvent> rideRequestedProducerTemplate(KafkaProperties kafkaProperties) {
        return createProducerTemplate(kafkaProperties);
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, RideToAcceptEvent> rideToAcceptProducerTemplate(KafkaProperties kafkaProperties) {
        return createProducerTemplate(kafkaProperties);
    }

    private <U, T> ReactiveKafkaProducerTemplate<U,T > createProducerTemplate(KafkaProperties kafkaProperties){
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
}
