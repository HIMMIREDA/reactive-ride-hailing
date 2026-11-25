package io.github.himmireda.ridehailing.rideservice.config.kafka;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Topics, dead-letter topics and the failure handling of the Kafka listeners.
 *
 * <p>Two failure paths, one dead-letter publisher:
 * <ul>
 *   <li>Poison records (can't be deserialized): ErrorHandlingDeserializer turns the failure into a
 *       DeserializationException before the listener runs. The container hands it to
 *       {@link DefaultErrorHandler}, which treats it as not retryable: straight to "&lt;topic&gt;.DLT"
 *       with the original bytes.</li>
 *   <li>Failures inside a listener's returned Mono: in Spring Kafka 3.3 a failed async result only
 *       reaches the container error handler for @RetryableTopic listeners (otherwise the record is
 *       logged and acknowledged). So {@link io.github.himmireda.ridehailing.rideservice.kafka.ResilientRecordProcessor}
 *       retries inside the reactive pipeline with exponential backoff, then hands the record to the
 *       same {@link DeadLetterPublishingRecoverer}.</li>
 * </ul>
 * Either way the offset is committed only once the record was processed or dead-lettered, and one
 * bad record never stops consumption.
 */
@Configuration
@EnableConfigurationProperties(KafkaRetryProperties.class)
public class KafkaConfig {

    @Value("${kafka.topics.ride-request-requested}")
    private String rideRequestRequestedTopicName;

    @Value("${kafka.topics.ride-proposal-accepted}")
    private String rideProposalAcceptedTopicName;

    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopicName;

    @Value("${kafka.topics.ride-proposal-proposed}")
    private String rideProposalProposedTopicName;

    @Bean
    public NewTopic rideRequestRequestedTopic() {
        return TopicBuilder.name(rideRequestRequestedTopicName).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic rideProposalAcceptedTopic() {
        return TopicBuilder.name(rideProposalAcceptedTopicName).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic rideProposalBookedTopic() {
        return TopicBuilder.name(rideProposalBookedTopicName).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic rideProposalProposedTopic() {
        return TopicBuilder.name(rideProposalProposedTopicName).partitions(1).replicas(1).build();
    }

    // Dead-letter topics of the topics this service consumes (same partition count as the source).
    @Bean
    public NewTopic rideProposalBookedDltTopic() {
        return TopicBuilder.name(rideProposalBookedTopicName + ".DLT").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic rideProposalProposedDltTopic() {
        return TopicBuilder.name(rideProposalProposedTopicName + ".DLT").partitions(1).replicas(1).build();
    }

    /** Publishes failed records to "&lt;topic&gt;.DLT" (same partition), with exception headers. */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, SpecificRecord> avroTemplate,
                                                                       KafkaProperties kafkaProperties,
                                                                       KafkaConnectionDetails connectionDetails) {
        // Poison records have no Avro object to serialize: their original bytes are republished as-is.
        Map<String, Object> rawProps = kafkaProperties.buildProducerProperties();
        rawProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", connectionDetails.getBootstrapServers()));
        KafkaTemplate<String, byte[]> rawTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(rawProps, new StringSerializer(), new ByteArraySerializer()));

        Map<Class<?>, KafkaOperations<?, ?>> templatesByValueType = new LinkedHashMap<>();
        templatesByValueType.put(byte[].class, rawTemplate);
        templatesByValueType.put(Object.class, avroTemplate);
        // Explicit "<topic>.DLT" name (Spring Kafka's default suffix is "-dlt"), same partition as the source.
        return new DeadLetterPublishingRecoverer(templatesByValueType,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    }

    /** Container error handler: handles failures raised before/outside the Mono (e.g. poison records). */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer, KafkaRetryProperties retry) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
