package io.github.himmireda.ridehailing.rideservice.config.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/** Retry policy of the Kafka listeners before a record is sent to its dead-letter topic. */
@ConfigurationProperties("app.kafka.retry")
public record KafkaRetryProperties(
        @DefaultValue("3") int maxRetries,
        @DefaultValue("500ms") Duration initialInterval,
        @DefaultValue("2.0") double multiplier,
        @DefaultValue("5s") Duration maxInterval) {
}
