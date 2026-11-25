package io.github.himmireda.ridehailing.rideservice.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/** Outbox relay settings (app.outbox.*). */
@ConfigurationProperties("app.outbox")
public record OutboxProperties(
        @DefaultValue("true") boolean relayEnabled,
        @DefaultValue("500ms") Duration pollInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("1s") Duration initialBackoff,
        @DefaultValue("30s") Duration maxBackoff) {
}
