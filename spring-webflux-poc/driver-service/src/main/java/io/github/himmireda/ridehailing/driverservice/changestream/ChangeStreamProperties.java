package io.github.himmireda.ridehailing.driverservice.changestream;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/** Shared ride-request change stream settings (app.change-stream.*). */
@ConfigurationProperties("app.change-stream")
public record ChangeStreamProperties(
        /* Per SSE subscriber: requests buffered while the client is slow; the oldest are dropped beyond. */
        @DefaultValue("256") int subscriberBuffer,
        @DefaultValue("500ms") Duration minRetryBackoff,
        @DefaultValue("30s") Duration maxRetryBackoff) {
}
