package io.github.himmireda.ridehailing.rideservice.outbox;

import io.github.himmireda.ridehailing.rideservice.kafka.EventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

@Configuration
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxConfig {

    @Bean
    @ConditionalOnProperty(name = "app.outbox.relay-enabled", havingValue = "true", matchIfMissing = true)
    public OutboxRelay outboxRelay(DatabaseClient databaseClient, TransactionalOperator transactionalOperator,
                                   EventPublisher eventPublisher, OutboxProperties properties) {
        return new OutboxRelay("ride-service-relay", databaseClient, transactionalOperator, eventPublisher, properties);
    }
}
