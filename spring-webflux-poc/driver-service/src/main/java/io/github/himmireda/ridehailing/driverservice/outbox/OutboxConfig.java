package io.github.himmireda.ridehailing.driverservice.outbox;

import io.github.himmireda.ridehailing.driverservice.kafka.EventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.transaction.reactive.TransactionalOperator;

@Configuration
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxConfig {

    /**
     * Multi-document transactions (business document + outbox document + processed-event marker).
     * Spring Boot does not auto-configure it; MongoDB needs a replica set for transactions.
     */
    @Bean
    public ReactiveMongoTransactionManager reactiveMongoTransactionManager(ReactiveMongoDatabaseFactory factory) {
        return new ReactiveMongoTransactionManager(factory);
    }

    @Bean
    public TransactionalOperator transactionalOperator(ReactiveMongoTransactionManager transactionManager) {
        return TransactionalOperator.create(transactionManager);
    }

    @Bean
    @ConditionalOnProperty(name = "app.outbox.relay-enabled", havingValue = "true", matchIfMissing = true)
    public OutboxRelay outboxRelay(ReactiveMongoTemplate mongoTemplate, EventPublisher eventPublisher,
                                   OutboxProperties properties) {
        return new OutboxRelay("driver-service-relay", mongoTemplate, eventPublisher, properties);
    }
}
