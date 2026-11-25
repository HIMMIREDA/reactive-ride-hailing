package io.github.himmireda.ridehailing.driverservice.outbox;

import io.github.himmireda.ridehailing.messaging.AvroBinary;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Writes an event to the outbox collection instead of sending it to Kafka. Call it inside the same
 * MongoDB transaction as the business change (@Transactional / TransactionalOperator backed by the
 * ReactiveMongoTransactionManager): both documents are committed together, or neither is.
 */
@Component
@RequiredArgsConstructor
public class OutboxWriter {

    private final ReactiveMongoTemplate mongoTemplate;

    /** @return the event id (also the Kafka "event-id" header once published) */
    public Mono<String> append(String topic, String messageKey, String aggregateId, SpecificRecord event) {
        return Mono.defer(() -> mongoTemplate.insert(OutboxDocument.builder()
                        .id(UUID.randomUUID().toString())
                        .aggregateId(aggregateId)
                        .eventType(AvroBinary.eventType(event))
                        .topic(topic)
                        .messageKey(messageKey)
                        .payload(AvroBinary.encode(event))
                        .createdAt(Instant.now())
                        .build()))
                .map(OutboxDocument::getId);
    }
}
