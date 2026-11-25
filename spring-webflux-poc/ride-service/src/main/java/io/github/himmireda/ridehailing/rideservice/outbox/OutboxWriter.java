package io.github.himmireda.ridehailing.rideservice.outbox;

import io.github.himmireda.ridehailing.messaging.AvroBinary;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Writes an event to the outbox table instead of sending it to Kafka.
 *
 * <p>Call it inside the same reactive transaction as the business change (@Transactional or a
 * TransactionalOperator): DatabaseClient joins the transaction bound to the subscriber context, so
 * either both rows are committed or neither is. That removes the dual write "save, then publish"
 * where a crash or a Kafka outage between the two steps lost the event.
 */
@Component
@RequiredArgsConstructor
public class OutboxWriter {

    private final DatabaseClient databaseClient;

    /** @return the event id (also the Kafka "event-id" header once published) */
    public Mono<UUID> append(String topic, String messageKey, String aggregateId, SpecificRecord event) {
        return Mono.defer(() -> {
            UUID eventId = UUID.randomUUID();
            return databaseClient.sql("""
                            INSERT INTO outbox (id, aggregate_id, event_type, topic, message_key, payload)
                            VALUES (:id, :aggregateId, :eventType, :topic, :messageKey, :payload)""")
                    .bind("id", eventId)
                    .bind("aggregateId", aggregateId)
                    .bind("eventType", AvroBinary.eventType(event))
                    .bind("topic", topic)
                    .bind("messageKey", messageKey)
                    .bind("payload", AvroBinary.encode(event))
                    .then()
                    .thenReturn(eventId);
        });
    }
}
