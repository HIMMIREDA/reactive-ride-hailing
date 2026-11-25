package io.github.himmireda.ridehailing.rideservice.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Idempotent consumers: records that an event was processed, in the same transaction as the state
 * change it causes. The primary key (event_id, consumer) makes a second delivery a no-op, even when
 * two deliveries of the same event are processed concurrently: the second INSERT waits for the first
 * transaction and then finds the row.
 */
@Component
@RequiredArgsConstructor
public class ProcessedEvents {

    private final DatabaseClient databaseClient;

    /** @return true the first time this consumer sees the event, false for a duplicate delivery */
    public Mono<Boolean> markProcessed(String eventId, String consumer) {
        return databaseClient.sql("""
                        INSERT INTO processed_events (event_id, consumer) VALUES (:eventId, :consumer)
                        ON CONFLICT DO NOTHING""")
                .bind("eventId", eventId)
                .bind("consumer", consumer)
                .fetch()
                .rowsUpdated()
                .map(rows -> rows == 1);
    }
}
