package io.github.himmireda.ridehailing.driverservice.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Idempotent consumers: one marker document per (consumer, event), written in the same MongoDB
 * transaction as the state change.
 *
 * <p>Read first, then insert: a duplicate-key error inside a MongoDB transaction aborts the whole
 * transaction, so it is not used for the normal duplicate case. If two deliveries race, the loser's
 * transaction fails (write conflict / duplicate key), the listener retries, and the retry finds the
 * marker committed by the winner.
 */
@Component
@RequiredArgsConstructor
public class ProcessedEvents {

    private final ReactiveMongoTemplate mongoTemplate;

    /** @return true the first time this consumer sees the event, false for a duplicate delivery */
    public Mono<Boolean> markProcessed(String eventId, String consumer) {
        String id = consumer + "|" + eventId;
        return mongoTemplate.exists(query(where("_id").is(id)), ProcessedEvent.class)
                .flatMap(alreadyProcessed -> alreadyProcessed
                        ? Mono.just(false)
                        : mongoTemplate.insert(new ProcessedEvent(id, eventId, consumer, Instant.now())).thenReturn(true));
    }

    @Document(collection = "processed_events")
    record ProcessedEvent(@Id String id, String eventId, String consumer, Instant processedAt) {
    }
}
