package io.github.himmireda.ridehailing.driverservice.outbox;

import io.github.himmireda.ridehailing.driverservice.kafka.EventPublisher;
import io.github.himmireda.ridehailing.messaging.AvroBinary;
import io.github.himmireda.ridehailing.messaging.EventHeaders;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Publishes the outbox collection to Kafka (polling relay).
 *
 * <p>MongoDB has no SELECT ... FOR UPDATE SKIP LOCKED, so documents are <em>claimed</em>: one atomic
 * findAndModify takes the oldest unpublished document that nobody holds (or whose claim lease
 * expired) and stamps it with this relay's name. Only one relay can win a given document, so several
 * instances can run side by side. The relay then publishes it (event id in a header) and sets
 * publishedAt after Kafka's ack. On failure it releases the claim, increments attempts, stops the
 * batch and backs off. A relay that dies mid-publish loses its claim when the lease expires.
 * Delivery is at-least-once; consumers deduplicate on the event id.
 */
@Slf4j
public class OutboxRelay implements SmartLifecycle {

    private final String name;
    private final ReactiveMongoTemplate mongoTemplate;
    private final EventPublisher eventPublisher;
    private final OutboxProperties properties;

    private final AtomicLong publishedCount = new AtomicLong();
    private volatile Disposable loop;
    private int consecutiveFailures;
    private Instant pausedUntil = Instant.EPOCH;

    public OutboxRelay(String name, ReactiveMongoTemplate mongoTemplate, EventPublisher eventPublisher,
                       OutboxProperties properties) {
        this.name = name;
        this.mongoTemplate = mongoTemplate;
        this.eventPublisher = eventPublisher;
        this.properties = properties;
    }

    @Override
    public void start() {
        loop = Flux.interval(properties.pollInterval())
                .onBackpressureDrop()
                .concatMap(tick -> pollOnce(), 1)
                .subscribe();
        log.info("Outbox relay {} started (every {}, batches of {})", name, properties.pollInterval(), properties.batchSize());
    }

    @Override
    public void stop() {
        Disposable current = loop;
        if (current != null) {
            current.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        Disposable current = loop;
        return current != null && !current.isDisposed();
    }

    public long publishedCount() {
        return publishedCount.get();
    }

    private Mono<Void> pollOnce() {
        if (Instant.now().isBefore(pausedUntil)) {
            return Mono.empty();
        }
        return publishNext(properties.batchSize())
                .doOnNext(this::updateBackoff)
                .onErrorResume(error -> {
                    log.warn("Outbox relay {}: poll failed: {}", name, error.toString());
                    updateBackoff(false);
                    return Mono.empty();
                })
                .then();
    }

    /** Claims and publishes up to {@code remaining} documents, one at a time, oldest first. */
    private Mono<Boolean> publishNext(int remaining) {
        if (remaining == 0) {
            return Mono.just(true);
        }
        return claimNext()
                .flatMap(document -> publish(document)
                        .then(markPublished(document))
                        .thenReturn(true)
                        .onErrorResume(error -> release(document, error).thenReturn(false)))
                .flatMap(published -> published ? publishNext(remaining - 1) : Mono.just(false))
                .defaultIfEmpty(true);  // nothing (left) to publish
    }

    private Mono<OutboxDocument> claimNext() {
        Instant now = Instant.now();
        Query unclaimed = Query.query(where("publishedAt").is(null)
                        .orOperator(where("claimedUntil").is(null), where("claimedUntil").lt(now)))
                .with(Sort.by("createdAt", "_id"));
        Update claim = new Update().set("claimedBy", name).set("claimedUntil", now.plus(properties.claimLease()));
        return mongoTemplate.findAndModify(unclaimed, claim, FindAndModifyOptions.options().returnNew(true),
                OutboxDocument.class);
    }

    private Mono<Void> publish(OutboxDocument document) {
        return Mono.defer(() -> {
            SpecificRecord event = AvroBinary.decode(document.getEventType(), document.getPayload());
            ProducerRecord<String, SpecificRecord> record =
                    new ProducerRecord<>(document.getTopic(), document.getMessageKey(), event);
            record.headers().add(EventHeaders.EVENT_ID, document.getId().getBytes(StandardCharsets.UTF_8));
            record.headers().add(EventHeaders.EVENT_TYPE, document.getEventType().getBytes(StandardCharsets.UTF_8));
            return eventPublisher.publish(record);
        }).doOnSuccess(ignored -> publishedCount.incrementAndGet());
    }

    private Mono<Void> markPublished(OutboxDocument document) {
        return mongoTemplate.updateFirst(
                        Query.query(where("_id").is(document.getId()).and("claimedBy").is(name)),
                        new Update().set("publishedAt", Instant.now()).inc("attempts", 1)
                                .unset("claimedBy").unset("claimedUntil"),
                        OutboxDocument.class)
                .then();
    }

    private Mono<Void> release(OutboxDocument document, Throwable error) {
        log.warn("Outbox relay {}: publishing event {} to {} failed: {}", name, document.getId(),
                document.getTopic(), error.toString());
        return mongoTemplate.updateFirst(
                        Query.query(where("_id").is(document.getId()).and("claimedBy").is(name)),
                        new Update().inc("attempts", 1).unset("claimedBy").unset("claimedUntil"),
                        OutboxDocument.class)
                .then();
    }

    private synchronized void updateBackoff(boolean success) {
        if (success) {
            consecutiveFailures = 0;
            pausedUntil = Instant.EPOCH;
            return;
        }
        consecutiveFailures++;
        long factor = 1L << Math.min(consecutiveFailures - 1, 16);
        Duration delay = properties.initialBackoff().multipliedBy(factor);
        if (delay.compareTo(properties.maxBackoff()) > 0) {
            delay = properties.maxBackoff();
        }
        pausedUntil = Instant.now().plus(delay);
        log.info("Outbox relay {}: backing off for {} after {} failed poll(s)", name, delay, consecutiveFailures);
    }
}
