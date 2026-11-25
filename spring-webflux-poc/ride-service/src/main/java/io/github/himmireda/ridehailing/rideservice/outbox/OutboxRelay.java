package io.github.himmireda.ridehailing.rideservice.outbox;

import io.github.himmireda.ridehailing.messaging.AvroBinary;
import io.github.himmireda.ridehailing.messaging.EventHeaders;
import io.github.himmireda.ridehailing.rideservice.kafka.EventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.SmartLifecycle;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes the outbox to Kafka (polling relay).
 *
 * <p>Every poll is one database transaction:
 * <ol>
 *   <li>{@code SELECT ... FOR UPDATE SKIP LOCKED} locks a batch of unpublished rows, oldest first.
 *       Rows locked by another relay instance are skipped instead of waited for, so several instances
 *       can run side by side and each row is published by exactly one of them.</li>
 *   <li>Each row is published in order with KafkaTemplate (event id in a header); published_at is set
 *       only after Kafka acknowledged it.</li>
 *   <li>At the first failure the batch stops (later events of the same ride must not overtake it),
 *       attempts is incremented and the relay backs off exponentially before the next poll.</li>
 * </ol>
 * Delivery is at-least-once: if the relay dies after Kafka's ack but before the commit, the row is
 * published again. Consumers deduplicate on the event id.
 */
@Slf4j
public class OutboxRelay implements SmartLifecycle {

    private final String name;
    private final DatabaseClient databaseClient;
    private final TransactionalOperator transactionalOperator;
    private final EventPublisher eventPublisher;
    private final OutboxProperties properties;

    private final AtomicLong publishedCount = new AtomicLong();
    private volatile Disposable loop;
    private int consecutiveFailures;
    private Instant pausedUntil = Instant.EPOCH;

    public OutboxRelay(String name, DatabaseClient databaseClient, TransactionalOperator transactionalOperator,
                       EventPublisher eventPublisher, OutboxProperties properties) {
        this.name = name;
        this.databaseClient = databaseClient;
        this.transactionalOperator = transactionalOperator;
        this.eventPublisher = eventPublisher;
        this.properties = properties;
    }

    @Override
    public void start() {
        loop = Flux.interval(properties.pollInterval())
                .onBackpressureDrop()                   // a slow poll skips ticks instead of queueing them
                .concatMap(tick -> pollOnce(), 1)       // never two polls of the same relay at once
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
        return publishBatch()
                .doOnNext(this::updateBackoff)
                .onErrorResume(error -> {
                    log.warn("Outbox relay {}: poll failed: {}", name, error.toString());
                    updateBackoff(false);
                    return Mono.empty();
                })
                .then();
    }

    /** @return true if every row of the batch was published (or there was nothing to publish) */
    Mono<Boolean> publishBatch() {
        Mono<List<OutboxRow>> lockBatch = databaseClient.sql("""
                        SELECT id, topic, message_key, event_type, payload
                        FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY created_at, id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED""")
                .bind("limit", properties.batchSize())
                .map(row -> new OutboxRow(
                        row.get("id", UUID.class),
                        row.get("topic", String.class),
                        row.get("message_key", String.class),
                        row.get("event_type", String.class),
                        row.get("payload", byte[].class)))
                .all()
                // Load the batch first: one R2DBC connection can't run the UPDATEs below while it is
                // still streaming this SELECT's result.
                .collectList();

        return lockBatch
                .flatMapMany(Flux::fromIterable)
                .concatMap(row -> publish(row)
                        .then(markPublished(row.id()))
                        .thenReturn(true)
                        .onErrorResume(error -> markFailed(row, error).thenReturn(false)))
                .takeUntil(published -> !published)     // stop at the first failure, keep the order
                .all(published -> published)
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> publish(OutboxRow row) {
        return Mono.defer(() -> {
            SpecificRecord event = AvroBinary.decode(row.eventType(), row.payload());
            ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(row.topic(), row.messageKey(), event);
            record.headers().add(EventHeaders.EVENT_ID, row.id().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add(EventHeaders.EVENT_TYPE, row.eventType().getBytes(StandardCharsets.UTF_8));
            return eventPublisher.publish(record);
        }).doOnSuccess(ignored -> publishedCount.incrementAndGet());
    }

    private Mono<Void> markPublished(UUID id) {
        return databaseClient.sql("UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = :id")
                .bind("id", id)
                .then();
    }

    private Mono<Void> markFailed(OutboxRow row, Throwable error) {
        log.warn("Outbox relay {}: publishing event {} to {} failed: {}", name, row.id(), row.topic(), error.toString());
        return databaseClient.sql("UPDATE outbox SET attempts = attempts + 1 WHERE id = :id")
                .bind("id", row.id())
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

    private record OutboxRow(UUID id, String topic, String messageKey, String eventType, byte[] payload) {
    }
}
