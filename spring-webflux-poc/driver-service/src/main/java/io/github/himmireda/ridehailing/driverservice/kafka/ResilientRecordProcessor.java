package io.github.himmireda.ridehailing.driverservice.kafka;

import io.github.himmireda.ridehailing.driverservice.config.kafka.KafkaRetryProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.util.function.Supplier;

/**
 * Wraps the work of an async (Mono-returning) Kafka listener: retries failures with exponential
 * backoff, then publishes the record to its dead-letter topic. The returned Mono completes in both
 * cases, so the container commits the offset and moves on: one failing record never stops consumption.
 *
 * <p>Why not the container's DefaultErrorHandler: in Spring Kafka 3.3 it only sees async failures
 * for @RetryableTopic listeners. Same recoverer, so DLT records look the same either way.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ResilientRecordProcessor {

    private final DeadLetterPublishingRecoverer deadLetterPublishingRecoverer;
    private final KafkaRetryProperties retry;

    public Mono<Void> process(ConsumerRecord<?, ?> record, Supplier<Mono<Void>> work) {
        return Mono.defer(work)
                .retryWhen(Retry.backoff(retry.maxRetries(), retry.initialInterval())
                        .multiplier(retry.multiplier())
                        .maxBackoff(retry.maxInterval())
                        // Bad data won't get better by waiting.
                        .filter(error -> !(error instanceof IllegalArgumentException))
                        .doBeforeRetry(signal -> log.warn("Retrying {}-{}@{} (attempt {}): {}", record.topic(),
                                record.partition(), record.offset(), signal.totalRetries() + 1,
                                signal.failure().getMessage()))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .onErrorResume(error -> deadLetter(record, error));
    }

    private Mono<Void> deadLetter(ConsumerRecord<?, ?> record, Throwable error) {
        log.error("Giving up on {}-{}@{}, sending it to {}.DLT: {}", record.topic(), record.partition(),
                record.offset(), record.topic(), error.toString());
        Exception exception = error instanceof Exception e ? e : new IllegalStateException(error);
        // The recoverer waits for the broker's ack: keep that off the reactive threads.
        return Mono.<Void>fromRunnable(() -> deadLetterPublishingRecoverer.accept(record, exception))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
