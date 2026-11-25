package io.github.himmireda.ridehailing.rideservice.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Reactive bridge over Spring Kafka's KafkaTemplate (replaces the deprecated reactive templates).
 *
 * <ul>
 *   <li>{@code Mono.fromFuture(() -> send(...))}: the supplier form is lazy, so nothing is sent until
 *       the Mono is subscribed (and a retry re-sends). {@code Mono.fromFuture(send(...))} would send
 *       immediately, at assembly time.</li>
 *   <li>{@code subscribeOn(boundedElastic)}: {@code send()} can block up to {@code max.block.ms}
 *       while it fetches metadata (e.g. when Kafka is unreachable). That must never run on a Netty
 *       event-loop thread.</li>
 *   <li>The Mono completes once the broker has acknowledged the record (acks=all).</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    public Mono<Void> publish(ProducerRecord<String, SpecificRecord> record) {
        return Mono.fromFuture(() -> kafkaTemplate.send(record))
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    public Mono<Void> publish(String topic, String key, SpecificRecord event) {
        return publish(new ProducerRecord<>(topic, key, event));
    }
}
