package io.github.himmireda.ridehailing.rideservice.kafka.consumer;

import io.github.himmireda.ridehailing.avro.RideProposalProposedEvent;
import io.github.himmireda.ridehailing.rideservice.kafka.ResilientRecordProcessor;
import io.github.himmireda.ridehailing.rideservice.kafka.RideProposalSinkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** Pushes driver offers to the riders' SSE streams (RideProposalSinkManager). */
@RequiredArgsConstructor
@Component
public class RideProposalProposedEventConsumer {

    private final RideProposalSinkManager rideProposalSinkManager;
    private final ResilientRecordProcessor resilientRecordProcessor;

    @KafkaListener(topics = "${kafka.topics.ride-proposal-proposed}")
    public Mono<Void> onRideProposalProposed(ConsumerRecord<String, RideProposalProposedEvent> record) {
        return resilientRecordProcessor.process(record,
                () -> Mono.fromRunnable(() -> rideProposalSinkManager.emitProposal(record.value())));
    }
}
