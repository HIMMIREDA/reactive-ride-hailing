package io.github.himmireda.ridehailing.rideservice.kafka.consumer;

import io.github.himmireda.ridehailing.avro.RideProposalProposedEvent;
import io.github.himmireda.ridehailing.rideservice.kafka.RideProposalSinkManager;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class RideProposalProposedEventConsumer {

    private final RideProposalSinkManager rideProposalSinkManager;


    private final ReactiveKafkaConsumerTemplate<String, RideProposalProposedEvent> reactiveRideProposalProposedKafkaConsumerTemplate;


    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumer() {
        reactiveRideProposalProposedKafkaConsumerTemplate
                .receive()
                .doOnNext(record -> {
                    rideProposalSinkManager.emitProposal(record.value());
                    record.receiverOffset().acknowledge();
                })
                .subscribe();
    }
}
