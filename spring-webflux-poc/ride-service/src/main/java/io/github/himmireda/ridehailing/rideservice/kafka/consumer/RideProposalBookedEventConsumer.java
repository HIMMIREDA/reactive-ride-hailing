package io.github.himmireda.ridehailing.rideservice.kafka.consumer;

import io.github.himmireda.ridehailing.avro.RideProposalBookedEvent;
import io.github.himmireda.ridehailing.rideservice.entity.BookedRideProposalEntity;
import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.rideservice.kafka.EventTargetNotFoundException;
import io.github.himmireda.ridehailing.rideservice.kafka.ResilientRecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.github.himmireda.ridehailing.rideservice.repository.BookedRideProposalRepository;
import io.github.himmireda.ridehailing.rideservice.repository.RideRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Component
public class RideProposalBookedEventConsumer {

    private final BookedRideProposalRepository bookedRideProposalRepository;
    private final RideRequestRepository rideRequestRepository;
    private final TransactionalOperator transactionalOperator;
    private final ResilientRecordProcessor resilientRecordProcessor;

    /**
     * Async listener (Spring Kafka 3.2+): returning a Mono switches the container to manual acks, and
     * the offset is committed only when the Mono completes. ResilientRecordProcessor retries failures
     * with backoff and dead-letters the record when retries are exhausted.
     */
    @KafkaListener(topics = "${kafka.topics.ride-proposal-booked}")
    public Mono<Void> onRideProposalBooked(ConsumerRecord<String, RideProposalBookedEvent> record) {
        RideProposalBookedEvent event = record.value();
        return resilientRecordProcessor.process(record, () -> {
                    UUID rideRequestId = UUID.fromString(event.getRideRequestId().toString());
                    return rideRequestRepository.findById(rideRequestId)
                            .switchIfEmpty(Mono.error(() -> new EventTargetNotFoundException(
                                    "Booked event for unknown ride request " + rideRequestId)))
                            .flatMap(rideRequest -> {
                                if (rideRequest.getStatus() != RideRequestStatus.ACCEPTED) {
                                    log.warn("Ignoring booked event for ride request {} in status {} (duplicate or out of order)",
                                            rideRequestId, rideRequest.getStatus());
                                    return Mono.empty();
                                }
                                BookedRideProposalEntity booked = mapToBookedRideProposalEntity(event);
                                rideRequest.setAcceptedRideProposalId(booked.getRideProposalId());
                                rideRequest.setStatus(RideRequestStatus.BOOKED);
                                return rideRequestRepository.save(rideRequest)
                                        .then(bookedRideProposalRepository.save(booked));
                            })
                            .then()
                            .as(transactionalOperator::transactional);
                });
    }

    private BookedRideProposalEntity mapToBookedRideProposalEntity(RideProposalBookedEvent event) {
        return BookedRideProposalEntity.builder()
                .rideProposalId(UUID.fromString(event.getRideProposalId().toString()))
                .driverId(UUID.fromString(event.getDriverId().toString()))
                .driverFirstName(event.getDriverFirstName().toString())
                .driverLastName(event.getDriverLastName().toString())
                .driverEmail(event.getDriverEmail().toString())
                .driverPhoneNumber(event.getDriverPhoneNumber().toString())
                .price(event.getPrice())
                .acceptedAt(LocalDateTime.now())
                .build();
    }
}
