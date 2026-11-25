package io.github.himmireda.ridehailing.driverservice.kafka.consumer;

import io.github.himmireda.ridehailing.avro.RideProposalAcceptedEvent;
import io.github.himmireda.ridehailing.avro.RideProposalBookedEvent;
import io.github.himmireda.ridehailing.driverservice.entity.RideProposalEntity;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.driverservice.kafka.EventPublisher;
import io.github.himmireda.ridehailing.driverservice.kafka.EventTargetNotFoundException;
import io.github.himmireda.ridehailing.driverservice.kafka.ResilientRecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.github.himmireda.ridehailing.driverservice.repository.RideProposalRepository;
import io.github.himmireda.ridehailing.driverservice.repository.RideRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** The rider accepted an offer: mark the request BOOKED and tell ride-service (booked event). */
@Slf4j
@Component
@RequiredArgsConstructor
public class RideProposalAcceptedEventConsumer {

    private final RideRequestRepository rideRequestRepository;
    private final RideProposalRepository rideProposalRepository;
    private final EventPublisher eventPublisher;
    private final ResilientRecordProcessor resilientRecordProcessor;

    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopic;

    /**
     * The offset is committed only after the request is saved AND the booked event is acknowledged
     * by Kafka (the returned Mono covers both). A missing request or offer fails the work, so the
     * record is retried and then dead-lettered instead of being skipped silently.
     */
    @KafkaListener(topics = "${kafka.topics.ride-proposal-accepted}")
    public Mono<Void> onRideProposalAccepted(ConsumerRecord<String, RideProposalAcceptedEvent> record) {
        RideProposalAcceptedEvent event = record.value();
        String rideRequestId = event.getRideRequestId().toString();
        String rideProposalId = event.getRideProposalId().toString();
        return resilientRecordProcessor.process(record, () -> Mono.zip(
                        rideRequestRepository.findById(rideRequestId)
                                .switchIfEmpty(Mono.error(() -> new EventTargetNotFoundException(
                                        "Accepted event for unknown ride request " + rideRequestId))),
                        rideProposalRepository.findById(rideProposalId)
                                .switchIfEmpty(Mono.error(() -> new EventTargetNotFoundException(
                                        "Accepted event for unknown ride proposal " + rideProposalId))))
                .flatMap(tuple -> {
                    RideRequestEntity rideRequest = tuple.getT1();
                    RideProposalEntity rideProposal = tuple.getT2();
                    if (rideRequest.getStatus() == RideRequestStatus.BOOKED) {
                        log.warn("Ignoring accepted event for ride request {}: already BOOKED (duplicate)", rideRequestId);
                        return Mono.empty();
                    }
                    rideRequest.setStatus(RideRequestStatus.BOOKED);
                    rideRequest.setAcceptedRideProposalId(rideProposal.getRideProposalId());
                    return rideRequestRepository.save(rideRequest)
                            .then(eventPublisher.publish(rideProposalBookedTopic, rideProposal.getRideProposalId(),
                                    buildBookedEvent(rideRequest, rideProposal)));
                })
                .then());
    }

    private RideProposalBookedEvent buildBookedEvent(RideRequestEntity request, RideProposalEntity proposal) {
        return RideProposalBookedEvent.newBuilder()
                .setCreatedAt(request.getCreatedAt().toString())
                .setRideRequestId(request.getRideRequestId())
                .setDriverId(proposal.getDriverId())
                .setDriverEmail(proposal.getDriverEmail())
                .setDriverFirstName(proposal.getDriverFirstName())
                .setDriverLastName(proposal.getDriverLastName())
                .setDriverPhoneNumber(proposal.getDriverPhoneNumber())
                .setPrice(proposal.getPrice())
                .setRideProposalId(proposal.getRideProposalId())
                .build();
    }
}
