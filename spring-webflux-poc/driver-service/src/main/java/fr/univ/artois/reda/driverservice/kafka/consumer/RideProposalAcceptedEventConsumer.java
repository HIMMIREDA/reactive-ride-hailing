package fr.univ.artois.reda.driverservice.kafka.consumer;

import fr.univ.artois.reda.avro.RideProposalAcceptedEvent;
import fr.univ.artois.reda.avro.RideProposalBookedEvent;
import fr.univ.artois.reda.driverservice.entity.RideProposalEntity;
import fr.univ.artois.reda.driverservice.entity.RideRequestEntity;
import fr.univ.artois.reda.driverservice.enums.RideRequestStatus;
import fr.univ.artois.reda.driverservice.kafka.KafkaFluxRegistry;
import fr.univ.artois.reda.driverservice.repository.RideProposalRepository;
import fr.univ.artois.reda.driverservice.repository.RideRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class RideProposalAcceptedEventConsumer {

    private final KafkaFluxRegistry kafkaFluxRegistry;
    private final RideRequestRepository rideRequestRepository;
    private final RideProposalRepository rideProposalRepository;
    private final ReactiveKafkaProducerTemplate<String, RideProposalBookedEvent> rideProposalBookedEventProducer;

    @Value("${kafka.topics.ride-proposal-accepted}")
    private String rideProposalAcceptedTopic;

    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopic;

    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumer() {
        kafkaFluxRegistry.getFlux(rideProposalAcceptedTopic)
                .autoConnect()
                .flatMap(record -> {
                    RideProposalAcceptedEvent event = (RideProposalAcceptedEvent) record.value();
                    String rideRequestId = event.getRideRequestId().toString();
                    String rideProposalId = event.getRideProposalId().toString();

                    return Mono.zip(
                                    rideRequestRepository.findById(rideRequestId),
                                    rideProposalRepository.findById(rideProposalId)
                            )
                            .switchIfEmpty(Mono.empty())
                            .flatMap(tuple -> {
                                RideRequestEntity rideRequest = tuple.getT1();
                                RideProposalEntity rideProposal = tuple.getT2();

                                if (rideRequest == null || rideProposal == null) {
                                    return Mono.empty();
                                }

                                rideRequest.setStatus(RideRequestStatus.BOOKED);
                                rideRequest.setAcceptedRideProposalId(rideProposal.getRideProposalId().toString());

                                return rideRequestRepository.save(rideRequest)
                                        .doOnSuccess(__ -> record.receiverOffset().acknowledge())
                                        .flatMap(__ -> {
                                            RideProposalBookedEvent eventToSend = buildBookedEvent(rideRequest, rideProposal);
                                            return rideProposalBookedEventProducer
                                                    .send(rideProposalBookedTopic, rideProposal.getRideProposalId(), eventToSend)
                                                    .then();
                                        });
                            });
                })
                .log()
                .subscribe();
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
