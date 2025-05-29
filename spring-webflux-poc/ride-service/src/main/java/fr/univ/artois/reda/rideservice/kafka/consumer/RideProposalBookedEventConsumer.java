package fr.univ.artois.reda.rideservice.kafka.consumer;

import fr.univ.artois.reda.avro.RideProposalBookedEvent;
import fr.univ.artois.reda.rideservice.entity.BookedRideProposalEntity;
import fr.univ.artois.reda.rideservice.enums.RideRequestStatus;
import fr.univ.artois.reda.rideservice.repository.BookedRideProposalRepository;
import fr.univ.artois.reda.rideservice.repository.RideRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

@RequiredArgsConstructor
@Component
public class RideProposalBookedEventConsumer {

    private final BookedRideProposalRepository bookedRideProposalRepository;
    private final RideRequestRepository rideRequestRepository;
    private final ReactiveKafkaConsumerTemplate<String, RideProposalBookedEvent> reactiveRideProposalBookedKafkaConsumerTemplate;
    private final TransactionalOperator transactionalOperator;

    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumer() {
        reactiveRideProposalBookedKafkaConsumerTemplate
                .receive()
                .flatMap(record -> {
                    RideProposalBookedEvent event = record.value();
                    BookedRideProposalEntity bookedEntity = mapToBookedRideProposalEntity(event);
                    UUID rideRequestId = UUID.fromString(event.getRideRequestId().toString());

                    return rideRequestRepository.findByRideRequestIdAndStatus(rideRequestId, RideRequestStatus.ACCEPTED)
                            .flatMap(rideRequest -> {
                                rideRequest.setAcceptedRideProposalId(bookedEntity.getRideProposalId());
                                rideRequest.setStatus(RideRequestStatus.BOOKED);
                                return rideRequestRepository.save(rideRequest);
                            })
                            .flatMap(savedRideRequest -> bookedRideProposalRepository.save(bookedEntity))
                            .then()
                            .as(transactionalOperator::transactional)
                            .doOnSuccess(unused -> record.receiverOffset().acknowledge());
                })
                .log()
                .subscribe();
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
