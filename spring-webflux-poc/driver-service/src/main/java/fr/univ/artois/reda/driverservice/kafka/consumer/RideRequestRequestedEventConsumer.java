package fr.univ.artois.reda.driverservice.kafka.consumer;

import fr.univ.artois.reda.avro.RideRequestRequestedEvent;
import fr.univ.artois.reda.driverservice.entity.RideRequestEntity;
import fr.univ.artois.reda.driverservice.enums.RideRequestStatus;
import fr.univ.artois.reda.driverservice.kafka.KafkaFluxRegistry;
import fr.univ.artois.reda.driverservice.repository.RideRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class RideRequestRequestedEventConsumer {
    private final KafkaFluxRegistry kafkaFluxRegistry;

    private final RideRequestRepository rideRequestRepository;

    @Value("${kafka.topics.ride-request-requested}")
    private String rideRequestRequestedTopicName;


    @Transactional
    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumer() {
        kafkaFluxRegistry.getFlux(rideRequestRequestedTopicName)
                .autoConnect()
                .flatMap(r ->{
                    RideRequestRequestedEvent event = (RideRequestRequestedEvent) r.value();
                    RideRequestEntity rideRequestEntity = mapToRideEntity(event);
                    return rideRequestRepository.save(rideRequestEntity)
                            .doOnNext(entity -> r.receiverOffset().acknowledge());
                })
                .subscribe();
    }

    private RideRequestEntity mapToRideEntity(RideRequestRequestedEvent event) {
        return RideRequestEntity.builder()
                .rideRequestId(event.getRideRequestId().toString())
                .createdAt(LocalDateTime.parse(event.getCreatedAt()).atZone(ZoneId.systemDefault()).toInstant())
                .customerEmail(event.getCustomerEmail().toString())
                .customerFirstName(event.getCustomerFirstName().toString())
                .customerLastName(event.getCustomerLastName().toString())
                .customerPhoneNumber(event.getCustomerPhoneNumber().toString())
                .seats(event.getSeats())
                .preferredPrice(event.getPreferredPrice())
                .status(RideRequestStatus.REQUESTED)
                .customerId(event.getCustomerId().toString())
                .startLocation(new GeoJsonPoint(event.getStartLng(), event.getStartLat()))
                .endLocation(new GeoJsonPoint(event.getEndLng(), event.getEndLat()))
                .build();
    }
}
