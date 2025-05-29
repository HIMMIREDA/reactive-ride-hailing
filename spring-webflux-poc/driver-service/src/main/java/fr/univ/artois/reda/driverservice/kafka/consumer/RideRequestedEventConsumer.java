package fr.univ.artois.reda.driverservice.kafka.consumer;

import fr.univ.artois.reda.avro.RideRequestedEvent;
import fr.univ.artois.reda.driverservice.entity.RideEntity;
import fr.univ.artois.reda.driverservice.enums.RideStatus;
import fr.univ.artois.reda.driverservice.kafka.KafkaFluxRegistry;
import fr.univ.artois.reda.driverservice.repository.RideRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
@RequiredArgsConstructor
public class RideRequestedEventConsumer {
    private final KafkaFluxRegistry kafkaFluxRegistry;

    private final RideRepository rideRepository;

    @Value("${kafka.topics.ride-requested}")
    private String rideRequestedTopicName;


    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumer() {
        kafkaFluxRegistry.getFlux(rideRequestedTopicName)
                .autoConnect()
                .flatMap(r ->{
                    RideRequestedEvent event = (RideRequestedEvent) r.value();
                    RideEntity rideEntity = mapToRideEntity(event);
                    return rideRepository.save(rideEntity)
                            .doOnNext(entity -> r.receiverOffset().acknowledge());
                })
                .subscribe();
    }

    private RideEntity mapToRideEntity(RideRequestedEvent event) {
        return RideEntity.builder()
                .id(event.getId().toString())
                .createdAt(LocalDateTime.parse(event.getCreatedAt()).atZone(ZoneId.systemDefault()).toInstant())
                .email(event.getEmail().toString())
                .firstName(event.getFirstName().toString())
                .lastName(event.getLastName().toString())
                .phoneNumber(event.getPhoneNumber().toString())
                .seats(event.getSeats())
                .preferredPrice(event.getPreferredPrice())
                .status(RideStatus.valueOf(event.getStatus().toString()))
                .userId(event.getUserId().toString())
                .startLocation(new GeoJsonPoint(event.getStartLng(), event.getStartLat()))
                .endLocation(new GeoJsonPoint(event.getEndLng(), event.getEndLat()))
                .build();
    }
}
