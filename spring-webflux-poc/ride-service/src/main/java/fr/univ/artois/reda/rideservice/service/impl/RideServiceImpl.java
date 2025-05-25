package fr.univ.artois.reda.userservice.service.impl;

import fr.univ.artois.reda.avro.RideRequestedEvent;
import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.userservice.entity.RideEntity;
import fr.univ.artois.reda.userservice.enums.RideStatus;
import fr.univ.artois.reda.userservice.mapper.RideMapper;
import fr.univ.artois.reda.userservice.repository.RideRepository;
import fr.univ.artois.reda.userservice.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RideServiceImpl implements RideService {

    private final RideRepository rideRepository;
    private final RideMapper rideMapper;
    private final ReactiveKafkaProducerTemplate<String, RideRequestedEvent> reactiveKafkaProducerTemplate;
    @Value("${kafka.topics.ride-requested}")
    private String rideRequestedTopicName;

    @Transactional
    @Override
    public Mono<CreateRideResponseDto> createRideRequest(UUID userId, CreateRideRequestDto requestDto) {
        RideEntity rideEntity = rideMapper.mapToRideRequestEntity(requestDto);
        rideEntity.setUserId(userId);
        rideEntity.setRideStatus(RideStatus.REQUESTED);

        return rideRepository.save(rideEntity)
                .flatMap(ride ->{
                    RideRequestedEvent rideRequestedEvent = RideRequestedEvent.newBuilder()
                            .setId(ride.getId().toString())
                            .setCreatedAt(ride.getCreatedAt().toString())
                            .setEndLat(ride.getEndLat())
                            .setEndLng(ride.getEndLng())
                            .setPreferredPrice(ride.getPreferredPrice())
                            .setSeats(ride.getSeats())
                            .setStartLat(ride.getStartLat())
                            .setStartLng(ride.getStartLng())
                            .setStatus(fr.univ.artois.reda.avro.RideStatus.REQUESTED)
                            .setUserId(ride.getUserId().toString())
                            .build();

                    return reactiveKafkaProducerTemplate.send(rideRequestedTopicName, ride.getId().toString(), rideRequestedEvent)
                            .map(sendResult -> ride);
                        }
                )
                .map(rideMapper::mapToCreateRideRequestResponseDto)
                .log();
    }
}
