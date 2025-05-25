package fr.univ.artois.reda.rideservice.service.impl;

import fr.univ.artois.reda.avro.RideRequestedEvent;
import fr.univ.artois.reda.avro.RideToAcceptEvent;
import fr.univ.artois.reda.rideservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.rideservice.dto.request.AcceptRideProposalRequestDto;
import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.RideProposalResponseDto;
import fr.univ.artois.reda.rideservice.entity.RideEntity;
import fr.univ.artois.reda.rideservice.enums.RideStatus;
import fr.univ.artois.reda.rideservice.exception.RideNotFoundException;
import fr.univ.artois.reda.rideservice.mapper.RideMapper;
import fr.univ.artois.reda.rideservice.repository.RideRepository;
import fr.univ.artois.reda.rideservice.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RideServiceImpl implements RideService {

    private final RideRepository rideRepository;
    private final RideMapper rideMapper;
    private final ReactiveKafkaProducerTemplate<String, RideRequestedEvent> rideRequestedProducerTemplate;
    private final ReactiveKafkaProducerTemplate<String, RideToAcceptEvent> rideToAcceptProducerTemplate;

    @Value("${kafka.topics.ride-requested}")
    private String rideRequestedTopicName;

    @Value("${kafka.topics.ride-to-accept}")
    private String rideToAcceptTopicName;

    @Transactional
    @Override
    public Mono<CreateRideResponseDto> createRideRequest(UserInternalDto userInternalDto, CreateRideRequestDto requestDto) {
        RideEntity rideEntity = rideMapper.mapToRideRequestEntity(requestDto);
        rideEntity.setUserId(userInternalDto.getUserId());
        rideEntity.setStatus(RideStatus.REQUESTED);

        return rideRepository.save(rideEntity)
                .flatMap(ride ->{
                    RideRequestedEvent event = RideRequestedEvent.newBuilder()
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
                            .setEmail(userInternalDto.getEmail())
                            .setFirstName(userInternalDto.getFirstName())
                            .setLastName(userInternalDto.getLastName())
                            .setPhoneNumber(userInternalDto.getPhoneNumber())
                            .build();

                    return rideRequestedProducerTemplate.send(rideRequestedTopicName, ride.getId().toString(), event)
                            .thenReturn(ride)
                            .log();
                        }
                )
                .map(rideMapper::mapToCreateRideRequestResponseDto)
                .log();
    }


    @Transactional
    @Override
    public Mono<AcceptRideProposalResponseDto> acceptRide(UserInternalDto userInternalDto, UUID rideId, AcceptRideProposalRequestDto requestDto) {
        return rideRepository.findByIdAndUserId(rideId, userInternalDto.getUserId())
                .switchIfEmpty(Mono.error(new RideNotFoundException(String.format("Ride with id %s not found", rideId))))
                .<RideEntity>handle((rideEntity, sink) -> {
                    if (rideEntity.getStatus() == RideStatus.REQUESTED) {
                        rideEntity.setStatus(RideStatus.TO_ACCEPT);
                        sink.next(rideEntity);
                        return;
                    }
                    sink.error(new RideNotFoundException(String.format("Ride with id %s is not in REQUESTED status", rideId)));
                })
                .flatMap(rideRepository::save)
                .flatMap(rideEntity -> {
                    RideToAcceptEvent event = RideToAcceptEvent.newBuilder()
                            .setId(rideEntity.getId().toString())
                            .setCreatedAt(rideEntity.getCreatedAt().toString())
                            .setEndLat(rideEntity.getEndLat())
                            .setEndLng(rideEntity.getEndLng())
                            .setPreferredPrice(rideEntity.getPreferredPrice())
                            .setSeats(rideEntity.getSeats())
                            .setStartLat(rideEntity.getStartLat())
                            .setStartLng(rideEntity.getStartLng())
                            .setStatus(fr.univ.artois.reda.avro.RideStatus.TO_ACCEPT)
                            .setUserId(rideEntity.getUserId().toString())
                            .setProposalId(requestDto.getProposalId().toString())
                            .setEmail(userInternalDto.getEmail())
                            .setFirstName(userInternalDto.getFirstName())
                            .setLastName(userInternalDto.getLastName())
                            .setPhoneNumber(userInternalDto.getPhoneNumber())
                            .build();
                    return rideToAcceptProducerTemplate.send(rideToAcceptTopicName, rideEntity.getId().toString(), event)
                            .thenReturn(rideEntity)
                            .log();
                })
                .map(rideMapper::mapToAcceptRideResponseDto)
                .log();
    }

    @Override
    public Flux<RideProposalResponseDto> getRideProposalStream(UserInternalDto userInternalDto, UUID rideId) {
        return Flux.just();
    }
}
