package fr.univ.artois.reda.driverservice.service.impl;

import fr.univ.artois.reda.avro.RideProposalProposedEvent;
import fr.univ.artois.reda.driverservice.dto.internal.DriverInternalDto;
import fr.univ.artois.reda.driverservice.dto.request.ProposeRideProposalRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideProposalResponseDto;
import fr.univ.artois.reda.driverservice.entity.RideProposalEntity;
import fr.univ.artois.reda.driverservice.enums.RideRequestStatus;
import fr.univ.artois.reda.driverservice.exception.RideRequestNotFoundException;
import fr.univ.artois.reda.driverservice.mapper.RideProposalMapper;
import fr.univ.artois.reda.driverservice.repository.RideProposalRepository;
import fr.univ.artois.reda.driverservice.repository.RideRequestRepository;
import fr.univ.artois.reda.driverservice.service.RideProposalService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RideProposalServiceImpl implements RideProposalService {
    private final RideProposalRepository rideProposalRepository;
    private final RideRequestRepository rideRequestRepository;
    private final RideProposalMapper rideProposalMapper;
    private final ReactiveKafkaProducerTemplate<String, RideProposalProposedEvent> rideProposalProposedEventReactiveKafkaProducerTemplate;

    @Value("${kafka.topics.ride-proposal-proposed}")
    private String rideProposalProposedTopicName;

    @Transactional
    @Override
    public Mono<ProposeRideProposalResponseDto> proposeRideProposal(DriverInternalDto driverInternalDto, ProposeRideProposalRequestDto proposeRideProposalRequestDto) {

        return rideRequestRepository.findByRideRequestIdAndStatus(proposeRideProposalRequestDto.getRideRequestId(), RideRequestStatus.REQUESTED)
                .switchIfEmpty(
                        Mono.error(new RideRequestNotFoundException(String.format("Ride with id: %s not found", proposeRideProposalRequestDto.getRideRequestId())))
                )
                .flatMap(ride -> {
                    RideProposalEntity rideProposalEntity = rideProposalMapper.mapToRideProposalEntity(proposeRideProposalRequestDto);
                    rideProposalEntity.setRideProposalId(UUID.randomUUID().toString());
                    rideProposalEntity.setDriverId(driverInternalDto.getDriverId().toString());
                    rideProposalEntity.setDriverEmail(driverInternalDto.getDriverEmail());
                    rideProposalEntity.setDriverFirstName(driverInternalDto.getDriverFirstName());
                    rideProposalEntity.setDriverLastName(driverInternalDto.getDriverLastName());
                    rideProposalEntity.setDriverPhoneNumber(driverInternalDto.getDriverPhoneNumber());
                    rideProposalEntity.setCreatedAt(Instant.now());
                    rideProposalEntity.setUpdatedAt(Instant.now());
                    return rideProposalRepository.save(rideProposalEntity);
                })
                .flatMap(rideProposal -> {
                    RideProposalProposedEvent event = RideProposalProposedEvent.newBuilder()
                            .setRideProposalId(rideProposal.getRideProposalId())
                            .setDriverId(driverInternalDto.getDriverId().toString())
                            .setDriverEmail(driverInternalDto.getDriverEmail())
                            .setDriverFirstName(driverInternalDto.getDriverFirstName())
                            .setDriverLastName(driverInternalDto.getDriverLastName())
                            .setDriverPhoneNumber(driverInternalDto.getDriverPhoneNumber())
                            .setRideRequestId(rideProposal.getRideRequestId())
                            .setPrice(rideProposal.getPrice())
                            .setCreatedAt(rideProposal.getCreatedAt().toString())
                            .build();
                    return rideProposalProposedEventReactiveKafkaProducerTemplate.send(rideProposalProposedTopicName, rideProposal.getRideProposalId(), event)
                            .thenReturn(rideProposalMapper.mapToProposeRideProposalResponseDto(rideProposal));
                });
    }
}
