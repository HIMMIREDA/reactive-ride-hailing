package io.github.himmireda.ridehailing.driverservice.service.impl;

import io.github.himmireda.ridehailing.avro.RideProposalProposedEvent;
import io.github.himmireda.ridehailing.driverservice.dto.internal.DriverInternalDto;
import io.github.himmireda.ridehailing.driverservice.dto.request.ProposeRideProposalRequestDto;
import io.github.himmireda.ridehailing.driverservice.dto.response.ProposeRideProposalResponseDto;
import io.github.himmireda.ridehailing.driverservice.entity.RideProposalEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.driverservice.exception.RideRequestNotFoundException;
import io.github.himmireda.ridehailing.driverservice.mapper.RideProposalMapper;
import io.github.himmireda.ridehailing.driverservice.repository.RideProposalRepository;
import io.github.himmireda.ridehailing.driverservice.repository.RideRequestRepository;
import io.github.himmireda.ridehailing.driverservice.service.RideProposalService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import io.github.himmireda.ridehailing.driverservice.kafka.EventPublisher;
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
    private final EventPublisher eventPublisher;

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
                    return eventPublisher.publish(rideProposalProposedTopicName, rideProposal.getRideProposalId(), event)
                            .thenReturn(rideProposalMapper.mapToProposeRideProposalResponseDto(rideProposal));
                });
    }
}
