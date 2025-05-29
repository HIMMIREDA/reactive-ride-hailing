package fr.univ.artois.reda.driverservice.service.impl;

import fr.univ.artois.reda.avro.RideProposalEvent;
import fr.univ.artois.reda.driverservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.driverservice.dto.request.ProposeRideRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideResponseDto;
import fr.univ.artois.reda.driverservice.entity.ProposalEntity;
import fr.univ.artois.reda.driverservice.enums.RideStatus;
import fr.univ.artois.reda.driverservice.exception.RideNotFoundException;
import fr.univ.artois.reda.driverservice.mapper.ProposalMapper;
import fr.univ.artois.reda.driverservice.repository.ProposalRepository;
import fr.univ.artois.reda.driverservice.repository.RideRepository;
import fr.univ.artois.reda.driverservice.service.ProposalService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ProposalServiceImpl implements ProposalService {
    private final ProposalRepository proposalRepository;
    private final RideRepository rideRepository;
    private final ProposalMapper proposalMapper;
    private final ReactiveKafkaProducerTemplate<String, RideProposalEvent> proposalEventProducerTemplate;

    @Value("${kafka.topics.ride-proposal}")
    private String rideProposalTopicName;

    @Transactional
    @Override
    public Mono<ProposeRideResponseDto> proposeRide(UserInternalDto userInternalDto, ProposeRideRequestDto proposeRideRequestDto) {
        ProposalEntity rideEntity = proposalMapper.mapToProposalEntity(proposeRideRequestDto);

        return rideRepository.findByIdAndStatus(proposeRideRequestDto.getRideRequestId(), RideStatus.REQUESTED)
                .switchIfEmpty(
                        Mono.error(new RideNotFoundException(String.format("Ride with id: %s not found", proposeRideRequestDto.getRideRequestId())))
                )
                .flatMap(ride -> {
                    ProposalEntity proposal = proposalMapper.mapToProposalEntity(proposeRideRequestDto);
                    proposal.setDriverId(userInternalDto.getUserId().toString());
                    return proposalRepository.save(proposal);
                })
                .flatMap(proposal -> {
                    RideProposalEvent event = RideProposalEvent.newBuilder()
                            .setId(proposal.getId())
                            .setDriverId(userInternalDto.getUserId().toString())
                            .setEmail(userInternalDto.getEmail())
                            .setFirstName(userInternalDto.getFirstName())
                            .setLastName(userInternalDto.getLastName())
                            .setPhoneNumber(userInternalDto.getPhoneNumber())
                            .setRideRequestId(proposal.getRideRequestId())
                            .setPrice(proposal.getPrice())
                            .setCreatedAt(proposal.getCreatedAt().toString())
                            .build();
                    return proposalEventProducerTemplate.send(rideProposalTopicName, event)
                            .thenReturn(proposalMapper.mapToProposeRideResponseDto(proposal));
                });
    }
}
