package io.github.himmireda.ridehailing.rideservice.service.impl;

import io.github.himmireda.ridehailing.avro.RideProposalAcceptedEvent;
import io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent;
import io.github.himmireda.ridehailing.rideservice.dto.internal.CustomerInternalDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.AcceptRideProposalRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.CreateRideRequestRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.AcceptRideProposalResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.CreateRideRequestResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.RideProposalResponseDto;
import io.github.himmireda.ridehailing.rideservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.rideservice.exception.RideRequestNotFoundException;
import io.github.himmireda.ridehailing.rideservice.kafka.RideProposalSinkManager;
import io.github.himmireda.ridehailing.rideservice.mapper.RideRequestMapper;
import io.github.himmireda.ridehailing.rideservice.repository.RideRequestRepository;
import io.github.himmireda.ridehailing.rideservice.service.RideRequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RideRequestServiceImpl implements RideRequestService {

    @Value("${kafka.topics.ride-request-requested}")
    private String rideRequestRequestedTopicName;

    @Value("${kafka.topics.ride-proposal-accepted}")
    private String rideProposalAcceptedTopicName;


    private final RideRequestRepository rideRequestRepository;
    private final RideRequestMapper rideRequestMapper;
    private final RideProposalSinkManager rideProposalSinkManager;
    private final ReactiveKafkaProducerTemplate<String, RideRequestRequestedEvent> rideRequestRequestedProducerTemplate;
    private final ReactiveKafkaProducerTemplate<String, RideProposalAcceptedEvent> rideProposalAcceptedProducerTemplate;



    @Transactional
    @Override
    public Mono<CreateRideRequestResponseDto> createRideRequest(CustomerInternalDto customerInternalDto, CreateRideRequestRequestDto requestDto) {
        RideRequestEntity rideRequestEntity = rideRequestMapper.mapToRideRequestEntity(requestDto);
        rideRequestEntity.setCustomerId(customerInternalDto.getCustomerId());
        rideRequestEntity.setStatus(RideRequestStatus.REQUESTED);

        return rideRequestRepository.save(rideRequestEntity)
                .flatMap(rideRequest -> {
                            RideRequestRequestedEvent event = RideRequestRequestedEvent.newBuilder()
                                    .setRideRequestId(rideRequest.getRideRequestId().toString())
                                    .setCreatedAt(rideRequest.getCreatedAt().toString())
                                    .setEndLat(rideRequest.getEndLat())
                                    .setEndLng(rideRequest.getEndLng())
                                    .setPreferredPrice(rideRequest.getPreferredPrice())
                                    .setSeats(rideRequest.getSeats())
                                    .setStartLat(rideRequest.getStartLat())
                                    .setStartLng(rideRequest.getStartLng())
                                    .setCustomerId(rideRequest.getCustomerId().toString())
                                    .setCustomerEmail(customerInternalDto.getCustomerEmail())
                                    .setCustomerFirstName(customerInternalDto.getCustomerFirstName())
                                    .setCustomerLastName(customerInternalDto.getCustomerLastName())
                                    .setCustomerPhoneNumber(customerInternalDto.getCustomerPhoneNumber())
                                    .build();

                            return rideRequestRequestedProducerTemplate.send(rideRequestRequestedTopicName, rideRequest.getRideRequestId().toString(), event)
                                    .thenReturn(rideRequest)
                                    .log();
                        }
                )
                .map(rideRequestMapper::mapToCreateRideRequestResponseDto)
                .log();
    }


    @Transactional
    @Override
    public Mono<AcceptRideProposalResponseDto> acceptRideProposal(CustomerInternalDto customerInternalDto, UUID rideRequestId, AcceptRideProposalRequestDto requestDto) {
        return rideRequestRepository.findByRideRequestIdAndCustomerId(rideRequestId, customerInternalDto.getCustomerId())
                .switchIfEmpty(Mono.error(new RideRequestNotFoundException(String.format("Ride Request with id %s not found", rideRequestId))))
                .<RideRequestEntity>handle((rideRequestEntity, sink) -> {
                    if (rideRequestEntity.getStatus() == RideRequestStatus.REQUESTED) {
                        rideRequestEntity.setStatus(RideRequestStatus.ACCEPTED);
                        sink.next(rideRequestEntity);
                        return;
                    }
                    sink.error(new RideRequestNotFoundException(String.format("Ride Request with id %s is not in REQUESTED status", rideRequestId)));
                })
                .flatMap(rideRequestRepository::save)
                .flatMap(rideRequestEntity -> {
                    RideProposalAcceptedEvent event = RideProposalAcceptedEvent.newBuilder()
                            .setRideRequestId(rideRequestEntity.getRideRequestId().toString())
                            .setRideProposalId(requestDto.getRideProposalId().toString())
                            .build();
                    return rideProposalAcceptedProducerTemplate.send(rideProposalAcceptedTopicName, rideRequestEntity.getRideRequestId().toString(), event)
                            .thenReturn(rideRequestEntity)
                            .log();
                })
                .map(rideRequest -> {
                    AcceptRideProposalResponseDto acceptRideProposalResponseDto = rideRequestMapper.mapToAcceptRideProposalResponseDto(rideRequest);
                    acceptRideProposalResponseDto.setRideProposalId(requestDto.getRideProposalId());
                    return acceptRideProposalResponseDto;
                })
                .log();
    }

    @Override
    public Flux<RideProposalResponseDto> getRideProposalStream(CustomerInternalDto customerInternalDto, UUID rideRequestId) {
        Sinks.Many<RideProposalResponseDto> sink = rideProposalSinkManager.register(rideRequestId.toString());
        return sink.asFlux()
                .doOnCancel(() -> rideProposalSinkManager.unregister(rideRequestId.toString()))
                .doOnTerminate(() -> rideProposalSinkManager.unregister(rideRequestId.toString()));
    }
}
