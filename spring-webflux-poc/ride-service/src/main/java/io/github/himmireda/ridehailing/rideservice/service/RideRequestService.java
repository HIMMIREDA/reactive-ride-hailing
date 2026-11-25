package io.github.himmireda.ridehailing.rideservice.service;

import io.github.himmireda.ridehailing.rideservice.dto.internal.CustomerInternalDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.AcceptRideProposalRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.CreateRideRequestRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.AcceptRideProposalResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.CreateRideRequestResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.RideProposalResponseDto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface RideRequestService {

    Mono<CreateRideRequestResponseDto> createRideRequest(CustomerInternalDto customerInternalDto, CreateRideRequestRequestDto requestDto);

    Mono<AcceptRideProposalResponseDto> acceptRideProposal(CustomerInternalDto customerInternalDto, UUID rideRequestId, AcceptRideProposalRequestDto requestDto);

    Flux<RideProposalResponseDto> getRideProposalStream(CustomerInternalDto customerInternalDto, UUID rideRequestId);
}
