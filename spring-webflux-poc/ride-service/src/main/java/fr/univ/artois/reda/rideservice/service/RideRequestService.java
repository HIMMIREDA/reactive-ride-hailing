package fr.univ.artois.reda.rideservice.service;

import fr.univ.artois.reda.rideservice.dto.internal.CustomerInternalDto;
import fr.univ.artois.reda.rideservice.dto.request.AcceptRideProposalRequestDto;
import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.RideProposalResponseDto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface RideRequestService {

    Mono<CreateRideRequestResponseDto> createRideRequest(CustomerInternalDto customerInternalDto, CreateRideRequestRequestDto requestDto);

    Mono<AcceptRideProposalResponseDto> acceptRideProposal(CustomerInternalDto customerInternalDto, UUID rideRequestId, AcceptRideProposalRequestDto requestDto);

    Flux<RideProposalResponseDto> getRideProposalStream(CustomerInternalDto customerInternalDto, UUID rideRequestId);
}
