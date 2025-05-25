package fr.univ.artois.reda.rideservice.service;

import fr.univ.artois.reda.rideservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.rideservice.dto.request.AcceptRideProposalRequestDto;
import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.RideProposalResponseDto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface RideService {

    Mono<CreateRideResponseDto> createRideRequest(UserInternalDto userInternalDto, CreateRideRequestDto requestDto);

    Mono<AcceptRideProposalResponseDto> acceptRide(UserInternalDto userInternalDto, UUID rideId, AcceptRideProposalRequestDto requestDto);

    Flux<RideProposalResponseDto> getRideProposalStream(UserInternalDto userInternalDto, UUID rideId);
}
