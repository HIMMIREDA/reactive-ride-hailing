package fr.univ.artois.reda.userservice.service;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideRequestResponseDto;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface RideRequestService {

    Mono<CreateRideRequestResponseDto> createRideRequest(UUID userId, CreateRideRequestRequestDto requestDto);

}
