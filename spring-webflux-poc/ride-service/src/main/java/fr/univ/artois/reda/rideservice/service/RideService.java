package fr.univ.artois.reda.userservice.service;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideResponseDto;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface RideService {

    Mono<CreateRideResponseDto> createRideRequest(UUID userId, CreateRideRequestDto requestDto);

}
