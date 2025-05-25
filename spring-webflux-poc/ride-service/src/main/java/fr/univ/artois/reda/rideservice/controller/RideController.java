package fr.univ.artois.reda.userservice.controller;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.userservice.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/rides")
@RequiredArgsConstructor
public class RideController {
    private final RideService rideService;

    @Value("${kafka.topics.ride-requested}")
    private String rideRequestedTopicName;

    @PostMapping
    public Mono<CreateRideResponseDto> createRideRequest(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestBody CreateRideRequestDto requestDto
    ) {
        return rideService.createRideRequest(userId, requestDto);
    }

}
