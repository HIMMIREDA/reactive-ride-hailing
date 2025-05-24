package fr.univ.artois.reda.userservice.controller;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.userservice.service.RideRequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/ride-requests")
@RequiredArgsConstructor
public class RideRequestController {
    private final RideRequestService rideRequestService;

    @PostMapping
    public Mono<CreateRideRequestResponseDto> createRideRequest(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestBody CreateRideRequestRequestDto requestDto
    ){
        return rideRequestService.createRideRequest(userId, requestDto);
    }


}
