package fr.univ.artois.reda.rideservice.controller;

import fr.univ.artois.reda.rideservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.rideservice.dto.request.AcceptRideProposalRequestDto;
import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.RideProposalResponseDto;
import fr.univ.artois.reda.rideservice.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/rides")
@RequiredArgsConstructor
public class RideController {
    private final RideService rideService;

    @PostMapping
    public Mono<CreateRideResponseDto> createRideRequest(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader("X-First-Name") String firstName,
            @RequestHeader("X-Last-Name") String lastName,
            @RequestHeader("X-Phone-Number") String phoneNumber,
            @RequestBody CreateRideRequestDto requestDto
    ) {
        UserInternalDto userInternalDto = UserInternalDto.builder()
                .userId(userId)
                .role(role)
                .email(email)
                .firstName(firstName)
                .lastName(lastName)
                .phoneNumber(phoneNumber)
                .build();
        return rideService.createRideRequest(userInternalDto, requestDto);
    }

    @PutMapping("/{rideId}/accept")
    public Mono<AcceptRideProposalResponseDto> acceptRideRequest(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader("X-First-Name") String firstName,
            @RequestHeader("X-Last-Name") String lastName,
            @RequestHeader("X-Phone-Number") String phoneNumber,
            @PathVariable UUID rideId,
            @RequestBody AcceptRideProposalRequestDto acceptRideProposalRequestDto
    ){

        UserInternalDto userInternalDto = UserInternalDto.builder()
                .userId(userId)
                .role(role)
                .email(email)
                .firstName(firstName)
                .lastName(lastName)
                .phoneNumber(phoneNumber)
                .build();
        return rideService.acceptRide(userInternalDto, rideId, acceptRideProposalRequestDto);
    }

    @GetMapping(value = "/{rideId}/proposals/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<RideProposalResponseDto> getRideProposalStream(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader("X-First-Name") String firstName,
            @RequestHeader("X-Last-Name") String lastName,
            @RequestHeader("X-Phone-Number") String phoneNumber,
            @PathVariable UUID rideId
            ){

        UserInternalDto userInternalDto = UserInternalDto.builder()
                .userId(userId)
                .role(role)
                .email(email)
                .firstName(firstName)
                .lastName(lastName)
                .phoneNumber(phoneNumber)
                .build();
        return rideService.getRideProposalStream(userInternalDto, rideId);
    }

}
