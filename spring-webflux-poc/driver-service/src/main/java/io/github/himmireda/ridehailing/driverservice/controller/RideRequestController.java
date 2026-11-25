package io.github.himmireda.ridehailing.driverservice.controller;

import io.github.himmireda.ridehailing.driverservice.dto.internal.DriverInternalDto;
import io.github.himmireda.ridehailing.driverservice.dto.request.ProposeRideProposalRequestDto;
import io.github.himmireda.ridehailing.driverservice.dto.response.ProposeRideProposalResponseDto;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.service.RideProposalService;
import io.github.himmireda.ridehailing.driverservice.service.RideRequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/rides")
@RequiredArgsConstructor
public class RideRequestController {

    private final RideRequestService rideRequestService;
    private final RideProposalService rideProposalService;

    @GetMapping(value = "/nearby/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<RideRequestEntity> getNearbyRideRequests(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader(value = "X-First-Name", defaultValue = "") String firstName,
            @RequestHeader(value = "X-Last-Name", defaultValue = "") String lastName,
            @RequestHeader(value = "X-Phone-Number", defaultValue = "") String phoneNumber,
            @RequestParam("lat") Double lat,
            @RequestParam("lng") Double lng,
            @RequestParam("radius") Integer radius
            ){
        DriverInternalDto driverInternalDto = DriverInternalDto.builder()
                .driverId(userId)
                .driverRole(role)
                .driverEmail(email)
                .driverFirstName(firstName)
                .driverLastName(lastName)
                .driverPhoneNumber(phoneNumber)
                .build();

        return rideRequestService.getNearbyRideRequests(driverInternalDto, lat, lng, radius);
    }

    @PostMapping("/proposals")
    public Mono<ProposeRideProposalResponseDto> proposeRideProposal(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader(value = "X-First-Name", defaultValue = "") String firstName,
            @RequestHeader(value = "X-Last-Name", defaultValue = "") String lastName,
            @RequestHeader(value = "X-Phone-Number", defaultValue = "") String phoneNumber,
            @RequestBody ProposeRideProposalRequestDto proposeRideProposalRequestDto){
        DriverInternalDto driverInternalDto = DriverInternalDto.builder()
                .driverId(userId)
                .driverRole(role)
                .driverEmail(email)
                .driverFirstName(firstName)
                .driverLastName(lastName)
                .driverPhoneNumber(phoneNumber)
                .build();

        return rideProposalService.proposeRideProposal(driverInternalDto, proposeRideProposalRequestDto);
    }

}
