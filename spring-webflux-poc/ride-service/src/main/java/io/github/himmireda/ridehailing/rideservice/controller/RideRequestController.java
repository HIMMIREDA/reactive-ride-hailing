package io.github.himmireda.ridehailing.rideservice.controller;

import io.github.himmireda.ridehailing.rideservice.dto.internal.CustomerInternalDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.AcceptRideProposalRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.CreateRideRequestRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.AcceptRideProposalResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.CreateRideRequestResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.RideProposalResponseDto;
import io.github.himmireda.ridehailing.rideservice.service.RideRequestService;
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

    @PostMapping
    public Mono<CreateRideRequestResponseDto> createRideRequest(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader(value = "X-First-Name", defaultValue = "") String firstName,
            @RequestHeader(value = "X-Last-Name", defaultValue = "") String lastName,
            @RequestHeader(value = "X-Phone-Number", defaultValue = "") String phoneNumber,
            @RequestBody CreateRideRequestRequestDto requestDto
    ) {
        CustomerInternalDto customerInternalDto = CustomerInternalDto.builder()
                .customerId(userId)
                .customerRole(role)
                .customerEmail(email)
                .customerFirstName(firstName)
                .customerLastName(lastName)
                .customerPhoneNumber(phoneNumber)
                .build();
        return rideRequestService.createRideRequest(customerInternalDto, requestDto);
    }

    @PutMapping("/{rideRequestId}/accept")
    public Mono<AcceptRideProposalResponseDto> acceptRidProposal(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader(value = "X-First-Name", defaultValue = "") String firstName,
            @RequestHeader(value = "X-Last-Name", defaultValue = "") String lastName,
            @RequestHeader(value = "X-Phone-Number", defaultValue = "") String phoneNumber,
            @PathVariable UUID rideRequestId,
            @RequestBody AcceptRideProposalRequestDto acceptRideProposalRequestDto
    ){

        CustomerInternalDto customerInternalDto = CustomerInternalDto.builder()
                .customerId(userId)
                .customerRole(role)
                .customerEmail(email)
                .customerFirstName(firstName)
                .customerLastName(lastName)
                .customerPhoneNumber(phoneNumber)
                .build();
        return rideRequestService.acceptRideProposal(customerInternalDto, rideRequestId, acceptRideProposalRequestDto);
    }

    @GetMapping(value = "/{rideRequestId}/proposals/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<RideProposalResponseDto> getRideProposalStream(
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("X-Role") String role,
            @RequestHeader("X-Email") String email,
            @RequestHeader(value = "X-First-Name", defaultValue = "") String firstName,
            @RequestHeader(value = "X-Last-Name", defaultValue = "") String lastName,
            @RequestHeader(value = "X-Phone-Number", defaultValue = "") String phoneNumber,
            @PathVariable UUID rideRequestId
            ){

        CustomerInternalDto customerInternalDto = CustomerInternalDto.builder()
                .customerId(userId)
                .customerRole(role)
                .customerEmail(email)
                .customerFirstName(firstName)
                .customerLastName(lastName)
                .customerPhoneNumber(phoneNumber)
                .build();
        return rideRequestService.getRideProposalStream(customerInternalDto, rideRequestId);
    }

}
