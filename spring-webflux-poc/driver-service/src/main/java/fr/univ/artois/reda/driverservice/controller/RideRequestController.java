package fr.univ.artois.reda.driverservice.controller;

import fr.univ.artois.reda.driverservice.dto.internal.DriverInternalDto;
import fr.univ.artois.reda.driverservice.dto.request.ProposeRideProposalRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideProposalResponseDto;
import fr.univ.artois.reda.driverservice.entity.RideRequestEntity;
import fr.univ.artois.reda.driverservice.service.RideProposalService;
import fr.univ.artois.reda.driverservice.service.RideRequestService;
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
            @RequestHeader("X-First-Name") String firstName,
            @RequestHeader("X-Last-Name") String lastName,
            @RequestHeader("X-Phone-Number") String phoneNumber,
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
            @RequestHeader("X-First-Name") String firstName,
            @RequestHeader("X-Last-Name") String lastName,
            @RequestHeader("X-Phone-Number") String phoneNumber,
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
