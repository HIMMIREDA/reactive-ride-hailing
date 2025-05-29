package fr.univ.artois.reda.driverservice.controller;

import fr.univ.artois.reda.driverservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.driverservice.dto.request.ProposeRideRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideResponseDto;
import fr.univ.artois.reda.driverservice.entity.ProposalEntity;
import fr.univ.artois.reda.driverservice.entity.RideEntity;
import fr.univ.artois.reda.driverservice.service.RideService;
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


    @GetMapping(value = "/nearby/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<RideEntity> getNearbyRides(
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
        UserInternalDto userInternalDto = UserInternalDto.builder()
                .userId(userId)
                .role(role)
                .email(email)
                .firstName(firstName)
                .lastName(lastName)
                .phoneNumber(phoneNumber)
                .build();

        return rideService.getNearbyRides(userInternalDto, lat, lng, radius);
    }

    @PostMapping("/api/rides/{rideId}/proposals")
    public Mono<ProposeRideResponseDto> proposeRide(ProposeRideRequestDto proposeRideRequestDto){
        return
    }

}
