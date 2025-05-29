package fr.univ.artois.reda.driverservice.service;

import fr.univ.artois.reda.driverservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.driverservice.dto.request.ProposeRideRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideResponseDto;
import reactor.core.publisher.Mono;

public interface ProposalService {
    Mono<ProposeRideResponseDto> proposeRide(UserInternalDto userInternalDto, ProposeRideRequestDto proposeRideRequestDto);
}
