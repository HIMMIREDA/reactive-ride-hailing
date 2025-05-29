package fr.univ.artois.reda.driverservice.service;

import fr.univ.artois.reda.driverservice.dto.internal.DriverInternalDto;
import fr.univ.artois.reda.driverservice.dto.request.ProposeRideProposalRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideProposalResponseDto;
import reactor.core.publisher.Mono;

public interface RideProposalService {
    Mono<ProposeRideProposalResponseDto> proposeRideProposal(DriverInternalDto driverInternalDto, ProposeRideProposalRequestDto proposeRideProposalRequestDto);
}
