package io.github.himmireda.ridehailing.driverservice.service;

import io.github.himmireda.ridehailing.driverservice.dto.internal.DriverInternalDto;
import io.github.himmireda.ridehailing.driverservice.dto.request.ProposeRideProposalRequestDto;
import io.github.himmireda.ridehailing.driverservice.dto.response.ProposeRideProposalResponseDto;
import reactor.core.publisher.Mono;

public interface RideProposalService {
    Mono<ProposeRideProposalResponseDto> proposeRideProposal(DriverInternalDto driverInternalDto, ProposeRideProposalRequestDto proposeRideProposalRequestDto);
}
