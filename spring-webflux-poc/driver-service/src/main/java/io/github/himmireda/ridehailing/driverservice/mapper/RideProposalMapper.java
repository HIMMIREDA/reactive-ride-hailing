package io.github.himmireda.ridehailing.driverservice.mapper;

import io.github.himmireda.ridehailing.driverservice.dto.request.ProposeRideProposalRequestDto;
import io.github.himmireda.ridehailing.driverservice.dto.response.ProposeRideProposalResponseDto;
import io.github.himmireda.ridehailing.driverservice.entity.RideProposalEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideProposalMapper {

    ProposeRideProposalResponseDto mapToProposeRideProposalResponseDto(RideProposalEntity rideProposalEntity);

    RideProposalEntity mapToRideProposalEntity(ProposeRideProposalRequestDto proposeRideProposalRequestDto);
}
