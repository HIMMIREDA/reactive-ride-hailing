package fr.univ.artois.reda.driverservice.mapper;

import fr.univ.artois.reda.driverservice.dto.request.ProposeRideProposalRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideProposalResponseDto;
import fr.univ.artois.reda.driverservice.entity.RideProposalEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideProposalMapper {

    ProposeRideProposalResponseDto mapToProposeRideProposalResponseDto(RideProposalEntity rideProposalEntity);

    RideProposalEntity mapToRideProposalEntity(ProposeRideProposalRequestDto proposeRideProposalRequestDto);
}
