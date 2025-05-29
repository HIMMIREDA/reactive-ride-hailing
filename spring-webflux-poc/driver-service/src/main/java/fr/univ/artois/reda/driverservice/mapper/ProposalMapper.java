package fr.univ.artois.reda.driverservice.mapper;

import fr.univ.artois.reda.driverservice.dto.request.ProposeRideRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideResponseDto;
import fr.univ.artois.reda.driverservice.entity.ProposalEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface ProposalMapper {

    ProposeRideResponseDto mapToProposeRideResponseDto(ProposalEntity proposalEntity);

    ProposalEntity mapToProposalEntity(ProposeRideRequestDto proposeRideRequestDto);
}
