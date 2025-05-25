package fr.univ.artois.reda.rideservice.mapper;

import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.rideservice.entity.RideEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideMapper {

    RideEntity mapToRideRequestEntity(CreateRideRequestDto createRideRequestDto);
    CreateRideResponseDto mapToCreateRideRequestResponseDto(RideEntity rideEntity);
    AcceptRideProposalResponseDto mapToAcceptRideResponseDto(RideEntity rideEntity);
}
