package fr.univ.artois.reda.rideservice.mapper;

import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.rideservice.entity.RideRequestEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideRequestMapper {

    RideRequestEntity mapToRideRequestEntity(CreateRideRequestRequestDto createRideRequestRequestDto);
    CreateRideRequestResponseDto mapToCreateRideRequestResponseDto(RideRequestEntity rideRequestEntity);
    AcceptRideProposalResponseDto mapToAcceptRideProposalResponseDto(RideRequestEntity rideRequestEntity);
}
