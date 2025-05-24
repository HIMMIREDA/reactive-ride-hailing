package fr.univ.artois.reda.userservice.mapper;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.userservice.entity.RideRequestEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideRequestMapper {

    RideRequestEntity mapToRideRequestEntity(CreateRideRequestRequestDto createRideRequestRequestDto);
    CreateRideRequestResponseDto mapToCreateRideRequestResponseDto(RideRequestEntity rideRequestEntity);
}
