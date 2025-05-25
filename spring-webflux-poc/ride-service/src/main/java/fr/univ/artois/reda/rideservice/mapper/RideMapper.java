package fr.univ.artois.reda.userservice.mapper;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.userservice.entity.RideEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideMapper {

    RideEntity mapToRideRequestEntity(CreateRideRequestDto createRideRequestDto);
    CreateRideResponseDto mapToCreateRideRequestResponseDto(RideEntity rideEntity);
}
