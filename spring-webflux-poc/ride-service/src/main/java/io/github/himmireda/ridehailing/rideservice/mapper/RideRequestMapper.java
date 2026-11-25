package io.github.himmireda.ridehailing.rideservice.mapper;

import io.github.himmireda.ridehailing.rideservice.dto.request.CreateRideRequestRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.AcceptRideProposalResponseDto;
import io.github.himmireda.ridehailing.rideservice.dto.response.CreateRideRequestResponseDto;
import io.github.himmireda.ridehailing.rideservice.entity.RideRequestEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface RideRequestMapper {

    RideRequestEntity mapToRideRequestEntity(CreateRideRequestRequestDto createRideRequestRequestDto);
    CreateRideRequestResponseDto mapToCreateRideRequestResponseDto(RideRequestEntity rideRequestEntity);
    AcceptRideProposalResponseDto mapToAcceptRideProposalResponseDto(RideRequestEntity rideRequestEntity);
}
