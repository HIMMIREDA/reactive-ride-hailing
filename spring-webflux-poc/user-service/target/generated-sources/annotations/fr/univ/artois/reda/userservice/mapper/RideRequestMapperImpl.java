package fr.univ.artois.reda.userservice.mapper;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.userservice.entity.RideRequestEntity;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-05-24T02:47:59+0200",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.2 (Amazon.com Inc.)"
)
@Component
public class RideRequestMapperImpl implements RideRequestMapper {

    @Override
    public RideRequestEntity mapToRideRequestEntity(CreateRideRequestRequestDto createRideRequestRequestDto) {
        if ( createRideRequestRequestDto == null ) {
            return null;
        }

        RideRequestEntity.RideRequestEntityBuilder rideRequestEntity = RideRequestEntity.builder();

        rideRequestEntity.seats( createRideRequestRequestDto.getSeats() );
        rideRequestEntity.preferredPrice( createRideRequestRequestDto.getPreferredPrice() );
        rideRequestEntity.startLat( createRideRequestRequestDto.getStartLat() );
        rideRequestEntity.startLng( createRideRequestRequestDto.getStartLng() );
        rideRequestEntity.endLat( createRideRequestRequestDto.getEndLat() );
        rideRequestEntity.endLng( createRideRequestRequestDto.getEndLng() );

        return rideRequestEntity.build();
    }

    @Override
    public CreateRideRequestResponseDto mapToCreateRideRequestResponseDto(RideRequestEntity rideRequestEntity) {
        if ( rideRequestEntity == null ) {
            return null;
        }

        CreateRideRequestResponseDto.CreateRideRequestResponseDtoBuilder createRideRequestResponseDto = CreateRideRequestResponseDto.builder();

        createRideRequestResponseDto.id( rideRequestEntity.getId() );
        createRideRequestResponseDto.seats( rideRequestEntity.getSeats() );
        createRideRequestResponseDto.preferredPrice( rideRequestEntity.getPreferredPrice() );
        createRideRequestResponseDto.startLat( rideRequestEntity.getStartLat() );
        createRideRequestResponseDto.startLng( rideRequestEntity.getStartLng() );
        createRideRequestResponseDto.endLat( rideRequestEntity.getEndLat() );
        createRideRequestResponseDto.endLng( rideRequestEntity.getEndLng() );
        if ( rideRequestEntity.getStatus() != null ) {
            createRideRequestResponseDto.status( rideRequestEntity.getStatus().name() );
        }

        return createRideRequestResponseDto.build();
    }
}
