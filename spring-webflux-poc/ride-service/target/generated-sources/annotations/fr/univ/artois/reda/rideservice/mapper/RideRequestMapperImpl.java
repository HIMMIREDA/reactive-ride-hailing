package fr.univ.artois.reda.rideservice.mapper;

import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.rideservice.entity.RideRequestEntity;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-05-30T22:18:20+0100",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.5 (Amazon.com Inc.)"
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

        createRideRequestResponseDto.rideRequestId( rideRequestEntity.getRideRequestId() );
        createRideRequestResponseDto.seats( rideRequestEntity.getSeats() );
        createRideRequestResponseDto.preferredPrice( rideRequestEntity.getPreferredPrice() );
        createRideRequestResponseDto.startLat( rideRequestEntity.getStartLat() );
        createRideRequestResponseDto.startLng( rideRequestEntity.getStartLng() );
        createRideRequestResponseDto.endLat( rideRequestEntity.getEndLat() );
        createRideRequestResponseDto.endLng( rideRequestEntity.getEndLng() );
        createRideRequestResponseDto.status( rideRequestEntity.getStatus() );

        return createRideRequestResponseDto.build();
    }

    @Override
    public AcceptRideProposalResponseDto mapToAcceptRideProposalResponseDto(RideRequestEntity rideRequestEntity) {
        if ( rideRequestEntity == null ) {
            return null;
        }

        AcceptRideProposalResponseDto.AcceptRideProposalResponseDtoBuilder acceptRideProposalResponseDto = AcceptRideProposalResponseDto.builder();

        acceptRideProposalResponseDto.seats( rideRequestEntity.getSeats() );
        acceptRideProposalResponseDto.preferredPrice( rideRequestEntity.getPreferredPrice() );
        acceptRideProposalResponseDto.startLat( rideRequestEntity.getStartLat() );
        acceptRideProposalResponseDto.startLng( rideRequestEntity.getStartLng() );
        acceptRideProposalResponseDto.endLat( rideRequestEntity.getEndLat() );
        acceptRideProposalResponseDto.endLng( rideRequestEntity.getEndLng() );
        acceptRideProposalResponseDto.status( rideRequestEntity.getStatus() );

        return acceptRideProposalResponseDto.build();
    }
}
