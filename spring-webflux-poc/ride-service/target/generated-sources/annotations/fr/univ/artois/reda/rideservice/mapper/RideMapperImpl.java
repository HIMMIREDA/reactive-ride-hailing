package fr.univ.artois.reda.rideservice.mapper;

import fr.univ.artois.reda.rideservice.dto.request.CreateRideRequestDto;
import fr.univ.artois.reda.rideservice.dto.response.AcceptRideProposalResponseDto;
import fr.univ.artois.reda.rideservice.dto.response.CreateRideResponseDto;
import fr.univ.artois.reda.rideservice.entity.RideEntity;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-05-25T10:11:19+0100",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.5 (Amazon.com Inc.)"
)
@Component
public class RideMapperImpl implements RideMapper {

    @Override
    public RideEntity mapToRideRequestEntity(CreateRideRequestDto createRideRequestDto) {
        if ( createRideRequestDto == null ) {
            return null;
        }

        RideEntity.RideEntityBuilder rideEntity = RideEntity.builder();

        rideEntity.seats( createRideRequestDto.getSeats() );
        rideEntity.preferredPrice( createRideRequestDto.getPreferredPrice() );
        rideEntity.startLat( createRideRequestDto.getStartLat() );
        rideEntity.startLng( createRideRequestDto.getStartLng() );
        rideEntity.endLat( createRideRequestDto.getEndLat() );
        rideEntity.endLng( createRideRequestDto.getEndLng() );

        return rideEntity.build();
    }

    @Override
    public CreateRideResponseDto mapToCreateRideRequestResponseDto(RideEntity rideEntity) {
        if ( rideEntity == null ) {
            return null;
        }

        CreateRideResponseDto.CreateRideResponseDtoBuilder createRideResponseDto = CreateRideResponseDto.builder();

        createRideResponseDto.id( rideEntity.getId() );
        createRideResponseDto.seats( rideEntity.getSeats() );
        createRideResponseDto.preferredPrice( rideEntity.getPreferredPrice() );
        createRideResponseDto.startLat( rideEntity.getStartLat() );
        createRideResponseDto.startLng( rideEntity.getStartLng() );
        createRideResponseDto.endLat( rideEntity.getEndLat() );
        createRideResponseDto.endLng( rideEntity.getEndLng() );
        createRideResponseDto.status( rideEntity.getStatus() );

        return createRideResponseDto.build();
    }

    @Override
    public AcceptRideProposalResponseDto mapToAcceptRideResponseDto(RideEntity rideEntity) {
        if ( rideEntity == null ) {
            return null;
        }

        AcceptRideProposalResponseDto.AcceptRideProposalResponseDtoBuilder acceptRideProposalResponseDto = AcceptRideProposalResponseDto.builder();

        acceptRideProposalResponseDto.id( rideEntity.getId() );
        acceptRideProposalResponseDto.seats( rideEntity.getSeats() );
        acceptRideProposalResponseDto.preferredPrice( rideEntity.getPreferredPrice() );
        acceptRideProposalResponseDto.startLat( rideEntity.getStartLat() );
        acceptRideProposalResponseDto.startLng( rideEntity.getStartLng() );
        acceptRideProposalResponseDto.endLat( rideEntity.getEndLat() );
        acceptRideProposalResponseDto.endLng( rideEntity.getEndLng() );
        acceptRideProposalResponseDto.status( rideEntity.getStatus() );

        return acceptRideProposalResponseDto.build();
    }
}
