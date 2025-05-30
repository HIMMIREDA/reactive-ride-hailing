package fr.univ.artois.reda.driverservice.mapper;

import fr.univ.artois.reda.driverservice.dto.request.ProposeRideProposalRequestDto;
import fr.univ.artois.reda.driverservice.dto.response.ProposeRideProposalResponseDto;
import fr.univ.artois.reda.driverservice.entity.RideProposalEntity;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-05-30T22:18:15+0100",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.5 (Amazon.com Inc.)"
)
@Component
public class RideProposalMapperImpl implements RideProposalMapper {

    @Override
    public ProposeRideProposalResponseDto mapToProposeRideProposalResponseDto(RideProposalEntity rideProposalEntity) {
        if ( rideProposalEntity == null ) {
            return null;
        }

        ProposeRideProposalResponseDto.ProposeRideProposalResponseDtoBuilder proposeRideProposalResponseDto = ProposeRideProposalResponseDto.builder();

        proposeRideProposalResponseDto.rideProposalId( rideProposalEntity.getRideProposalId() );
        proposeRideProposalResponseDto.rideRequestId( rideProposalEntity.getRideRequestId() );
        proposeRideProposalResponseDto.price( rideProposalEntity.getPrice() );
        proposeRideProposalResponseDto.createdAt( rideProposalEntity.getCreatedAt() );

        return proposeRideProposalResponseDto.build();
    }

    @Override
    public RideProposalEntity mapToRideProposalEntity(ProposeRideProposalRequestDto proposeRideProposalRequestDto) {
        if ( proposeRideProposalRequestDto == null ) {
            return null;
        }

        RideProposalEntity.RideProposalEntityBuilder rideProposalEntity = RideProposalEntity.builder();

        rideProposalEntity.rideRequestId( proposeRideProposalRequestDto.getRideRequestId() );
        rideProposalEntity.price( proposeRideProposalRequestDto.getPrice() );

        return rideProposalEntity.build();
    }
}
