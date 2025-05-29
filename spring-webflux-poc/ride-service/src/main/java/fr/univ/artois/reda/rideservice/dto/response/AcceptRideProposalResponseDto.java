package fr.univ.artois.reda.rideservice.dto.response;

import fr.univ.artois.reda.rideservice.enums.RideRequestStatus;
import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class AcceptRideProposalResponseDto {
    private UUID rideProposalId;
    private int seats;
    private double preferredPrice;
    private double startLat;
    private  double startLng;
    private double endLat;
    private  double endLng;
    private RideRequestStatus status;
}
