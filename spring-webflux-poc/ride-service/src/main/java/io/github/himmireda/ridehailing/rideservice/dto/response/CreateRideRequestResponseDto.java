package io.github.himmireda.ridehailing.rideservice.dto.response;

import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class CreateRideRequestResponseDto {
    private UUID rideRequestId;
    private int seats;
    private double preferredPrice;
    private double startLat;
    private  double startLng;
    private double endLat;
    private  double endLng;
    private RideRequestStatus status;
}
