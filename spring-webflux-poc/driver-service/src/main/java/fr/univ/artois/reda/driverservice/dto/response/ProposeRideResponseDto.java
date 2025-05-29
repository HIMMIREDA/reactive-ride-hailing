package fr.univ.artois.reda.driverservice.dto.response;

import lombok.*;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class ProposeRideResponseDto {
    private String id;
    private String rideRequestId;
    private double price;
    private Instant createdAt;
}
