package fr.univ.artois.reda.driverservice.dto.request;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class ProposeRideRequestDto {
    private String rideRequestId;
    private double price;
}
