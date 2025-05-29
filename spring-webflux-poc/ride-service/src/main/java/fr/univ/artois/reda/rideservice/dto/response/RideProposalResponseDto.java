package fr.univ.artois.reda.rideservice.dto.response;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class RideProposalResponseDto {
    private String rideProposalId;
    private String rideRequestId;
    private String driverId;
    private double price;
    private String createdAt;
    private String driverEmail;
    private String driverFirstName;
    private String driverLastName;
    private String driverPhoneNumber;
}
