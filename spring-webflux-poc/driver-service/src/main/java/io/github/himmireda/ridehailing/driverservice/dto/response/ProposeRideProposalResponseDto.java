package io.github.himmireda.ridehailing.driverservice.dto.response;

import lombok.*;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class ProposeRideProposalResponseDto {
    private String rideProposalId;
    private String rideRequestId;
    private double price;
    private Instant createdAt;
}
