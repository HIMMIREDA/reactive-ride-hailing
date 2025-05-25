package fr.univ.artois.reda.rideservice.dto.response;

import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class RideProposalResponseDto {
    private UUID driverId;
    private double price;
    private UUID id;
}
