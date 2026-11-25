package io.github.himmireda.ridehailing.rideservice.dto.request;

import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class AcceptRideProposalRequestDto {
    private UUID rideProposalId;
}
