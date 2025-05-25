package fr.univ.artois.reda.userservice.entity;

import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Table("accepted_proposals")
public class AcceptedProposalEntity {
    @Id
    private UUID id;


    private UUID rideRequestId;

    private UUID proposalId;

    private UUID driverId;

    private double price;

    private LocalDateTime acceptedAt;

    @CreatedDate
    private LocalDateTime createdAt;

    @LastModifiedDate
    private LocalDateTime updatedAt;
}
