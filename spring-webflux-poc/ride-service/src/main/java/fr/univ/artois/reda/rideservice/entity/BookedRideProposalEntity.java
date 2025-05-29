package fr.univ.artois.reda.rideservice.entity;

import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Table("booked_ride_proposals")
public class BookedRideProposalEntity {
    @Id
    private UUID id;

    private UUID rideProposalId;
    private UUID driverId;
    private String driverFirstName;
    private String driverLastName;
    private String driverEmail;
    private String driverPhoneNumber;

    private double price;

    private LocalDateTime acceptedAt;

    @CreatedDate
    private LocalDateTime createdAt;

    @LastModifiedDate
    private LocalDateTime updatedAt;
}
