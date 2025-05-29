package fr.univ.artois.reda.driverservice.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document(collection = "ride_proposals")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RideProposalEntity {

    @Id
    private String rideProposalId;

    private String rideRequestId;

    private double price;

    private String driverId;
    private String driverFirstName;
    private String driverLastName;
    private String driverEmail;
    private String driverPhoneNumber;

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;
}
