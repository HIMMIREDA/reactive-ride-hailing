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

@Document(collection = "proposals")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProposalEntity {

    @Id
    private String id;

    private String rideRequestId;

    private double price;

    private String driverId;

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;
}
