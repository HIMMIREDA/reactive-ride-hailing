package fr.univ.artois.reda.driverservice.entity;


import fr.univ.artois.reda.driverservice.enums.RideRequestStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document(collection = "ride_requests")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RideRequestEntity {

    @Id
    private String rideRequestId;

    private String customerId;

    private int seats;

    private double preferredPrice;

    @GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE)
    private GeoJsonPoint startLocation;

    @GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE)
    private GeoJsonPoint endLocation;

    private RideRequestStatus status;

    private Instant createdAt;

    private String customerEmail;
    private String customerFirstName;
    private String customerLastName;
    private String customerPhoneNumber;

    private String acceptedRideProposalId;
}
