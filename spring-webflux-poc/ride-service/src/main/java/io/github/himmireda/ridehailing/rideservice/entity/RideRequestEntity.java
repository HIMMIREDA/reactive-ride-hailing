    package io.github.himmireda.ridehailing.rideservice.entity;

    import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
    import lombok.*;
    import org.springframework.data.annotation.CreatedDate;
    import org.springframework.data.annotation.Id;
    import org.springframework.data.annotation.LastModifiedDate;
    import org.springframework.data.relational.core.mapping.Table;

    import java.time.LocalDateTime;
    import java.util.UUID;

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter @Setter
    @Builder
    @Table("ride_requests")
    public class RideRequestEntity {
        @Id
        private UUID rideRequestId;

        private UUID customerId;

        private int seats;

        private double preferredPrice;

        private double startLat;

        private  double startLng;

        private double endLat;

        private  double endLng;

        private RideRequestStatus status;

        private UUID acceptedRideProposalId;

        @CreatedDate
        private LocalDateTime createdAt;

        @LastModifiedDate
        private LocalDateTime updatedAt;


    }
