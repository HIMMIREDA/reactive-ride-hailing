    package fr.univ.artois.reda.rideservice.entity;

    import fr.univ.artois.reda.rideservice.enums.RideStatus;
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
    @Table("rides")
    public class RideEntity {
        @Id
        private UUID id;

        private UUID userId;

        private int seats;

        private double preferredPrice;

        private double startLat;

        private  double startLng;

        private double endLat;

        private  double endLng;

        private RideStatus status;

        private UUID acceptedProposalId;

        @CreatedDate
        private LocalDateTime createdAt;

        @LastModifiedDate
        private LocalDateTime updatedAt;


    }
