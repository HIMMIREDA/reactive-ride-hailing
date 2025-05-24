    package fr.univ.artois.reda.userservice.entity;

    import fr.univ.artois.reda.userservice.enums.Status;
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
    @Getter @Setter
    @Builder
    @Table("ride_requests")
    public class RideRequestEntity {
        @Id
        private UUID id;

        private UUID userId;

        private int seats;

        private double preferredPrice;

        private double startLat;

        private  double startLng;

        private double endLat;

        private  double endLng;

        private Status status;

        @CreatedDate
        private LocalDateTime createdAt;

        @LastModifiedDate
        private LocalDateTime updatedAt;


    }
