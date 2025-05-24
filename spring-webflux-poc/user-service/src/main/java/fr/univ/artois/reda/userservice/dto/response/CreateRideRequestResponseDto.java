package fr.univ.artois.reda.userservice.dto.response;

import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class CreateRideRequestResponseDto {
    private UUID id;
    private int seats;
    private double preferredPrice;
    private double startLat;
    private  double startLng;
    private double endLat;
    private  double endLng;
    private String status;
}
