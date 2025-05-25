package fr.univ.artois.reda.userservice.dto.request;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class CreateRideRequestDto {

    private int seats;
    private double preferredPrice;
    private double startLat;
    private  double startLng;
    private double endLat;
    private  double endLng;
}
