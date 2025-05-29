package fr.univ.artois.reda.driverservice.dto.internal;

import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class DriverInternalDto {
    private UUID driverId;
    private String driverFirstName;
    private String driverLastName;
    private String driverEmail;
    private String driverPhoneNumber;
    private String driverRole;

}
