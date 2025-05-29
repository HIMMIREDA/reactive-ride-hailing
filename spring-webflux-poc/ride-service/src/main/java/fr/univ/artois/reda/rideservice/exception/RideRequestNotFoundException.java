package fr.univ.artois.reda.rideservice.exception;

import org.springframework.web.server.ResponseStatusException;

public class RideRequestNotFoundException extends ResponseStatusException {
    public RideRequestNotFoundException(String reason) {
        super(org.springframework.http.HttpStatus.NOT_FOUND, reason);
    }
}
