package fr.univ.artois.reda.rideservice.exception;

import org.springframework.web.server.ResponseStatusException;

public class RideNotFoundException extends ResponseStatusException {
    public RideNotFoundException(String reason) {
        super(org.springframework.http.HttpStatus.NOT_FOUND, reason);
    }
}
