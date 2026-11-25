package io.github.himmireda.ridehailing.rideservice.exception;

import org.springframework.web.server.ResponseStatusException;

public class RideRequestNotFoundException extends ResponseStatusException {
    public RideRequestNotFoundException(String reason) {
        super(org.springframework.http.HttpStatus.NOT_FOUND, reason);
    }
}
