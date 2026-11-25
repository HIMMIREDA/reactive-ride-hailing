package io.github.himmireda.ridehailing.driverservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class RideRequestNotFoundException extends ResponseStatusException {
    public RideRequestNotFoundException(String reason) {
        super(HttpStatus.NOT_FOUND, reason);
    }
}
