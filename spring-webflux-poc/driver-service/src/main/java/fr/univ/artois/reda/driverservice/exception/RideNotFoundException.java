package fr.univ.artois.reda.driverservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.server.ResponseStatusException;

public class RideNotFoundException extends ResponseStatusException {
    public RideNotFoundException(String reason) {
        super(HttpStatus.NOT_FOUND, reason);
    }
}
