package io.github.himmireda.ridehailing.rideservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * The ride request exists but its current status does not allow the requested transition
 * (e.g. accepting a request that is already ACCEPTED or BOOKED). Rendered as 409 Conflict.
 */
public class RideRequestStatusConflictException extends ResponseStatusException {
    public RideRequestStatusConflictException(String reason) {
        super(HttpStatus.CONFLICT, reason);
    }
}
