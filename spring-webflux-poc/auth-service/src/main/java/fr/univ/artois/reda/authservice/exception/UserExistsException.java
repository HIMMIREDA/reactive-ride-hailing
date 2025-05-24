package fr.univ.artois.reda.authservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class UserExistsException extends ResponseStatusException {
    public UserExistsException(String reason) {
        super(HttpStatus.BAD_REQUEST, reason);
    }
}
