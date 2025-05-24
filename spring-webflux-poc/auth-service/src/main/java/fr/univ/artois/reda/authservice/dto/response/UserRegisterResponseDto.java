package fr.univ.artois.reda.authservice.dto.response;

import fr.univ.artois.reda.authservice.enums.Role;

import java.util.UUID;

public record UserRegisterResponseDto(
        UUID id,
        String email,
        String firstName,
        String lastName,
        Role role
) {
}
