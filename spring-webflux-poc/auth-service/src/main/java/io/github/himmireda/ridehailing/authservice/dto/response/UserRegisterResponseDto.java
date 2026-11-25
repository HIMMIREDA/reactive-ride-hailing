package io.github.himmireda.ridehailing.authservice.dto.response;

import io.github.himmireda.ridehailing.authservice.enums.Role;

import java.util.UUID;

public record UserRegisterResponseDto(
        UUID id,
        String email,
        String firstName,
        String lastName,
        String phoneNumber,
        Role role
) {
}
