package fr.univ.artois.reda.userservice.dto.response;

public record UserRegisterResponseDto(
        String id,
        String email,
        String firstName,
        String lastName
) {
}
