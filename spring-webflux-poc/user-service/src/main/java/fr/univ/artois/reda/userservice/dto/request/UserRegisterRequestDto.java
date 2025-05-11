package fr.univ.artois.reda.userservice.dto.request;

public record UserRegisterRequestDto(
        String email,
        String password,
        String firstName,
        String lastName
) {

}
