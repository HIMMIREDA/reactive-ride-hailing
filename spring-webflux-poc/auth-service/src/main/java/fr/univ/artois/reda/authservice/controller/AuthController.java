package fr.univ.artois.reda.authservice.controller;

import fr.univ.artois.reda.authservice.dto.request.AuthRequestDto;
import fr.univ.artois.reda.authservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.authservice.dto.response.AuthResponseDto;
import fr.univ.artois.reda.authservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.authservice.service.AuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping
    public Mono<ResponseEntity<UserRegisterResponseDto>> registerUser(@RequestBody UserRegisterRequestDto userRegisterRequestDto) {
        return authService.createUser(userRegisterRequestDto)
                .map(userRegisterResponseDto -> ResponseEntity.status(HttpStatus.CREATED).body(
                        userRegisterResponseDto
                ));
    }

    @PostMapping("/auth")
    public Mono<ResponseEntity<AuthResponseDto>> auth(@RequestBody AuthRequestDto authRequestDto) {
        return authService.auth(authRequestDto)
                .map(authResponseDto -> ResponseEntity.status(HttpStatus.OK).body(
                        authResponseDto
                ));
    }
}
