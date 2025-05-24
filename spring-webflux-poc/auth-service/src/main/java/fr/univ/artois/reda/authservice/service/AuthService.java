package fr.univ.artois.reda.authservice.service;

import fr.univ.artois.reda.authservice.dto.request.AuthRequestDto;
import fr.univ.artois.reda.authservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.authservice.dto.response.AuthResponseDto;
import fr.univ.artois.reda.authservice.dto.response.UserRegisterResponseDto;
import reactor.core.publisher.Mono;

public interface AuthService {

    Mono<UserRegisterResponseDto> createUser(UserRegisterRequestDto userRegisterRequestDto);
    Mono<AuthResponseDto> auth(AuthRequestDto authRequestDto);

}
