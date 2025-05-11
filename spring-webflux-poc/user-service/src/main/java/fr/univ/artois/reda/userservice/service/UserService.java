package fr.univ.artois.reda.userservice.service;

import fr.univ.artois.reda.userservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.userservice.dto.response.UserRegisterResponseDto;
import reactor.core.publisher.Mono;

public interface UserService {

    Mono<UserRegisterResponseDto> createUser(UserRegisterRequestDto userRegisterRequestDto);
}
