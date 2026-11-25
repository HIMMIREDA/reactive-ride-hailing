package io.github.himmireda.ridehailing.authservice.service;

import io.github.himmireda.ridehailing.authservice.dto.request.AuthRequestDto;
import io.github.himmireda.ridehailing.authservice.dto.request.UserRegisterRequestDto;
import io.github.himmireda.ridehailing.authservice.dto.response.AuthResponseDto;
import io.github.himmireda.ridehailing.authservice.dto.response.UserRegisterResponseDto;
import reactor.core.publisher.Mono;

public interface AuthService {

    Mono<UserRegisterResponseDto> createUser(UserRegisterRequestDto userRegisterRequestDto);
    Mono<AuthResponseDto> auth(AuthRequestDto authRequestDto);

}
