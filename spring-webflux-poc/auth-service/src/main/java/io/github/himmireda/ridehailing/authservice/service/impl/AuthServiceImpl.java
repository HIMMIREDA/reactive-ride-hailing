package io.github.himmireda.ridehailing.authservice.service.impl;

import io.github.himmireda.ridehailing.authservice.dto.request.AuthRequestDto;
import io.github.himmireda.ridehailing.authservice.dto.request.UserRegisterRequestDto;
import io.github.himmireda.ridehailing.authservice.dto.response.AuthResponseDto;
import io.github.himmireda.ridehailing.authservice.dto.response.UserRegisterResponseDto;
import io.github.himmireda.ridehailing.authservice.entity.UserEntity;
import io.github.himmireda.ridehailing.authservice.exception.UserExistsException;
import io.github.himmireda.ridehailing.authservice.exception.UserNotFoundException;
import io.github.himmireda.ridehailing.authservice.mapper.UserMapper;
import io.github.himmireda.ridehailing.authservice.repository.UserRepository;
import io.github.himmireda.ridehailing.authservice.service.AuthService;
import io.github.himmireda.ridehailing.authservice.util.JwtUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private final UserRepository userRepository;
    private final UserMapper userMapper;
    private final BCryptPasswordEncoder bCryptPasswordEncoder;
    private final JwtUtil jwtUtil;

    @Transactional
    @Override
    public Mono<UserRegisterResponseDto> createUser(UserRegisterRequestDto userRegisterRequestDto) {
        return userRepository.existsByEmailAndRole(userRegisterRequestDto.getEmail(), userRegisterRequestDto.getRole())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new UserExistsException("The user with the email " + userRegisterRequestDto.getEmail() + " already exists"));
                    }
                    return Mono.fromCallable(() -> bCryptPasswordEncoder.encode(userRegisterRequestDto.getPassword()))
                            .subscribeOn(Schedulers.boundedElastic())
                            .map(encodedPassword -> {
                                userRegisterRequestDto.setPassword(encodedPassword);
                                return userMapper.mapToUserEntity(userRegisterRequestDto);
                            })
                            .flatMap(userRepository::save)
                            .map(userMapper::mapToUserRegisterResponseDto);
                })
                .log();
    }


    @Override
    public Mono<AuthResponseDto> auth(AuthRequestDto authRequestDto) {
        return userRepository.findByEmailAndRole(authRequestDto.getEmail(), authRequestDto.getRole())
                .switchIfEmpty(Mono.error(new UserNotFoundException(String.format("User with email: %s not found", authRequestDto.getEmail()))))
                .<UserEntity>handle((userEntity, sink) -> {
                    if (bCryptPasswordEncoder.matches(authRequestDto.getPassword(), userEntity.getPassword())) {
                        sink.next(userEntity);
                        return;
                    }
                    sink.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid credentials"));
                })
                .flatMap(userEntity -> Mono.zip(Mono.just(userEntity), jwtUtil.generateToken(userEntity)))
                .map((tuple2) -> {
                    final AuthResponseDto authResponseDto = userMapper.mapToAuthResponseDto(tuple2.getT1());
                    final String token = tuple2.getT2();
                    authResponseDto.setToken(token);
                    return authResponseDto;
                })
                .log();
    }
}
