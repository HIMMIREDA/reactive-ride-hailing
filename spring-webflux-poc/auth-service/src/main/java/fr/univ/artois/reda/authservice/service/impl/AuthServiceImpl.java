package fr.univ.artois.reda.authservice.service.impl;

import fr.univ.artois.reda.authservice.dto.request.AuthRequestDto;
import fr.univ.artois.reda.authservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.authservice.dto.response.AuthResponseDto;
import fr.univ.artois.reda.authservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.authservice.exception.UserExistsException;
import fr.univ.artois.reda.authservice.exception.UserNotFoundException;
import fr.univ.artois.reda.authservice.mapper.UserMapper;
import fr.univ.artois.reda.authservice.repository.UserRepository;
import fr.univ.artois.reda.authservice.service.AuthService;
import fr.univ.artois.reda.authservice.util.JwtUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

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
                .<Boolean>handle((exists, sink) -> {
                    if (exists) {
                        sink.error(new UserExistsException("The user with the email " + userRegisterRequestDto.getEmail() + " already exists"));
                    } else {
                        userRegisterRequestDto.setPassword(bCryptPasswordEncoder.encode(userRegisterRequestDto.getPassword()));
                        sink.next(true);
                    }
                })
                .flatMap(exists -> userRepository.save(userMapper.mapToUserEntity(userRegisterRequestDto)))
                .map(userMapper::mapToUserRegisterResponseDto)
                .log();
    }


    @Override
    public Mono<AuthResponseDto> auth(AuthRequestDto authRequestDto) {
        return userRepository.findByEmailAndRole(authRequestDto.getEmail(), authRequestDto.getRole())
                .switchIfEmpty(Mono.error(new UserNotFoundException(String.format("User with email: %s not found", authRequestDto.getEmail()))))
                .<AuthResponseDto>handle((userEntity, sink) -> {
                    if (bCryptPasswordEncoder.matches(authRequestDto.getPassword(), userEntity.getPassword())) {
                        sink.next(userMapper.mapToAuthResponseDto(userEntity));
                        return;
                    }
                    sink.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid credentials"));
                })
                .flatMap(authResponseDto -> Mono.zip(Mono.just(authResponseDto), jwtUtil.generateToken(authResponseDto.getEmail(), authResponseDto.getId().toString(), authResponseDto.getRole())))
                .map((tuple2) -> {
                    final AuthResponseDto authResponseDto = tuple2.getT1();
                    final String token = tuple2.getT2();
                    authResponseDto.setToken(token);
                    return authResponseDto;
                })
                .log();
    }
}
