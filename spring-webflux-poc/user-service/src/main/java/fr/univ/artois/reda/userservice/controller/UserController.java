package fr.univ.artois.reda.userservice.controller;

import fr.univ.artois.reda.userservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.userservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.userservice.service.UserService;
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
public class UserController {

    private final UserService userService;

    @PostMapping
    public Mono<ResponseEntity<UserRegisterResponseDto>> registerUser(@RequestBody UserRegisterRequestDto userRegisterRequestDto){
        return userService.createUser(userRegisterRequestDto)
                .map(userRegisterResponseDto -> ResponseEntity.status(HttpStatus.CREATED).body(
                        userRegisterResponseDto
                ));
    }
}
