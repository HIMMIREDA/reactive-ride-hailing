package fr.univ.artois.reda.userservice.service.impl;

import fr.univ.artois.reda.userservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.userservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.userservice.exception.UserExistsException;
import fr.univ.artois.reda.userservice.mapper.UserMapper;
import fr.univ.artois.reda.userservice.repository.UserRepository;
import fr.univ.artois.reda.userservice.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    //    private final TransactionalOperator transactionalOperator;
    private final UserRepository userRepository;
    private final UserMapper userMapper;

    @Transactional
    @Override
    public Mono<UserRegisterResponseDto> createUser(UserRegisterRequestDto userRegisterRequestDto) {
        return userRepository.existsByEmail(userRegisterRequestDto.email())
                .handle((exists, sink) -> {
                    if (exists) {
                        sink.error(new UserExistsException("The user with the email " + userRegisterRequestDto.email() + " already exists"));
                    } else {
                        sink.next(true);
                    }
                })
                .flatMap(exists -> userRepository.save(userMapper.mapToUserEntity(userRegisterRequestDto)))
                .map(userMapper::mapToUserRegisterResponseDto)
                .log();
    }
}
