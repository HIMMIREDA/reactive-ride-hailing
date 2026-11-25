package io.github.himmireda.ridehailing.authservice.repository;

import io.github.himmireda.ridehailing.authservice.entity.UserEntity;
import io.github.himmireda.ridehailing.authservice.enums.Role;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface UserRepository extends ReactiveCrudRepository<UserEntity, Long> {
    Mono<UserEntity> findByEmailAndRole(String email, Role role);
    Mono<Boolean> existsByEmailAndRole(String email, Role role);
}
