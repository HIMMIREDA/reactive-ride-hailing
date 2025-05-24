package fr.univ.artois.reda.authservice.repository;

import fr.univ.artois.reda.authservice.entity.UserEntity;
import fr.univ.artois.reda.authservice.enums.Role;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface UserRepository extends ReactiveCrudRepository<UserEntity, Long> {
    Mono<UserEntity> findByEmailAndRole(String email, Role role);
    Mono<Boolean> existsByEmailAndRole(String email, Role role);
}
