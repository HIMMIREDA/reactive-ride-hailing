package fr.univ.artois.reda.userservice.repository;

import fr.univ.artois.reda.userservice.entity.UserEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface UserRepository extends ReactiveCrudRepository<UserEntity, Long> {
    Mono<UserEntity> findByEmail(String email);

    Mono<Boolean> existsByEmail(String email);
}
