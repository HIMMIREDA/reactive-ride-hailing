package fr.univ.artois.reda.rideservice.repository;

import fr.univ.artois.reda.rideservice.entity.RideEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface RideRepository extends ReactiveCrudRepository<RideEntity, UUID> {
    Mono<RideEntity> findByIdAndUserId(UUID id, UUID userId);
}
