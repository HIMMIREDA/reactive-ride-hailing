package fr.univ.artois.reda.userservice.repository;

import fr.univ.artois.reda.userservice.entity.RideEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface RideRepository extends ReactiveCrudRepository<RideEntity, UUID> {
}
