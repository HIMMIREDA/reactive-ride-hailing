package fr.univ.artois.reda.userservice.repository;

import fr.univ.artois.reda.userservice.entity.RideRequestEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface RideRequestRepository extends ReactiveCrudRepository<RideRequestEntity, UUID> {
}
