package fr.univ.artois.reda.rideservice.repository;

import fr.univ.artois.reda.rideservice.entity.AcceptedProposalEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;

public interface AcceptedProposalRepository extends ReactiveCrudRepository<AcceptedProposalEntity, UUID>{
}
