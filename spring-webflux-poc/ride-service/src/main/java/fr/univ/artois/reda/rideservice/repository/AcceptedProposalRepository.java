package fr.univ.artois.reda.userservice.repository;

import fr.univ.artois.reda.userservice.entity.AcceptedProposalEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;

public interface AcceptedProposalRepository extends ReactiveCrudRepository<AcceptedProposalEntity, UUID>{
}
