package fr.univ.artois.reda.rideservice.repository;

import fr.univ.artois.reda.rideservice.entity.BookedRideProposalEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;

public interface BookedRideProposalRepository extends ReactiveCrudRepository<BookedRideProposalEntity, UUID>{
}
