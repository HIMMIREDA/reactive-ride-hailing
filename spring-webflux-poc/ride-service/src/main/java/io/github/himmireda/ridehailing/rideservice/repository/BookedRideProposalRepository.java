package io.github.himmireda.ridehailing.rideservice.repository;

import io.github.himmireda.ridehailing.rideservice.entity.BookedRideProposalEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;

public interface BookedRideProposalRepository extends ReactiveCrudRepository<BookedRideProposalEntity, UUID>{
}
