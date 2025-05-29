package fr.univ.artois.reda.driverservice.repository;

import fr.univ.artois.reda.driverservice.entity.RideProposalEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface RideProposalRepository extends ReactiveMongoRepository<RideProposalEntity, String> {

}
