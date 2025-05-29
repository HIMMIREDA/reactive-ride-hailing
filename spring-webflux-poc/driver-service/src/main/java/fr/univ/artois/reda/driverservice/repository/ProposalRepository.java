package fr.univ.artois.reda.driverservice.repository;

import fr.univ.artois.reda.driverservice.entity.ProposalEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProposalRepository extends ReactiveMongoRepository<ProposalEntity, String> {

}
