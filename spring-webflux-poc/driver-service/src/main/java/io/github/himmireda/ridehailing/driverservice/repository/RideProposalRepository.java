package io.github.himmireda.ridehailing.driverservice.repository;

import io.github.himmireda.ridehailing.driverservice.entity.RideProposalEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface RideProposalRepository extends ReactiveMongoRepository<RideProposalEntity, String> {

}
