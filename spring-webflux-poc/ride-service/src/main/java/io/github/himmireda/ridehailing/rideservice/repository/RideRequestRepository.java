package io.github.himmireda.ridehailing.rideservice.repository;

import io.github.himmireda.ridehailing.rideservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface RideRequestRepository extends ReactiveCrudRepository<RideRequestEntity, UUID> {
    Mono<RideRequestEntity> findByRideRequestIdAndCustomerId(UUID rideRequestId, UUID customerId);
    Mono<RideRequestEntity> findByRideRequestIdAndStatus(UUID rideRequestId, RideRequestStatus status);
}
