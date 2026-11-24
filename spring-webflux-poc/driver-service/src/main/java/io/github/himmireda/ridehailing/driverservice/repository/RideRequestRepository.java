package io.github.himmireda.ridehailing.driverservice.repository;

import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import org.springframework.data.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface RideRequestRepository extends ReactiveMongoRepository<RideRequestEntity, String> {

//    @Query("""
//        {
//          'status': ?2,
//          'startLocation': {
//            $nearSphere: {
//              $geometry: { type: 'Point', coordinates: [?0, ?1] },
//              $maxDistance: ?3
//            }
//          }
//        }
//    """)
//    Flux<RideEntity> findNearbyStartLocationRides(double lng, double lat, RideStatus status, double radiusInMeters);

    Flux<RideRequestEntity> findByStatusAndStartLocationNear(RideRequestStatus status, GeoJsonPoint startLocation, Distance distance);

    Mono<RideRequestEntity> findByRideRequestIdAndStatus(String id, RideRequestStatus status);
}
