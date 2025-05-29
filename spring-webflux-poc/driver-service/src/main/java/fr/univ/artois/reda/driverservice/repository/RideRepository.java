package fr.univ.artois.reda.driverservice.repository;

import fr.univ.artois.reda.driverservice.entity.RideEntity;
import fr.univ.artois.reda.driverservice.enums.RideStatus;
import org.springframework.data.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface RideRepository extends ReactiveMongoRepository<RideEntity, String> {

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

    Flux<RideEntity> findByStatusAndStartLocationNear(RideStatus status, GeoJsonPoint startLocation, Distance distance);

    Mono<RideEntity> findByIdAndStatus(String id, RideStatus status);
}
