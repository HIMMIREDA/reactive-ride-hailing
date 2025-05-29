package fr.univ.artois.reda.driverservice.service.impl;

import fr.univ.artois.reda.driverservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.driverservice.entity.RideEntity;
import fr.univ.artois.reda.driverservice.enums.RideStatus;
import fr.univ.artois.reda.driverservice.repository.RideRepository;
import fr.univ.artois.reda.driverservice.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class RideServiceImpl implements RideService {
    private final RideRepository rideRepository;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    @Override
    public Flux<RideEntity> getNearbyRides(UserInternalDto userInternalDto, Double lat, Double lng, Integer radiusKm) {
        GeoJsonPoint driverPoint = new GeoJsonPoint(lng, lat);
        Distance distance = new Distance(radiusKm, Metrics.KILOMETERS);
        Flux<RideEntity> nearRides = rideRepository.findByStatusAndStartLocationNear(RideStatus.REQUESTED, driverPoint, distance);
        Flux<RideEntity> liveNearbyRides = reactiveMongoTemplate
                .changeStream(RideEntity.class)
                .watchCollection("rides")
                .filter(Criteria.where("status").is(RideStatus.REQUESTED.toString()))
                .listen()
                .mapNotNull(ChangeStreamEvent::getBody)
                .filter(ride -> ride != null && isWithinRadius(driverPoint, ride.getStartLocation(), radiusKm))
                .log();
        return Flux.concat(nearRides, liveNearbyRides);
    }
    // formule de haversine pour calculer la distance du grand cercle entre 2 pts en utilisant lat et lng
    private boolean isWithinRadius(GeoJsonPoint center, GeoJsonPoint target, int radiusKm) {
        double lat1 = center.getY();
        double lon1 = center.getX();
        double lat2 = target.getY();
        double lon2 = target.getX();

        double R = 6371;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c;

        return distance <= radiusKm;
    }

}
