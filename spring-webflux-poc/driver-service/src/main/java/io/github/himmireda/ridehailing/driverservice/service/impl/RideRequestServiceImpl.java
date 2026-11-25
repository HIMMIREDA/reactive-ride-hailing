package io.github.himmireda.ridehailing.driverservice.service.impl;

import io.github.himmireda.ridehailing.driverservice.dto.internal.DriverInternalDto;
import io.github.himmireda.ridehailing.driverservice.changestream.RideRequestChangeStream;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.driverservice.repository.RideRequestRepository;
import io.github.himmireda.ridehailing.driverservice.service.RideRequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class RideRequestServiceImpl implements RideRequestService {

    private final RideRequestRepository rideRequestRepository;
    private final RideRequestChangeStream rideRequestChangeStream;

    /**
     * Nearby requests = history (geo query) followed by the live feed. The live feed is the one shared
     * change stream of the service, filtered here by distance: no MongoDB cursor per SSE connection.
     */
    @Override
    public Flux<RideRequestEntity> getNearbyRideRequests(DriverInternalDto driverInternalDto, Double lat, Double lng, Integer radiusKm) {
        GeoJsonPoint driverPoint = new GeoJsonPoint(lng, lat);
        Distance distance = new Distance(radiusKm, Metrics.KILOMETERS);
        Flux<RideRequestEntity> nearRides = rideRequestRepository.findByStatusAndStartLocationNear(RideRequestStatus.REQUESTED, driverPoint, distance);
        Flux<RideRequestEntity> liveNearbyRides = rideRequestChangeStream.rideRequests()
                .filter(ride -> ride.getStartLocation() != null && isWithinRadius(driverPoint, ride.getStartLocation(), radiusKm));
        return Flux.concat(nearRides, liveNearbyRides);
    }
    // Haversine formula: great-circle distance between two points from their latitude and longitude
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
