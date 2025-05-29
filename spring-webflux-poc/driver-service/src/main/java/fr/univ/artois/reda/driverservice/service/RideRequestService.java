package fr.univ.artois.reda.driverservice.service;

import fr.univ.artois.reda.driverservice.dto.internal.DriverInternalDto;
import fr.univ.artois.reda.driverservice.entity.RideRequestEntity;
import reactor.core.publisher.Flux;

public interface RideRequestService {


    Flux<RideRequestEntity> getNearbyRideRequests(DriverInternalDto driverInternalDto, Double lat, Double lng, Integer radius);
}
