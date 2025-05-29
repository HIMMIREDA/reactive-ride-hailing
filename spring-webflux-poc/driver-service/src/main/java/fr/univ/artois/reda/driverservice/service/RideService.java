package fr.univ.artois.reda.driverservice.service;

import fr.univ.artois.reda.driverservice.dto.internal.UserInternalDto;
import fr.univ.artois.reda.driverservice.entity.RideEntity;
import reactor.core.publisher.Flux;

public interface RideService {


    Flux<RideEntity> getNearbyRides(UserInternalDto userInternalDto, Double lat, Double lng, Integer radius);
}
