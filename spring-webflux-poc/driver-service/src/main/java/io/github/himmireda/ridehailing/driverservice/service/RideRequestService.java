package io.github.himmireda.ridehailing.driverservice.service;

import io.github.himmireda.ridehailing.driverservice.dto.internal.DriverInternalDto;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import reactor.core.publisher.Flux;

public interface RideRequestService {


    Flux<RideRequestEntity> getNearbyRideRequests(DriverInternalDto driverInternalDto, Double lat, Double lng, Integer radius);
}
