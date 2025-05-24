package fr.univ.artois.reda.userservice.service.impl;

import fr.univ.artois.reda.userservice.dto.request.CreateRideRequestRequestDto;
import fr.univ.artois.reda.userservice.dto.response.CreateRideRequestResponseDto;
import fr.univ.artois.reda.userservice.entity.RideRequestEntity;
import fr.univ.artois.reda.userservice.enums.Status;
import fr.univ.artois.reda.userservice.mapper.RideRequestMapper;
import fr.univ.artois.reda.userservice.repository.RideRequestRepository;
import fr.univ.artois.reda.userservice.service.RideRequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RideRequestServiceImpl implements RideRequestService {

    private final RideRequestRepository rideRequestRepository;
    private final RideRequestMapper rideRequestMapper;

    @Transactional
    @Override
    public Mono<CreateRideRequestResponseDto> createRideRequest(UUID userId, CreateRideRequestRequestDto requestDto) {
        RideRequestEntity rideRequestEntity = rideRequestMapper.mapToRideRequestEntity(requestDto);
        rideRequestEntity.setUserId(userId);
        rideRequestEntity.setStatus(Status.REQUESTED);
        return rideRequestRepository.save(rideRequestEntity)
                .map(rideRequestMapper::mapToCreateRideRequestResponseDto);
    }
}
