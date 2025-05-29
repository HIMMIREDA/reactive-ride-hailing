package fr.univ.artois.reda.rideservice.kafka;

import fr.univ.artois.reda.avro.RideProposalProposedEvent;
import fr.univ.artois.reda.rideservice.dto.response.RideProposalResponseDto;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RideProposalSinkManager {

    private final Map<String, Sinks.Many<RideProposalResponseDto>> sinkMap = new ConcurrentHashMap<>();

    public Sinks.Many<RideProposalResponseDto> register(String rideRequestId) {
        return sinkMap.computeIfAbsent(rideRequestId, id ->
                Sinks.many().multicast().onBackpressureBuffer()
        );
    }

    public void unregister(String rideRequestId) {
        sinkMap.remove(rideRequestId);
    }

    public void emitProposal(RideProposalProposedEvent proposalEvent) {
        String id = proposalEvent.getRideRequestId().toString();

        Sinks.Many<RideProposalResponseDto> sink = sinkMap.get(id);
        if (sink != null) {
            sink.tryEmitNext(mapToRideProposalResponseDto(proposalEvent));
        }
    }

    private RideProposalResponseDto mapToRideProposalResponseDto(RideProposalProposedEvent rideProposalProposedEvent) {
        return RideProposalResponseDto.builder()
                .rideProposalId(rideProposalProposedEvent.getRideProposalId().toString())
                .rideRequestId(rideProposalProposedEvent.getRideRequestId().toString())
                .driverId(rideProposalProposedEvent.getDriverId().toString())
                .driverPhoneNumber(rideProposalProposedEvent.getDriverPhoneNumber().toString())
                .driverLastName(rideProposalProposedEvent.getDriverLastName().toString())
                .driverFirstName(rideProposalProposedEvent.getDriverFirstName().toString())
                .driverEmail(rideProposalProposedEvent.getDriverEmail().toString())
                .price(rideProposalProposedEvent.getPrice())
                .createdAt(rideProposalProposedEvent.getCreatedAt().toString())
                .build();
    }
}
