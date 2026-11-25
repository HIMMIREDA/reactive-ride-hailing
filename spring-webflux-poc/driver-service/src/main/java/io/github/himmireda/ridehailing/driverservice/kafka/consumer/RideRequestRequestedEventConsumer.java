package io.github.himmireda.ridehailing.driverservice.kafka.consumer;

import io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.driverservice.kafka.ProcessedEvents;
import io.github.himmireda.ridehailing.driverservice.kafka.ResilientRecordProcessor;
import io.github.himmireda.ridehailing.messaging.EventIds;
import org.springframework.transaction.reactive.TransactionalOperator;
import io.github.himmireda.ridehailing.driverservice.repository.RideRequestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.ZoneId;

/** Stores new ride requests so drivers can find them (geo query + change stream). */
@Slf4j
@Component
@RequiredArgsConstructor
public class RideRequestRequestedEventConsumer {

    private static final String CONSUMER = "driver-service.ride-request-requested";

    private final RideRequestRepository rideRequestRepository;
    private final ResilientRecordProcessor resilientRecordProcessor;
    private final ProcessedEvents processedEvents;
    private final TransactionalOperator transactionalOperator;

    /** Async listener: the offset is committed once the document is saved (or the record dead-lettered). */
    @KafkaListener(topics = "${kafka.topics.ride-request-requested}")
    public Mono<Void> onRideRequestRequested(ConsumerRecord<String, RideRequestRequestedEvent> record) {
        String eventId = EventIds.of(record);
        return resilientRecordProcessor.process(record, () -> processedEvents.markProcessed(eventId, CONSUMER)
                .flatMap(firstDelivery -> firstDelivery
                        ? rideRequestRepository.save(mapToRideEntity(record.value())).then()
                        : Mono.<Void>fromRunnable(() -> log.info("Skipping duplicate requested event {}", eventId)))
                .as(transactionalOperator::transactional));
    }

    private RideRequestEntity mapToRideEntity(RideRequestRequestedEvent event) {
        return RideRequestEntity.builder()
                .rideRequestId(event.getRideRequestId().toString())
                .createdAt(LocalDateTime.parse(event.getCreatedAt()).atZone(ZoneId.systemDefault()).toInstant())
                .customerEmail(event.getCustomerEmail().toString())
                .customerFirstName(event.getCustomerFirstName().toString())
                .customerLastName(event.getCustomerLastName().toString())
                .customerPhoneNumber(event.getCustomerPhoneNumber().toString())
                .seats(event.getSeats())
                .preferredPrice(event.getPreferredPrice())
                .status(RideRequestStatus.REQUESTED)
                .customerId(event.getCustomerId().toString())
                .startLocation(new GeoJsonPoint(event.getStartLng(), event.getStartLat()))
                .endLocation(new GeoJsonPoint(event.getEndLng(), event.getEndLat()))
                .build();
    }
}
