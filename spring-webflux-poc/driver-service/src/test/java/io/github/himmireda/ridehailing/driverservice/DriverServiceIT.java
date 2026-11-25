package io.github.himmireda.ridehailing.driverservice;

import io.github.himmireda.ridehailing.avro.RideProposalAcceptedEvent;
import io.github.himmireda.ridehailing.avro.RideProposalBookedEvent;
import io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent;
import io.github.himmireda.ridehailing.driverservice.entity.RideProposalEntity;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.driverservice.repository.RideProposalRepository;
import io.github.himmireda.ridehailing.driverservice.repository.RideRequestRepository;
import io.github.himmireda.ridehailing.driverservice.support.AvroKafkaTestClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.github.himmireda.ridehailing.driverservice.kafka.EventPublisher;
import io.github.himmireda.ridehailing.driverservice.outbox.OutboxDocument;
import io.github.himmireda.ridehailing.driverservice.outbox.OutboxProperties;
import io.github.himmireda.ridehailing.driverservice.outbox.OutboxRelay;
import io.github.himmireda.ridehailing.driverservice.outbox.OutboxWriter;
import io.github.himmireda.ridehailing.messaging.EventHeaders;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * driver-service against real MongoDB and Kafka containers.
 *
 * <p>{@link MongoDBContainer} starts MongoDB as a single-node replica set. That matters here:
 * change streams (used by the nearby SSE feed) only exist on replica sets.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class DriverServiceIT {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7.0");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.properties.schema.registry.url", () -> AvroKafkaTestClient.SCHEMA_REGISTRY_URL);
        registry.add("spring.kafka.properties.auto.register.schemas", () -> true);
        registry.add("app.kafka.retry.initial-interval", () -> "100ms");
        registry.add("app.kafka.retry.max-interval", () -> "500ms");
        registry.add("app.outbox.poll-interval", () -> "200ms");
        registry.add("app.outbox.batch-size", () -> "5");
        registry.add("app.outbox.initial-backoff", () -> "200ms");
        registry.add("app.outbox.max-backoff", () -> "1s");
    }

    private static final ParameterizedTypeReference<Map<String, Object>> JSON_OBJECT = new ParameterizedTypeReference<>() {};

    @Autowired
    private WebTestClient webTestClient;
    @Autowired
    private RideRequestRepository rideRequestRepository;
    @Autowired
    private RideProposalRepository rideProposalRepository;
    @Autowired
    private ReactiveMongoTemplate mongoTemplate;
    @Autowired
    private OutboxWriter outboxWriter;
    @Autowired
    private OutboxRelay outboxRelay;
    @Autowired
    private OutboxProperties outboxProperties;
    @Autowired
    private EventPublisher eventPublisher;

    @Value("${kafka.topics.ride-request-requested}")
    private String rideRequestRequestedTopic;
    @Value("${kafka.topics.ride-proposal-accepted}")
    private String rideProposalAcceptedTopic;
    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopic;

    @Test
    void consumingRequestedEventStoresRideRequestDocument() {
        String rideRequestId = UUID.randomUUID().toString();
        String customerId = UUID.randomUUID().toString();

        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideRequestRequestedTopic, rideRequestId,
                RideRequestRequestedEvent.newBuilder()
                        .setRideRequestId(rideRequestId)
                        .setCustomerId(customerId)
                        .setCustomerEmail("rider@example.com")
                        .setCustomerFirstName("Ada")
                        .setCustomerLastName("Lovelace")
                        .setCustomerPhoneNumber("0600000001")
                        .setSeats(2)
                        .setPreferredPrice(12.22)
                        .setStartLat(33.2).setStartLng(20.2)
                        .setEndLat(50.1).setEndLng(20.1)
                        .setCreatedAt(LocalDateTime.now().toString())
                        .build());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            RideRequestEntity stored = rideRequestRepository.findById(rideRequestId).block(Duration.ofSeconds(5));
            assertThat(stored).isNotNull();
            assertThat(stored.getStatus()).isEqualTo(RideRequestStatus.REQUESTED);
            assertThat(stored.getCustomerId()).isEqualTo(customerId);
            // GeoJSON stores (longitude, latitude), in that order.
            assertThat(stored.getStartLocation().getX()).isEqualTo(20.2);
            assertThat(stored.getStartLocation().getY()).isEqualTo(33.2);
        });
    }

    @Test
    void nearbyStreamEmitsRequestCreatedAfterSubscription() {
        // Coordinates far from the other tests' requests, and every assertion filters on our own ids.
        double lat = 10.0;
        double lng = 10.0;
        String anchorId = UUID.randomUUID().toString();
        String liveId = UUID.randomUUID().toString();

        // The endpoint is Flux.concat(history query, change stream). An existing nearby request makes the
        // server emit right away, so the HTTP response starts and the client can subscribe.
        rideRequestRepository.save(requestedRideAt(anchorId, lat, lng)).block(Duration.ofSeconds(5));

        Flux<String> streamedIds = webTestClient
                .mutate().responseTimeout(Duration.ofSeconds(30)).build()
                .get().uri(uri -> uri.path("/api/rides/nearby/stream")
                        .queryParam("lat", lat).queryParam("lng", lng).queryParam("radius", 5).build())
                .headers(h -> {
                    h.set("X-User-Id", UUID.randomUUID().toString());
                    h.set("X-Role", "DRIVER");
                    h.set("X-Email", "driver@example.com");
                    h.set("X-First-Name", "Grace");
                    h.set("X-Last-Name", "Hopper");
                    h.set("X-Phone-Number", "0600000002");
                })
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                // returnResult (not expectBody) gives the body as a live Flux: SSE never completes.
                .returnResult(JSON_OBJECT)
                .getResponseBody()
                .map(event -> (String) event.get("rideRequestId"))
                .filter(Set.of(anchorId, liveId)::contains);

        // StepVerifier on an infinite stream: check the elements we expect, then cancel.
        StepVerifier.create(streamedIds)
                .expectNext(anchorId)                  // from the history query
                // After the history part completes, concat opens the change stream. Opening it is an
                // asynchronous round trip to MongoDB, so give it a moment before inserting.
                .thenAwait(Duration.ofSeconds(1))
                .then(() -> rideRequestRepository.save(requestedRideAt(liveId, lat, lng)).block(Duration.ofSeconds(5)))
                .expectNext(liveId)                    // from the change stream
                .thenCancel()
                .verify(Duration.ofSeconds(30));
    }

    @Test
    void consumingAcceptedEventMarksRequestBookedAndPublishesBookedEvent() {
        String rideRequestId = UUID.randomUUID().toString();
        String rideProposalId = UUID.randomUUID().toString();
        String driverId = UUID.randomUUID().toString();
        rideRequestRepository.save(requestedRideAt(rideRequestId, -20.0, -40.0)).block(Duration.ofSeconds(5));
        rideProposalRepository.save(RideProposalEntity.builder()
                        .rideProposalId(rideProposalId)
                        .rideRequestId(rideRequestId)
                        .price(15.5)
                        .driverId(driverId)
                        .driverEmail("driver@example.com")
                        .driverFirstName("Grace")
                        .driverLastName("Hopper")
                        .driverPhoneNumber("0600000002")
                        .build())
                .block(Duration.ofSeconds(5));

        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalAcceptedTopic, rideRequestId,
                RideProposalAcceptedEvent.newBuilder()
                        .setRideRequestId(rideRequestId)
                        .setRideProposalId(rideProposalId)
                        .build());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            RideRequestEntity stored = rideRequestRepository.findById(rideRequestId).block(Duration.ofSeconds(5));
            assertThat(stored.getStatus()).isEqualTo(RideRequestStatus.BOOKED);
            assertThat(stored.getAcceptedRideProposalId()).isEqualTo(rideProposalId);
        });

        ConsumerRecord<String, RideProposalBookedEvent> booked = AvroKafkaTestClient.awaitRecord(
                kafka.getBootstrapServers(), rideProposalBookedTopic,
                r -> r.value().getRideRequestId().toString().equals(rideRequestId));
        assertThat(booked.key()).isEqualTo(rideProposalId);
        // Published by the outbox relay: the event id header is the outbox document id.
        OutboxDocument outbox = mongoTemplate.findOne(Query.query(Criteria.where("aggregateId").is(rideRequestId)
                .and("eventType").is(RideProposalBookedEvent.getClassSchema().getFullName())), OutboxDocument.class).block();
        assertThat(AvroKafkaTestClient.header(booked, EventHeaders.EVENT_ID)).isEqualTo(outbox.getId());
        assertThat(booked.value().getRideProposalId()).hasToString(rideProposalId);
        assertThat(booked.value().getDriverId()).hasToString(driverId);
        assertThat(booked.value().getPrice()).isEqualTo(15.5);
    }

    @Test
    void acceptedEventForUnknownOfferIsRetriedThenDeadLettered() {
        String rideRequestId = UUID.randomUUID().toString();
        rideRequestRepository.save(requestedRideAt(rideRequestId, 5.0, 5.0)).block(Duration.ofSeconds(5));
        String unknownProposalId = UUID.randomUUID().toString();

        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalAcceptedTopic, rideRequestId,
                RideProposalAcceptedEvent.newBuilder()
                        .setRideRequestId(rideRequestId)
                        .setRideProposalId(unknownProposalId)
                        .build());

        ConsumerRecord<String, byte[]> dead = AvroKafkaTestClient.awaitRawRecord(
                kafka.getBootstrapServers(), rideProposalAcceptedTopic + ".DLT", r -> rideRequestId.equals(r.key()));
        assertThat(AvroKafkaTestClient.header(dead, "kafka_dlt-exception-fqcn")).contains("EventTargetNotFoundException");
        assertThat(AvroKafkaTestClient.header(dead, "kafka_dlt-exception-message")).contains(unknownProposalId);
        // Nothing was booked.
        assertThat(rideRequestRepository.findById(rideRequestId).block(Duration.ofSeconds(5)).getStatus())
                .isEqualTo(RideRequestStatus.REQUESTED);
    }

    @Nested
    class TransactionalOutbox {

        @Test
        void twoRelayInstancesPublishEachEventExactlyOnce() {
            String topic = "outbox-relay-it-" + UUID.randomUUID();
            int events = 40;
            IntStream.range(0, events).forEach(i -> outboxWriter.append(topic, "key-" + i, "aggregate-" + i,
                    RideProposalAcceptedEvent.newBuilder()
                            .setRideRequestId("ride-" + i)
                            .setRideProposalId("proposal-" + i)
                            .build()).block(Duration.ofSeconds(5)));

            // A second relay, like a second driver-service instance, claiming from the same collection.
            OutboxRelay secondRelay = new OutboxRelay("second-relay", mongoTemplate, eventPublisher, outboxProperties);
            long firstBefore = outboxRelay.publishedCount();
            secondRelay.start();
            try {
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(mongoTemplate.count(
                        Query.query(Criteria.where("topic").is(topic).and("publishedAt").is(null)), OutboxDocument.class)
                        .block()).isZero());
            } finally {
                secondRelay.stop();
            }

            List<ConsumerRecord<String, byte[]>> records = AvroKafkaTestClient.readAll(
                    kafka.getBootstrapServers(), topic, events, Duration.ofSeconds(3));
            Map<String, Long> perEventId = records.stream().collect(Collectors.groupingBy(
                    r -> AvroKafkaTestClient.header(r, EventHeaders.EVENT_ID), Collectors.counting()));
            assertThat(perEventId).hasSize(events);
            assertThat(perEventId.values()).allMatch(count -> count == 1L);
            System.out.printf("relays shared the work: first=%d second=%d%n",
                    outboxRelay.publishedCount() - firstBefore, secondRelay.publishedCount());
        }
    }

    @Nested
    class IdempotentConsumers {

        @Test
        void sameAcceptedEventDeliveredTwiceBooksOnce() {
            String rideRequestId = UUID.randomUUID().toString();
            String rideProposalId = UUID.randomUUID().toString();
            rideRequestRepository.save(requestedRideAt(rideRequestId, -10.0, -30.0)).block(Duration.ofSeconds(5));
            rideProposalRepository.save(RideProposalEntity.builder()
                    .rideProposalId(rideProposalId).rideRequestId(rideRequestId).price(20)
                    .driverId(UUID.randomUUID().toString()).driverEmail("driver@example.com")
                    .driverFirstName("Grace").driverLastName("Hopper").driverPhoneNumber("0600000002")
                    .build()).block(Duration.ofSeconds(5));
            String eventId = UUID.randomUUID().toString();
            RideProposalAcceptedEvent accepted = RideProposalAcceptedEvent.newBuilder()
                    .setRideRequestId(rideRequestId).setRideProposalId(rideProposalId).build();

            // At-least-once delivery: the same event (same id) arrives twice, back to back.
            for (int delivery = 0; delivery < 2; delivery++) {
                AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalAcceptedTopic, rideRequestId,
                        accepted, Map.of(EventHeaders.EVENT_ID, eventId));
            }

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(rideRequestRepository.findById(rideRequestId).block().getStatus())
                            .isEqualTo(RideRequestStatus.BOOKED));
            // Exactly one booked event was written to the outbox, and one processed-event marker exists.
            Query bookedForRide = Query.query(Criteria.where("aggregateId").is(rideRequestId)
                    .and("eventType").is(RideProposalBookedEvent.getClassSchema().getFullName()));
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(mongoTemplate.count(bookedForRide, OutboxDocument.class).block()).isEqualTo(1L);
                assertThat(mongoTemplate.getCollection("processed_events")
                        .flatMap(c -> Mono.from(c.countDocuments(new org.bson.Document("eventId", eventId)))).block())
                        .isEqualTo(1L);
            });
        }
    }

    private static RideRequestEntity requestedRideAt(String id, double lat, double lng) {
        return RideRequestEntity.builder()
                .rideRequestId(id)
                .customerId(UUID.randomUUID().toString())
                .seats(1)
                .preferredPrice(10)
                .startLocation(new GeoJsonPoint(lng, lat))
                .endLocation(new GeoJsonPoint(lng + 0.1, lat + 0.1))
                .status(RideRequestStatus.REQUESTED)
                .createdAt(Instant.now())
                .customerEmail("rider@example.com")
                .customerFirstName("Ada")
                .customerLastName("Lovelace")
                .customerPhoneNumber("0600000001")
                .build();
    }
}
