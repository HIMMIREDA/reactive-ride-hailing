package io.github.himmireda.ridehailing.rideservice;

import io.github.himmireda.ridehailing.avro.RideProposalBookedEvent;
import io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent;
import io.github.himmireda.ridehailing.rideservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.rideservice.repository.RideRequestRepository;
import io.github.himmireda.ridehailing.rideservice.support.AvroKafkaTestClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ride-service against real PostgreSQL and Kafka containers. Avro goes through the in-memory mock
 * Schema Registry (see {@link AvroKafkaTestClient}).
 *
 * <p>Kafka gets both {@code @ServiceConnection} (used by Spring Boot's own Kafka beans, e.g. the
 * KafkaAdmin that creates the topics) and a {@code @DynamicPropertySource} entry, because this
 * service's producer/consumer configs read {@code spring.kafka.bootstrap-servers} with {@code @Value}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class RideServiceIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", postgres::getJdbcUrl);
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", () -> AvroKafkaTestClient.SCHEMA_REGISTRY_URL);
        // The mock registry starts empty, so let the serializers register schemas on first use.
        registry.add("spring.kafka.properties.auto.register.schemas", () -> true);
    }

    @Autowired
    private WebTestClient webTestClient;
    @Autowired
    private RideRequestRepository rideRequestRepository;
    @Autowired
    private DatabaseClient databaseClient;

    @Value("${kafka.topics.ride-request-requested}")
    private String rideRequestRequestedTopic;
    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopic;

    @Test
    void createRideRequestStoresRequestedRowAndPublishesEvent() {
        UUID customerId = UUID.randomUUID();

        Map<String, Object> response = webTestClient.post().uri("/api/rides")
                // The gateway normally adds these headers after validating the JWT.
                .header("X-User-Id", customerId.toString())
                .header("X-Role", "USER")
                .header("X-Email", "rider@example.com")
                .header("X-First-Name", "Ada")
                .header("X-Last-Name", "Lovelace")
                .header("X-Phone-Number", "0600000001")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("startLng", 20.2, "startLat", 33.2, "endLng", 20.1, "endLat", 50.1,
                        "seats", 2, "preferredPrice", 12.22))
                .exchange()
                .expectStatus().isOk()
                .expectBody(new ParameterizedTypeReference<Map<String, Object>>() {})
                .returnResult().getResponseBody();
        UUID rideRequestId = UUID.fromString((String) response.get("rideRequestId"));
        assertThat(response).containsEntry("status", "REQUESTED");

        RideRequestEntity row = rideRequestRepository.findById(rideRequestId).block(Duration.ofSeconds(5));
        assertThat(row).isNotNull();
        assertThat(row.getStatus()).isEqualTo(RideRequestStatus.REQUESTED);
        assertThat(row.getCustomerId()).isEqualTo(customerId);

        ConsumerRecord<String, RideRequestRequestedEvent> record = AvroKafkaTestClient.awaitRecord(
                kafka.getBootstrapServers(), rideRequestRequestedTopic,
                r -> r.key().equals(rideRequestId.toString()));
        RideRequestRequestedEvent event = record.value();
        assertThat(event.getCustomerId()).hasToString(customerId.toString());
        assertThat(event.getCustomerEmail()).hasToString("rider@example.com");
        assertThat(event.getSeats()).isEqualTo(2);
        assertThat(event.getStartLat()).isEqualTo(33.2);
    }

    @Test
    void acceptingRequestThatIsNotRequestedReturns409() {
        UUID customerId = UUID.randomUUID();
        RideRequestEntity booked = rideRequestRepository.save(RideRequestEntity.builder()
                        .customerId(customerId)
                        .seats(1).preferredPrice(10)
                        .startLat(33.2).startLng(20.2).endLat(50.1).endLng(20.1)
                        .status(RideRequestStatus.BOOKED)
                        .build())
                .block(Duration.ofSeconds(5));

        webTestClient.put().uri("/api/rides/{id}/accept", booked.getRideRequestId())
                .header("X-User-Id", customerId.toString())
                .header("X-Role", "USER")
                .header("X-Email", "rider@example.com")
                .header("X-First-Name", "Ada")
                .header("X-Last-Name", "Lovelace")
                .header("X-Phone-Number", "0600000001")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("rideProposalId", UUID.randomUUID().toString()))
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.status").isEqualTo(409);

        // Nothing changed: the request is still BOOKED.
        assertThat(rideRequestRepository.findById(booked.getRideRequestId()).block(Duration.ofSeconds(5)).getStatus())
                .isEqualTo(RideRequestStatus.BOOKED);
    }

    @Test
    void consumingBookedEventMarksAcceptedRequestBooked() {
        // Arrange: a request the rider has already accepted (inserted directly, independent of other tests).
        RideRequestEntity accepted = rideRequestRepository.save(RideRequestEntity.builder()
                        .customerId(UUID.randomUUID())
                        .seats(1).preferredPrice(10)
                        .startLat(33.2).startLng(20.2).endLat(50.1).endLng(20.1)
                        .status(RideRequestStatus.ACCEPTED)
                        .build())
                .block(Duration.ofSeconds(5));
        UUID rideRequestId = accepted.getRideRequestId();
        UUID rideProposalId = UUID.randomUUID();

        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalBookedTopic, rideProposalId.toString(),
                RideProposalBookedEvent.newBuilder()
                        .setRideRequestId(rideRequestId.toString())
                        .setRideProposalId(rideProposalId.toString())
                        .setDriverId(UUID.randomUUID().toString())
                        .setDriverEmail("driver@example.com")
                        .setDriverFirstName("Grace")
                        .setDriverLastName("Hopper")
                        .setDriverPhoneNumber("0600000002")
                        .setPrice(15.5)
                        .setCreatedAt(LocalDateTime.now().toString())
                        .build());

        // The consumer runs asynchronously. Awaitility retries the assertion until it passes,
        // or fails after the timeout: no fixed sleep, and as fast as the system allows.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            RideRequestEntity row = rideRequestRepository.findById(rideRequestId).block(Duration.ofSeconds(5));
            assertThat(row.getStatus()).isEqualTo(RideRequestStatus.BOOKED);
            assertThat(row.getAcceptedRideProposalId()).isEqualTo(rideProposalId);
        });

        Long bookedRows = databaseClient
                .sql("SELECT count(*) FROM booked_ride_proposals WHERE ride_proposal_id = :id")
                .bind("id", rideProposalId)
                .map(r -> r.get(0, Long.class))
                .one()
                .block(Duration.ofSeconds(5));
        assertThat(bookedRows).isEqualTo(1L);
    }
}
