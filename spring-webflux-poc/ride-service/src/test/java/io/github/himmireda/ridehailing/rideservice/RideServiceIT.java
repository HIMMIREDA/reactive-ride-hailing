package io.github.himmireda.ridehailing.rideservice;

import io.github.himmireda.ridehailing.avro.RideProposalBookedEvent;
import io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent;
import io.github.himmireda.ridehailing.rideservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.messaging.EventHeaders;
import io.github.himmireda.ridehailing.rideservice.kafka.EventPublisher;
import io.github.himmireda.ridehailing.rideservice.outbox.OutboxProperties;
import io.github.himmireda.ridehailing.rideservice.outbox.OutboxRelay;
import io.github.himmireda.ridehailing.rideservice.outbox.OutboxWriter;
import io.github.himmireda.ridehailing.rideservice.repository.RideRequestRepository;
import org.junit.jupiter.api.Nested;
import org.springframework.transaction.reactive.TransactionalOperator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.dao.TransientDataAccessResourceException;
import reactor.core.publisher.Mono;
import java.nio.charset.StandardCharsets;
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
import org.mockito.stubbing.Answer;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

/**
 * ride-service against real PostgreSQL and Kafka containers. Avro goes through the in-memory mock
 * Schema Registry (see {@link AvroKafkaTestClient}).
 *
 * <p>Kafka uses {@code @ServiceConnection}: Spring Boot's auto-configured KafkaTemplate, listener
 * containers and KafkaAdmin all take the container's address from it.
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
        registry.add("spring.kafka.properties.schema.registry.url", () -> AvroKafkaTestClient.SCHEMA_REGISTRY_URL);
        // The mock registry starts empty, so let the serializers register schemas on first use.
        registry.add("spring.kafka.properties.auto.register.schemas", () -> true);
        // Short listener backoff so retry -> dead-letter tests run fast.
        registry.add("app.kafka.retry.initial-interval", () -> "100ms");
        registry.add("app.kafka.retry.max-interval", () -> "500ms");
        // Fast outbox relay with small batches (several relays compete in one test).
        registry.add("app.outbox.poll-interval", () -> "200ms");
        registry.add("app.outbox.batch-size", () -> "5");
        registry.add("app.outbox.initial-backoff", () -> "200ms");
        registry.add("app.outbox.max-backoff", () -> "1s");
    }

    @Autowired
    private WebTestClient webTestClient;
    // Real repository wrapped in a Mockito spy: calls go through unless a test stubs them.
    @MockitoSpyBean
    private RideRequestRepository rideRequestRepository;
    @Autowired
    private DatabaseClient databaseClient;
    @MockitoSpyBean
    private OutboxWriter outboxWriter;
    @Autowired
    private OutboxRelay outboxRelay;
    @Autowired
    private OutboxProperties outboxProperties;
    @Autowired
    private EventPublisher eventPublisher;
    @Autowired
    private TransactionalOperator transactionalOperator;

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
        // Published by the outbox relay: the event id header is the outbox row id.
        assertThat(AvroKafkaTestClient.header(record, EventHeaders.EVENT_ID)).isEqualTo(outboxIdOf(rideRequestId.toString()));
        assertThat(event.getCustomerId()).hasToString(customerId.toString());
        assertThat(event.getCustomerEmail()).hasToString("rider@example.com");
        assertThat(event.getSeats()).isEqualTo(2);
        assertThat(event.getStartLat()).isEqualTo(33.2);
    }

    @Test
    void createRideRequestWithoutOptionalProfileHeadersUsesEmptyValues() {
        // The gateway does not forward profile claims the token lacks (e.g. no phone number at sign-up).
        UUID customerId = UUID.randomUUID();
        Map<String, Object> response = webTestClient.post().uri("/api/rides")
                .header("X-User-Id", customerId.toString())
                .header("X-Role", "USER")
                .header("X-Email", "rider@example.com")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("startLng", 20.2, "startLat", 33.2, "endLng", 20.1, "endLat", 50.1,
                        "seats", 1, "preferredPrice", 9.5))
                .exchange()
                .expectStatus().isOk()
                .expectBody(new ParameterizedTypeReference<Map<String, Object>>() {})
                .returnResult().getResponseBody();
        String rideRequestId = (String) response.get("rideRequestId");

        RideRequestRequestedEvent event = AvroKafkaTestClient.<RideRequestRequestedEvent>awaitRecord(
                kafka.getBootstrapServers(), rideRequestRequestedTopic, r -> r.key().equals(rideRequestId)).value();
        assertThat(event.getCustomerPhoneNumber()).hasToString("");
        assertThat(event.getCustomerFirstName()).hasToString("");
        assertThat(event.getCustomerEmail()).hasToString("rider@example.com");
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

    @Test
    void poisonRecordGoesToDeadLetterTopicAndNextRecordIsStillProcessed() {
        UUID rideRequestId = saveRide(RideRequestStatus.ACCEPTED);
        String poisonKey = "poison-" + UUID.randomUUID();
        byte[] poison = "this is not an Avro record".getBytes(StandardCharsets.UTF_8);
        UUID rideProposalId = UUID.randomUUID();

        AvroKafkaTestClient.sendRaw(kafka.getBootstrapServers(), rideProposalBookedTopic, poisonKey, poison);
        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalBookedTopic, rideProposalId.toString(),
                bookedEvent(rideRequestId, rideProposalId));

        // The valid record right behind the poison one is processed: consumption did not stop.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(statusOf(rideRequestId)).isEqualTo("BOOKED"));

        // The poison record was not retried: it went straight to the DLT with its original bytes.
        ConsumerRecord<String, byte[]> dead = AvroKafkaTestClient.awaitRawRecord(
                kafka.getBootstrapServers(), rideProposalBookedTopic + ".DLT", r -> poisonKey.equals(r.key()));
        assertThat(dead.value()).isEqualTo(poison);
        assertThat(dltExceptionInfo(dead)).contains("DeserializationException");
    }

    @Test
    void transientFailureIsRetriedThenSucceeds() {
        UUID rideRequestId = saveRide(RideRequestStatus.ACCEPTED);
        // First lookup of this ride fails like a database hiccup, the next ones go to the real repository.
        // (The bean is a JDK proxy of an interface: doCallRealMethod() can't be used. Spring's spy delegates
        // to the real bean through its default answer, so reuse that answer for the next calls.)
        Answer<?> delegateToRealRepository = Mockito.mockingDetails(rideRequestRepository)
                .getMockCreationSettings().getDefaultAnswer();
        doReturn(Mono.error(new TransientDataAccessResourceException("simulated database hiccup")))
                .doAnswer(delegateToRealRepository)
                .when(rideRequestRepository).findById(rideRequestId);
        UUID rideProposalId = UUID.randomUUID();

        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalBookedTopic, rideProposalId.toString(),
                bookedEvent(rideRequestId, rideProposalId));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(statusOf(rideRequestId)).isEqualTo("BOOKED"));
        verify(rideRequestRepository, atLeast(2)).findById(rideRequestId);
    }

    @Test
    void bookedEventForUnknownRideIsRetriedThenDeadLettered() {
        UUID unknownRideRequestId = UUID.randomUUID();
        UUID rideProposalId = UUID.randomUUID();

        AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalBookedTopic, rideProposalId.toString(),
                bookedEvent(unknownRideRequestId, rideProposalId));

        ConsumerRecord<String, byte[]> dead = AvroKafkaTestClient.awaitRawRecord(
                kafka.getBootstrapServers(), rideProposalBookedTopic + ".DLT",
                r -> rideProposalId.toString().equals(r.key()));
        assertThat(dltExceptionInfo(dead)).contains("EventTargetNotFoundException");
    }

    private UUID saveRide(RideRequestStatus status) {
        return rideRequestRepository.save(RideRequestEntity.builder()
                        .customerId(UUID.randomUUID())
                        .seats(1).preferredPrice(10)
                        .startLat(33.2).startLng(20.2).endLat(50.1).endLng(20.1)
                        .status(status)
                        .build())
                .block(Duration.ofSeconds(5))
                .getRideRequestId();
    }

    /** Reads the status with SQL, bypassing the spied repository. */
    private String statusOf(UUID rideRequestId) {
        return databaseClient.sql("SELECT status FROM ride_requests WHERE ride_request_id = :id")
                .bind("id", rideRequestId)
                .map(row -> row.get(0, String.class))
                .one()
                .block(Duration.ofSeconds(5));
    }

    private static RideProposalBookedEvent bookedEvent(UUID rideRequestId, UUID rideProposalId) {
        return RideProposalBookedEvent.newBuilder()
                .setRideRequestId(rideRequestId.toString())
                .setRideProposalId(rideProposalId.toString())
                .setDriverId(UUID.randomUUID().toString())
                .setDriverEmail("driver@example.com")
                .setDriverFirstName("Grace")
                .setDriverLastName("Hopper")
                .setDriverPhoneNumber("0600000002")
                .setPrice(15.5)
                .setCreatedAt(LocalDateTime.now().toString())
                .build();
    }

    /** The exception headers DeadLetterPublishingRecoverer adds to a dead-lettered record. */
    private static String dltExceptionInfo(ConsumerRecord<?, ?> record) {
        return String.join(" | ",
                String.valueOf(AvroKafkaTestClient.header(record, "kafka_dlt-exception-fqcn")),
                String.valueOf(AvroKafkaTestClient.header(record, "kafka_dlt-exception-cause-fqcn")),
                String.valueOf(AvroKafkaTestClient.header(record, "kafka_dlt-exception-message")));
    }

    @Nested
    class TransactionalOutbox {

        @Test
        void rideRowAndOutboxRowAreWrittenInOneTransaction() {
            UUID customerId = UUID.randomUUID();
            // The outbox insert fails once: the ride row must be rolled back with it.
            Answer<?> realOutboxWriter = Mockito.mockingDetails(outboxWriter).getMockCreationSettings().getDefaultAnswer();
            doReturn(Mono.error(new IllegalStateException("simulated outbox failure")))
                    .doAnswer(realOutboxWriter)
                    .when(outboxWriter).append(any(), any(), any(), any());

            postRide(customerId).expectStatus().is5xxServerError();

            Long rows = databaseClient.sql("SELECT count(*) FROM ride_requests WHERE customer_id = :customerId")
                    .bind("customerId", customerId)
                    .map(row -> row.get(0, Long.class)).one().block(Duration.ofSeconds(5));
            assertThat(rows).isZero();
        }

        @Test
        void rideCreatedWhileKafkaIsDownIsPublishedOnceKafkaIsBack() {
            String rideRequestId;
            // Pause (not stop) the broker: it keeps its address, but every client request hangs.
            kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
            try {
                // The API doesn't need Kafka anymore: the event waits in the outbox.
                rideRequestId = (String) postRide(UUID.randomUUID()).expectStatus().isOk()
                        .expectBody(new ParameterizedTypeReference<Map<String, Object>>() {})
                        .returnResult().getResponseBody().get("rideRequestId");

                // The relay tries, fails (producer timeouts) and backs off; the row stays unpublished.
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    Map<String, Object> outbox = outboxRowOf(rideRequestId);
                    assertThat((Integer) outbox.get("attempts")).isGreaterThanOrEqualTo(1);
                    assertThat(outbox.get("published_at")).isNull();
                });
            } finally {
                kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
            }

            ConsumerRecord<String, RideRequestRequestedEvent> record = AvroKafkaTestClient.awaitRecord(
                    kafka.getBootstrapServers(), rideRequestRequestedTopic, r -> r.key().equals(rideRequestId));
            assertThat(AvroKafkaTestClient.header(record, EventHeaders.EVENT_ID)).isEqualTo(outboxIdOf(rideRequestId));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(outboxRowOf(rideRequestId).get("published_at")).isNotNull());
        }

        @Test
        void twoRelayInstancesPublishEachEventExactlyOnce() {
            String topic = "outbox-relay-it-" + UUID.randomUUID();
            int events = 40;
            IntStream.range(0, events).forEach(i -> outboxWriter.append(topic, "key-" + i, "aggregate-" + i,
                            sampleRequestedEvent("ride-" + i))
                    .block(Duration.ofSeconds(5)));

            // A second relay, like a second ride-service instance, polling the same table.
            OutboxRelay secondRelay = new OutboxRelay("second-relay", databaseClient, transactionalOperator,
                    eventPublisher, outboxProperties);
            long firstBefore = outboxRelay.publishedCount();
            secondRelay.start();
            try {
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(unpublishedCount(topic)).isZero());
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
        void sameBookedEventDeliveredTwiceChangesStateOnce() {
            UUID rideRequestId = saveRide(RideRequestStatus.ACCEPTED);
            UUID rideProposalId = UUID.randomUUID();
            String eventId = UUID.randomUUID().toString();
            RideProposalBookedEvent event = bookedEvent(rideRequestId, rideProposalId);

            // At-least-once delivery: the same event (same id) arrives twice, back to back.
            for (int delivery = 0; delivery < 2; delivery++) {
                AvroKafkaTestClient.send(kafka.getBootstrapServers(), rideProposalBookedTopic, rideProposalId.toString(),
                        event, Map.of(EventHeaders.EVENT_ID, eventId));
            }

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(statusOf(rideRequestId)).isEqualTo("BOOKED"));
            // Both deliveries were consumed: one applied, one recognized as a duplicate.
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(countWhere("booked_ride_proposals", "ride_proposal_id", rideProposalId)).isEqualTo(1L);
                assertThat(countWhere("processed_events", "event_id", eventId)).isEqualTo(1L);
            });
        }
    }

    private WebTestClient.ResponseSpec postRide(UUID customerId) {
        return webTestClient.post().uri("/api/rides")
                .header("X-User-Id", customerId.toString())
                .header("X-Role", "USER")
                .header("X-Email", "rider@example.com")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("startLng", 20.2, "startLat", 33.2, "endLng", 20.1, "endLat", 50.1,
                        "seats", 1, "preferredPrice", 10.0))
                .exchange();
    }

    private String outboxIdOf(String aggregateId) {
        return String.valueOf(outboxRowOf(aggregateId).get("id"));
    }

    private Map<String, Object> outboxRowOf(String aggregateId) {
        return databaseClient.sql("SELECT id, attempts, published_at FROM outbox WHERE aggregate_id = :aggregateId")
                .bind("aggregateId", aggregateId)
                .fetch().one().block(Duration.ofSeconds(5));
    }

    private long unpublishedCount(String topic) {
        return databaseClient.sql("SELECT count(*) FROM outbox WHERE topic = :topic AND published_at IS NULL")
                .bind("topic", topic)
                .map(row -> row.get(0, Long.class)).one().block(Duration.ofSeconds(5));
    }

    private long countWhere(String table, String column, Object value) {
        return databaseClient.sql("SELECT count(*) FROM " + table + " WHERE " + column + " = :value")
                .bind("value", value)
                .map(row -> row.get(0, Long.class)).one().block(Duration.ofSeconds(5));
    }

    private static RideRequestRequestedEvent sampleRequestedEvent(String rideRequestId) {
        return RideRequestRequestedEvent.newBuilder()
                .setRideRequestId(rideRequestId)
                .setCustomerId(UUID.randomUUID().toString())
                .setCustomerEmail("rider@example.com")
                .setCustomerFirstName("Ada")
                .setCustomerLastName("Lovelace")
                .setCustomerPhoneNumber("0600000001")
                .setSeats(1).setPreferredPrice(10)
                .setStartLat(33.2).setStartLng(20.2).setEndLat(50.1).setEndLng(20.1)
                .setCreatedAt(LocalDateTime.now().toString())
                .build();
    }
}
