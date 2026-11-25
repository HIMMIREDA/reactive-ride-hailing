package io.github.himmireda.ridehailing.rideservice.service.impl;

import io.github.himmireda.ridehailing.avro.RideProposalAcceptedEvent;
import io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent;
import io.github.himmireda.ridehailing.rideservice.dto.internal.CustomerInternalDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.AcceptRideProposalRequestDto;
import io.github.himmireda.ridehailing.rideservice.dto.request.CreateRideRequestRequestDto;
import io.github.himmireda.ridehailing.rideservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.rideservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.rideservice.exception.RideRequestNotFoundException;
import io.github.himmireda.ridehailing.rideservice.exception.RideRequestStatusConflictException;
import io.github.himmireda.ridehailing.rideservice.kafka.RideProposalSinkManager;
import io.github.himmireda.ridehailing.rideservice.mapper.RideRequestMapperImpl;
import io.github.himmireda.ridehailing.rideservice.repository.RideRequestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests: no Spring context, no database, no Kafka. Collaborators are Mockito mocks,
 * except the MapStruct mapper, which is the real generated implementation (it is pure code).
 *
 * <p>Reactive code is lazy: calling {@code createRideRequest(...)} only builds a pipeline.
 * {@link StepVerifier} subscribes to it and checks each signal in order
 * (onNext / onError / onComplete), so the test fails if the pipeline never emits.
 */
@ExtendWith(MockitoExtension.class)
class RideRequestServiceImplTest {

    private static final String REQUESTED_TOPIC = "test-ride-request-requested";
    private static final String ACCEPTED_TOPIC = "test-ride-proposal-accepted";

    @Mock
    private RideRequestRepository rideRequestRepository;
    @Mock
    private RideProposalSinkManager rideProposalSinkManager;
    @Mock
    private ReactiveKafkaProducerTemplate<String, RideRequestRequestedEvent> requestedProducer;
    @Mock
    private ReactiveKafkaProducerTemplate<String, RideProposalAcceptedEvent> acceptedProducer;

    private RideRequestServiceImpl service;

    private final CustomerInternalDto customer = CustomerInternalDto.builder()
            .customerId(UUID.randomUUID())
            .customerEmail("rider@example.com")
            .customerFirstName("Ada")
            .customerLastName("Lovelace")
            .customerPhoneNumber("0600000001")
            .customerRole("USER")
            .build();

    @BeforeEach
    void setUp() {
        // Both producer templates have the same raw type, so @InjectMocks could mix them up: build by hand.
        service = new RideRequestServiceImpl(rideRequestRepository, new RideRequestMapperImpl(),
                rideProposalSinkManager, requestedProducer, acceptedProducer);
        // The topic names are @Value fields, which only Spring fills in.
        ReflectionTestUtils.setField(service, "rideRequestRequestedTopicName", REQUESTED_TOPIC);
        ReflectionTestUtils.setField(service, "rideProposalAcceptedTopicName", ACCEPTED_TOPIC);
    }

    /**
     * A PublisherProbe wraps a Mono and records whether it was subscribed to. Asserting
     * {@code wasSubscribed()} catches a classic reactive bug: building the "send" Mono but
     * never chaining it, so nothing is actually sent.
     */
    @SuppressWarnings("unchecked")
    private static PublisherProbe<SenderResult<Void>> sendProbe() {
        return PublisherProbe.of(Mono.just((SenderResult<Void>) mock(SenderResult.class)));
    }

    @Nested
    class CreateRideRequest {

        private final CreateRideRequestRequestDto request = CreateRideRequestRequestDto.builder()
                .seats(2).preferredPrice(12.5)
                .startLat(33.2).startLng(20.2).endLat(50.1).endLng(20.1)
                .build();

        @Test
        void savesRequestWithStatusRequestedAndPublishesEvent() {
            UUID generatedId = UUID.randomUUID();
            // Simulate the database: it assigns the id and the creation date.
            when(rideRequestRepository.save(any(RideRequestEntity.class))).thenAnswer(invocation -> {
                RideRequestEntity entity = invocation.getArgument(0);
                entity.setRideRequestId(generatedId);
                entity.setCreatedAt(LocalDateTime.of(2026, 1, 1, 10, 0));
                return Mono.just(entity);
            });
            PublisherProbe<SenderResult<Void>> send = sendProbe();
            when(requestedProducer.send(anyString(), anyString(), any(RideRequestRequestedEvent.class)))
                    .thenReturn(send.mono());

            StepVerifier.create(service.createRideRequest(customer, request))
                    .assertNext(response -> {
                        assertThat(response.getRideRequestId()).isEqualTo(generatedId);
                        assertThat(response.getStatus()).isEqualTo(RideRequestStatus.REQUESTED);
                        assertThat(response.getSeats()).isEqualTo(2);
                    })
                    .verifyComplete();

            ArgumentCaptor<RideRequestEntity> saved = ArgumentCaptor.forClass(RideRequestEntity.class);
            verify(rideRequestRepository).save(saved.capture());
            assertThat(saved.getValue().getStatus()).isEqualTo(RideRequestStatus.REQUESTED);
            assertThat(saved.getValue().getCustomerId()).isEqualTo(customer.getCustomerId());

            ArgumentCaptor<RideRequestRequestedEvent> event = ArgumentCaptor.forClass(RideRequestRequestedEvent.class);
            verify(requestedProducer).send(eq(REQUESTED_TOPIC), eq(generatedId.toString()), event.capture());
            assertThat(event.getValue().getRideRequestId()).hasToString(generatedId.toString());
            assertThat(event.getValue().getCustomerId()).hasToString(customer.getCustomerId().toString());
            assertThat(event.getValue().getCustomerEmail()).hasToString("rider@example.com");
            assertThat(event.getValue().getStartLat()).isEqualTo(33.2);
            assertThat(event.getValue().getPreferredPrice()).isEqualTo(12.5);
            send.assertWasSubscribed();
        }
    }

    @Nested
    class AcceptRideProposal {

        private final UUID rideRequestId = UUID.randomUUID();
        private final UUID rideProposalId = UUID.randomUUID();
        private final AcceptRideProposalRequestDto request = AcceptRideProposalRequestDto.builder()
                .rideProposalId(rideProposalId)
                .build();

        private RideRequestEntity existingRequest(RideRequestStatus status) {
            return RideRequestEntity.builder()
                    .rideRequestId(rideRequestId)
                    .customerId(customer.getCustomerId())
                    .seats(1).preferredPrice(10)
                    .status(status)
                    .createdAt(LocalDateTime.now())
                    .build();
        }

        @Test
        void happyPathMarksRequestAcceptedAndPublishesEvent() {
            when(rideRequestRepository.findByRideRequestIdAndCustomerId(rideRequestId, customer.getCustomerId()))
                    .thenReturn(Mono.just(existingRequest(RideRequestStatus.REQUESTED)));
            when(rideRequestRepository.save(any(RideRequestEntity.class)))
                    .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
            PublisherProbe<SenderResult<Void>> send = sendProbe();
            when(acceptedProducer.send(anyString(), anyString(), any(RideProposalAcceptedEvent.class)))
                    .thenReturn(send.mono());

            StepVerifier.create(service.acceptRideProposal(customer, rideRequestId, request))
                    .assertNext(response -> {
                        assertThat(response.getStatus()).isEqualTo(RideRequestStatus.ACCEPTED);
                        assertThat(response.getRideProposalId()).isEqualTo(rideProposalId);
                    })
                    .verifyComplete();

            ArgumentCaptor<RideRequestEntity> saved = ArgumentCaptor.forClass(RideRequestEntity.class);
            verify(rideRequestRepository).save(saved.capture());
            assertThat(saved.getValue().getStatus()).isEqualTo(RideRequestStatus.ACCEPTED);

            ArgumentCaptor<RideProposalAcceptedEvent> event = ArgumentCaptor.forClass(RideProposalAcceptedEvent.class);
            verify(acceptedProducer).send(eq(ACCEPTED_TOPIC), eq(rideRequestId.toString()), event.capture());
            assertThat(event.getValue().getRideRequestId()).hasToString(rideRequestId.toString());
            assertThat(event.getValue().getRideProposalId()).hasToString(rideProposalId.toString());
            send.assertWasSubscribed();
        }

        @Test
        void requestNotFoundFailsWithNotFoundAndSendsNothing() {
            when(rideRequestRepository.findByRideRequestIdAndCustomerId(rideRequestId, customer.getCustomerId()))
                    .thenReturn(Mono.empty());

            StepVerifier.create(service.acceptRideProposal(customer, rideRequestId, request))
                    .expectErrorSatisfies(error -> {
                        assertThat(error).isInstanceOf(RideRequestNotFoundException.class);
                        assertThat(((RideRequestNotFoundException) error).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    })
                    .verify();

            verify(rideRequestRepository, never()).save(any());
            verifyNoInteractions(acceptedProducer);
        }

        // The request exists but can no longer be accepted: 409 Conflict, not 404 Not Found.
        @ParameterizedTest
        @EnumSource(value = RideRequestStatus.class, names = {"ACCEPTED", "BOOKED"})
        void requestNotInRequestedStatusFailsWithConflictAndSendsNothing(RideRequestStatus status) {
            when(rideRequestRepository.findByRideRequestIdAndCustomerId(rideRequestId, customer.getCustomerId()))
                    .thenReturn(Mono.just(existingRequest(status)));

            StepVerifier.create(service.acceptRideProposal(customer, rideRequestId, request))
                    .expectErrorSatisfies(error -> {
                        assertThat(error)
                                .isInstanceOf(RideRequestStatusConflictException.class)
                                .hasMessageContaining("is not in REQUESTED status")
                                .hasMessageContaining("current status: " + status);
                        assertThat(((RideRequestStatusConflictException) error).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    })
                    .verify();

            verify(rideRequestRepository, never()).save(any());
            verifyNoInteractions(acceptedProducer);
        }
    }
}
