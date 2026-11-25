package io.github.himmireda.ridehailing.driverservice.service.impl;

import io.github.himmireda.ridehailing.avro.RideProposalProposedEvent;
import io.github.himmireda.ridehailing.driverservice.dto.internal.DriverInternalDto;
import io.github.himmireda.ridehailing.driverservice.dto.request.ProposeRideProposalRequestDto;
import io.github.himmireda.ridehailing.driverservice.entity.RideProposalEntity;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import io.github.himmireda.ridehailing.driverservice.exception.RideRequestNotFoundException;
import io.github.himmireda.ridehailing.driverservice.mapper.RideProposalMapperImpl;
import io.github.himmireda.ridehailing.driverservice.outbox.OutboxWriter;
import io.github.himmireda.ridehailing.driverservice.repository.RideProposalRepository;
import io.github.himmireda.ridehailing.driverservice.repository.RideRequestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RideProposalServiceImplTest {

    private static final String PROPOSED_TOPIC = "test-ride-proposal-proposed";

    @Mock
    private RideProposalRepository rideProposalRepository;
    @Mock
    private RideRequestRepository rideRequestRepository;
    @Mock
    private OutboxWriter outboxWriter;

    private RideProposalServiceImpl service;

    private final String rideRequestId = UUID.randomUUID().toString();
    private final DriverInternalDto driver = DriverInternalDto.builder()
            .driverId(UUID.randomUUID())
            .driverEmail("driver@example.com")
            .driverFirstName("Grace")
            .driverLastName("Hopper")
            .driverPhoneNumber("0600000002")
            .driverRole("DRIVER")
            .build();
    private final ProposeRideProposalRequestDto request = ProposeRideProposalRequestDto.builder()
            .rideRequestId(rideRequestId)
            .price(15.5)
            .build();

    @BeforeEach
    void setUp() {
        service = new RideProposalServiceImpl(rideProposalRepository, rideRequestRepository,
                new RideProposalMapperImpl(), outboxWriter);
        ReflectionTestUtils.setField(service, "rideProposalProposedTopicName", PROPOSED_TOPIC);
    }

    @Test
    void happyPathSavesProposalWithDriverDetailsAndPublishesEvent() {
        when(rideRequestRepository.findByRideRequestIdAndStatus(rideRequestId, RideRequestStatus.REQUESTED))
                .thenReturn(Mono.just(RideRequestEntity.builder()
                        .rideRequestId(rideRequestId).status(RideRequestStatus.REQUESTED).build()));
        when(rideProposalRepository.save(any(RideProposalEntity.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        PublisherProbe<String> send = PublisherProbe.of(Mono.just(UUID.randomUUID().toString()));
        when(outboxWriter.append(anyString(), anyString(), anyString(), any(RideProposalProposedEvent.class)))
                .thenReturn(send.mono());

        StepVerifier.create(service.proposeRideProposal(driver, request))
                .assertNext(response -> {
                    assertThat(response.getRideProposalId()).isNotBlank();
                    assertThat(response.getRideRequestId()).isEqualTo(rideRequestId);
                    assertThat(response.getPrice()).isEqualTo(15.5);
                    assertThat(response.getCreatedAt()).isNotNull();
                })
                .verifyComplete();

        ArgumentCaptor<RideProposalEntity> saved = ArgumentCaptor.forClass(RideProposalEntity.class);
        verify(rideProposalRepository).save(saved.capture());
        RideProposalEntity proposal = saved.getValue();
        assertThat(proposal.getRideRequestId()).isEqualTo(rideRequestId);
        assertThat(proposal.getDriverId()).isEqualTo(driver.getDriverId().toString());
        assertThat(proposal.getDriverEmail()).isEqualTo("driver@example.com");

        ArgumentCaptor<RideProposalProposedEvent> event = ArgumentCaptor.forClass(RideProposalProposedEvent.class);
        verify(outboxWriter).append(eq(PROPOSED_TOPIC), eq(proposal.getRideProposalId()), eq(rideRequestId), event.capture());
        assertThat(event.getValue().getRideProposalId()).hasToString(proposal.getRideProposalId());
        assertThat(event.getValue().getRideRequestId()).hasToString(rideRequestId);
        assertThat(event.getValue().getDriverId()).hasToString(driver.getDriverId().toString());
        assertThat(event.getValue().getPrice()).isEqualTo(15.5);
        send.assertWasSubscribed();
    }

    @Test
    void requestNotFoundFailsAndSavesOrSendsNothing() {
        // "Not found" also covers a request that exists but is no longer REQUESTED: the query filters on status.
        when(rideRequestRepository.findByRideRequestIdAndStatus(rideRequestId, RideRequestStatus.REQUESTED))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.proposeRideProposal(driver, request))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(RideRequestNotFoundException.class)
                        .hasMessageContaining(rideRequestId))
                .verify();

        verify(rideProposalRepository, never()).save(any());
        verifyNoInteractions(outboxWriter);
    }
}
