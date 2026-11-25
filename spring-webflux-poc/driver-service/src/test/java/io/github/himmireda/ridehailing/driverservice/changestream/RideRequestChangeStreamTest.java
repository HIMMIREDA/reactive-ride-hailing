package io.github.himmireda.ridehailing.driverservice.changestream;

import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The fan-out and resume logic of the shared change stream, without MongoDB: openChangeStream() is
 * replaced by a source the test controls.
 */
class RideRequestChangeStreamTest {

    private static final ChangeStreamProperties PROPERTIES =
            new ChangeStreamProperties(16, Duration.ofMillis(10), Duration.ofMillis(50));

    private RideRequestChangeStream changeStream;

    @AfterEach
    void stop() {
        if (changeStream != null) {
            changeStream.stop();
        }
    }

    @Test
    void slowSubscriberDoesNotBlockTheOthers() {
        Sinks.Many<RideRequestChangeStream.Change> source = Sinks.many().unicast().onBackpressureBuffer();
        changeStream = withSource(resumeAfter -> source.asFlux());
        changeStream.start();

        List<String> fast = new CopyOnWriteArrayList<>();
        Disposable fastSubscription = changeStream.rideRequests().subscribe(ride -> fast.add(ride.getRideRequestId()));
        SlowSubscriber slow = new SlowSubscriber();
        changeStream.rideRequests().subscribe(slow);   // requests one element, then nothing

        for (int i = 0; i < 1_000; i++) {
            source.tryEmitNext(change("ride-" + i));
        }

        // The fast subscriber got everything, although the slow one stopped reading after one element.
        assertThat(fast).hasSize(1_000);
        assertThat(slow.received).containsExactly("ride-0");
        // The slow one kept only the newest elements in its bounded buffer (16), the oldest were dropped.
        slow.requestAll();
        assertThat(slow.received).hasSize(1 + 16).endsWith("ride-999");
        assertThat(changeStream.droppedForSlowSubscribers()).isEqualTo(1_000 - 1 - 16);
        fastSubscription.dispose();
    }

    @Test
    void reopensAfterAnErrorResumingAfterTheLastEvent() {
        List<BsonValue> openedWith = new CopyOnWriteArrayList<>();
        changeStream = withSource(resumeAfter -> {
            openedWith.add(resumeAfter);
            if (openedWith.size() == 1) {
                // First watch: two events, then the cursor fails.
                return Flux.just(change("ride-1"), change("ride-2"))
                        .concatWith(Flux.error(new IllegalStateException("simulated cursor failure")));
            }
            return Flux.just(change("ride-3")).concatWith(Flux.never());
        });
        List<String> received = new CopyOnWriteArrayList<>();
        changeStream.rideRequests().subscribe(ride -> received.add(ride.getRideRequestId()));

        changeStream.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(received).containsExactly("ride-1", "ride-2", "ride-3"));
        // Reopened once, from the token of the last event delivered before the failure.
        assertThat(openedWith).containsExactly(null, token("ride-2"));
        assertThat(changeStream.watchesOpened()).isEqualTo(2);
    }

    private RideRequestChangeStream withSource(Function<BsonValue, Flux<RideRequestChangeStream.Change>> source) {
        return new RideRequestChangeStream(null, PROPERTIES) {
            @Override
            protected Flux<Change> openChangeStream(BsonValue resumeAfter) {
                return source.apply(resumeAfter);
            }
        };
    }

    private static RideRequestChangeStream.Change change(String rideRequestId) {
        return new RideRequestChangeStream.Change(
                RideRequestEntity.builder().rideRequestId(rideRequestId).build(), token(rideRequestId));
    }

    private static BsonValue token(String rideRequestId) {
        return new BsonString("token-" + rideRequestId);
    }

    /** Requests one element on subscribe and nothing more until requestAll(). */
    private static final class SlowSubscriber extends BaseSubscriber<RideRequestEntity> {
        final List<String> received = new CopyOnWriteArrayList<>();

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(1);
        }

        @Override
        protected void hookOnNext(RideRequestEntity value) {
            received.add(value.getRideRequestId());
        }

        void requestAll() {
            request(Long.MAX_VALUE);
        }
    }
}
