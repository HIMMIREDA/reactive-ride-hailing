package io.github.himmireda.ridehailing.driverservice.changestream;

import com.mongodb.MongoException;
import io.github.himmireda.ridehailing.driverservice.entity.RideRequestEntity;
import io.github.himmireda.ridehailing.driverservice.enums.RideRequestStatus;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonValue;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ONE MongoDB change stream on ride_requests for the whole service, shared by every SSE subscriber.
 *
 * <ul>
 *   <li>Hot stream: the watch is opened at startup (SmartLifecycle) and events go into a
 *       {@code Sinks.many().multicast().directBestEffort()}. Chosen over {@code publish().autoConnect()}:
 *       with publish() all subscribers share one prefetch queue, so the slowest one throttles everybody,
 *       and the stream's lifecycle depends on its subscribers. directBestEffort delivers an event only to
 *       subscribers that have demand and never waits for the others.</li>
 *   <li>Slow clients: each subscriber gets its own bounded buffer that drops the oldest requests when
 *       full ({@link #rideRequests()}). Chosen over onBackpressureLatest because a driver should see
 *       several distinct requests, not only the newest one. A slow client loses old requests; nobody
 *       else is affected.</li>
 *   <li>Resume: the resume token of every event is kept. After an error (or if the stream completes)
 *       the watch is reopened with resumeAfter(token) and exponential backoff, so events that happened
 *       in between are not lost. The MongoDB driver already resumes on its own after "resumable" errors
 *       (killed cursor, primary step-down); this covers everything else. If the token is no longer in
 *       the oplog, the stream restarts from now.</li>
 * </ul>
 */
@Slf4j
public class RideRequestChangeStream implements SmartLifecycle {

    /** MongoDB error code: the resume token is no longer in the oplog. */
    private static final int CHANGE_STREAM_HISTORY_LOST = 286;

    private final ReactiveMongoTemplate mongoTemplate;
    private final ChangeStreamProperties properties;
    private final Sinks.Many<RideRequestEntity> sink = Sinks.many().multicast().directBestEffort();
    private final AtomicInteger watchesOpened = new AtomicInteger();
    private final AtomicLong droppedForSlowSubscribers = new AtomicLong();

    private volatile BsonValue resumeToken;
    private volatile Disposable subscription;

    public RideRequestChangeStream(ReactiveMongoTemplate mongoTemplate, ChangeStreamProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.properties = properties;
    }

    /** Live REQUESTED ride requests, for one subscriber (hot: only what happens after subscribing). */
    public Flux<RideRequestEntity> rideRequests() {
        return sink.asFlux()
                .onBackpressureBuffer(properties.subscriberBuffer(),
                        dropped -> droppedForSlowSubscribers.incrementAndGet(),
                        BufferOverflowStrategy.DROP_OLDEST);
    }

    /** How many times the underlying watch() was opened (1 + reopenings after failures). */
    public int watchesOpened() {
        return watchesOpened.get();
    }

    public long droppedForSlowSubscribers() {
        return droppedForSlowSubscribers.get();
    }

    @Override
    public void start() {
        subscription = Flux.defer(() -> {
                    watchesOpened.incrementAndGet();
                    return openChangeStream(resumeToken);
                })
                .doOnNext(change -> resumeToken = change.resumeToken())
                .doOnError(this::forgetTokenIfHistoryLost)
                .repeat()                                   // the stream ended (e.g. invalidated): reopen
                .retryWhen(Retry.backoff(Long.MAX_VALUE, properties.minRetryBackoff())
                        .maxBackoff(properties.maxRetryBackoff())
                        .doBeforeRetry(signal -> log.warn("Ride request change stream failed ({}), reopening {}",
                                signal.failure().toString(), resumeToken == null ? "from now" : "after the last event")))
                .subscribe(change -> sink.tryEmitNext(change.body()));  // FAIL_ZERO_SUBSCRIBER is fine: nobody listens
        log.info("Ride request change stream started");
    }

    /**
     * Opens the change stream on ride_requests (REQUESTED documents), resuming after {@code resumeAfter}
     * when given. Overridable so tests can inject failures or a fake source.
     */
    protected Flux<Change> openChangeStream(BsonValue resumeAfter) {
        ChangeStreamWithFilterAndProjection<RideRequestEntity> stream = mongoTemplate
                .changeStream(RideRequestEntity.class)
                .watchCollection("ride_requests")
                .filter(Criteria.where("status").is(RideRequestStatus.REQUESTED.toString()));
        return (resumeAfter == null ? stream.listen() : stream.resumeAfter(resumeAfter).listen())
                .filter(event -> event.getBody() != null)
                .map(Change::of);
    }

    private void forgetTokenIfHistoryLost(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof MongoException mongo && mongo.getCode() == CHANGE_STREAM_HISTORY_LOST) {
                log.warn("Resume token no longer in the oplog: the change stream restarts from now");
                resumeToken = null;
                return;
            }
        }
    }

    @Override
    public void stop() {
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
        }
        sink.tryEmitComplete();
    }

    @Override
    public boolean isRunning() {
        Disposable current = subscription;
        return current != null && !current.isDisposed();
    }

    /** A change: the document and the token to resume after it. */
    public record Change(RideRequestEntity body, BsonValue resumeToken) {
        static Change of(ChangeStreamEvent<RideRequestEntity> event) {
            return new Change(event.getBody(), event.getResumeToken());
        }
    }
}
