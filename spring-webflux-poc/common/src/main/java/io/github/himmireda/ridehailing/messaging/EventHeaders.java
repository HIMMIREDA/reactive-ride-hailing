package io.github.himmireda.ridehailing.messaging;

/** Kafka record headers set by the outbox relays and read by the idempotent consumers. */
public final class EventHeaders {

    /** Unique id of the event (the outbox row/document id). Consumers deduplicate on it. */
    public static final String EVENT_ID = "event-id";

    /** Full name of the Avro schema of the payload, e.g. io.github.himmireda.ridehailing.avro.RideRequestRequestedEvent. */
    public static final String EVENT_TYPE = "event-type";

    private EventHeaders() {
    }
}
