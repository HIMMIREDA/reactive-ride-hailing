package io.github.himmireda.ridehailing.driverservice.kafka;

/**
 * An event refers to an entity this service doesn't have. Thrown by listeners so the record is
 * retried (it may be a race between topics) and then sent to the dead-letter topic: never skipped
 * silently.
 */
public class EventTargetNotFoundException extends RuntimeException {
    public EventTargetNotFoundException(String message) {
        super(message);
    }
}
