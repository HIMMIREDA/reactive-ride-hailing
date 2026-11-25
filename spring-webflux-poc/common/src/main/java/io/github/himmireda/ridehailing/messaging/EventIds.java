package io.github.himmireda.ridehailing.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/** Identity of a delivered event, used by idempotent consumers. */
public final class EventIds {

    private EventIds() {
    }

    /**
     * The {@value EventHeaders#EVENT_ID} header set by the outbox relay. Records without it (sent by
     * something else) fall back to their position, which is unique per topic-partition: a redelivery
     * of the same record is still recognized.
     */
    public static String of(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EventHeaders.EVENT_ID);
        if (header != null && header.value() != null && header.value().length > 0) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
