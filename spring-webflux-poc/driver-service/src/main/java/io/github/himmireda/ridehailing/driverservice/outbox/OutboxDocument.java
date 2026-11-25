package io.github.himmireda.ridehailing.driverservice.outbox;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** An event waiting to be published (transactional outbox). */
@Document(collection = "outbox")
@CompoundIndex(name = "outbox_unpublished_idx", def = "{'publishedAt': 1, 'createdAt': 1}")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxDocument {
    @Id
    private String id;              // also the event id (Kafka header "event-id")
    private String aggregateId;
    private String eventType;       // Avro full name, used to decode the payload
    private String topic;
    private String messageKey;
    private byte[] payload;         // Avro binary of the event
    private Instant createdAt;
    private Instant publishedAt;    // set once Kafka acknowledged the record
    private int attempts;
    private String claimedBy;       // relay currently publishing it
    private Instant claimedUntil;   // claim lease
}
