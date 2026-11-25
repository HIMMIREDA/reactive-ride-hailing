-- Transactional outbox: events are written here in the same transaction as the business change,
-- then published to Kafka by the outbox relay (OutboxRelay).
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,               -- also the event id (Kafka header "event-id")
    aggregate_id TEXT        NOT NULL,           -- the ride request the event is about
    event_type   TEXT        NOT NULL,           -- Avro full name, used to decode the payload
    topic        TEXT        NOT NULL,
    message_key  TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,           -- Avro binary of the event
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,                    -- set once Kafka acknowledged the record
    attempts     INT         NOT NULL DEFAULT 0
);

-- The relay only ever reads unpublished rows, oldest first.
CREATE INDEX outbox_unpublished_idx ON outbox (created_at, id) WHERE published_at IS NULL;
