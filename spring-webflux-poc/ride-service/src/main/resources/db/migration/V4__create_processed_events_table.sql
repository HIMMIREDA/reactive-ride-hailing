-- Idempotent consumers: one row per (event, consumer), inserted in the same transaction as the state
-- change. A redelivered event hits the primary key and is skipped.
CREATE TABLE processed_events (
    event_id     TEXT        NOT NULL,
    consumer     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer)
);
