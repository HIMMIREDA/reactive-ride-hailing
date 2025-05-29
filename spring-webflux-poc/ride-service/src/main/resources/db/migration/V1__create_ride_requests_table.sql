CREATE EXTENSION IF NOT EXISTS "pgcrypto";


CREATE TABLE IF NOT EXISTS ride_requests (
                               ride_request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                               customer_id UUID NOT NULL,
                               accepted_ride_proposal_id UUID DEFAULT NULL,
                               seats INT NOT NULL,
                               preferred_price DOUBLE PRECISION NOT NULL,
                               start_lat DOUBLE PRECISION NOT NULL,
                               start_lng DOUBLE PRECISION NOT NULL,
                               end_lat DOUBLE PRECISION NOT NULL,
                               end_lng DOUBLE PRECISION NOT NULL,
                               status TEXT CHECK ( status IN  ('REQUESTED', 'ACCEPTED', 'BOOKED')),
                               created_at TIMESTAMP NOT NULL,
                               updated_at TIMESTAMP NOT NULL
);
