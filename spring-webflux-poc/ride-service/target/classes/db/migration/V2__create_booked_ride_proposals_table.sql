CREATE EXTENSION IF NOT EXISTS "pgcrypto";


CREATE TABLE booked_ride_proposals (
                                   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                    ride_proposal_id UUID NOT NULL ,
                                    driver_id UUID NOT NULL,
                                    driver_first_name VARCHAR(255) DEFAULT NULL,
                                    driver_last_name VARCHAR(255) DEFAULT NULL,
                                    driver_email VARCHAR(255) DEFAULT NULL,
                                    driver_phone_number VARCHAR(255) DEFAULT NULL,
                                    price DOUBLE PRECISION NOT NULL,
                                    accepted_at TIMESTAMP NOT NULL,
                                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);