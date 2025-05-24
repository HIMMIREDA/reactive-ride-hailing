CREATE TABLE accepted_proposals (
                                    id UUID PRIMARY KEY,
                                    ride_request_id UUID NOT NULL,
                                    proposal_id UUID NOT NULL,
                                    driver_id UUID NOT NULL,
                                    price DOUBLE PRECISION NOT NULL,
                                    accepted_at TIMESTAMP NOT NULL,
                                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);