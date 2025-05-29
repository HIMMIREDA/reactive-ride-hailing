ALTER TABLE rides
DROP CONSTRAINT IF EXISTS rides_status_check;

ALTER TABLE rides
    ADD CONSTRAINT ride_requests_status_check
        CHECK (status IN (
                          'REQUESTED',
                          'TO_ACCEPT',
                          'BOOKED',
                          'ONGOING',
                          'FINISHED',
                          'CANCELED'
            ));
