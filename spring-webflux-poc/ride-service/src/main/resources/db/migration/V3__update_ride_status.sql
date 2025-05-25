ALTER TABLE ride_requests
DROP CONSTRAINT IF EXISTS ride_requests_status_check;

ALTER TABLE ride_requests
    ADD CONSTRAINT ride_requests_status_check
        CHECK (status IN (
                          'REQUESTED',
                          'TO_ACCEPT',
                          'BOOKED',
                          'ONGOING',
                          'FINISHED',
                          'CANCELED'
            ));
