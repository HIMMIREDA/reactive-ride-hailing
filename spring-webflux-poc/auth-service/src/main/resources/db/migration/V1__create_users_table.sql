CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
                       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                       first_name VARCHAR(255),
                       last_name VARCHAR(255),
                       email VARCHAR(255) NOT NULL,
                       password VARCHAR(255),
                       role TEXT CHECK (role IN ('USER', 'DRIVER')),
                       created_at timestamptz,
                       updated_at timestamptz
);

CREATE UNIQUE INDEX unique_email_user
    ON users(email)
    WHERE role = 'USER';

CREATE UNIQUE INDEX unique_email_driver
    ON users(email)
    WHERE role = 'DRIVER';
