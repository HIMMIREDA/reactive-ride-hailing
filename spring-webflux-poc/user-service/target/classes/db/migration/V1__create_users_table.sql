CREATE TABLE users (
                       id BIGSERIAL PRIMARY KEY,
                       first_name VARCHAR(255),
                       last_name VARCHAR(255),
                       email VARCHAR(255) NOT NULL,
                       password VARCHAR(255),
                       created_at DATE,
                       updated_at DATE,
                       CONSTRAINT uq_users_email UNIQUE (email)
);
