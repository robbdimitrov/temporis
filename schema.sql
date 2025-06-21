CREATE TABLE partitions (
    id VARCHAR(255) PRIMARY KEY
);

CREATE TABLE timers (
    id SERIAL PRIMARY KEY,
    partition_id VARCHAR(255) REFERENCES partitions(id),
    name VARCHAR(100),
    interval_ms BIGINT NOT NULL,
    once BOOLEAN NOT NULL
);
