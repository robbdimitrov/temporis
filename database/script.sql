CREATE DATABASE timers;

\c timers

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

INSERT INTO partitions (id) VALUES ('partition-1'), ('partition-2'), ('partition-3'), ('partition-4'), ('partition-5'), ('partition-6');

INSERT INTO timers (partition_id, name, interval_ms, once)
SELECT
    (ARRAY['partition-1', 'partition-2', 'partition-3', 'partition-4', 'partition-5', 'partition-6'])[floor(random() * 6 + 1)],
    'timer-' || generate_series(1, 20),
    floor(random() * 10000 + 1000)::bigint,
    (random() > 0.5)::boolean
FROM generate_series(1, 20);
