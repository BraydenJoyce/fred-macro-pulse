CREATE TABLE IF NOT EXISTS raw_observations (
    series_id        VARCHAR   NOT NULL,
    observation_date DATE      NOT NULL,
    value            VARCHAR,            -- raw string from API; "." means missing
    vintage_date     TIMESTAMP NOT NULL DEFAULT now(),
    run_id           VARCHAR   NOT NULL
);
