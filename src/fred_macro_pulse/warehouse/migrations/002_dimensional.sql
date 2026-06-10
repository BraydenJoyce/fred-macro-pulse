CREATE TABLE IF NOT EXISTS dim_series (
    series_id           VARCHAR PRIMARY KEY,
    title               VARCHAR NOT NULL,
    units               VARCHAR,
    frequency           VARCHAR,           -- Monthly, Quarterly, Weekly, Daily, etc.
    seasonal_adjustment VARCHAR,           -- SA, NSA, SAAR
    category            VARCHAR,           -- Growth, Labor, Inflation, etc.
    notes               VARCHAR,
    last_updated        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_observations (
    series_id        VARCHAR  NOT NULL,
    observation_date DATE     NOT NULL,
    value            DOUBLE,              -- null means missing
    is_revised       BOOLEAN  DEFAULT FALSE,
    loaded_at        TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_date)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       VARCHAR   PRIMARY KEY,
    started_at   TIMESTAMP NOT NULL DEFAULT now(),
    finished_at  TIMESTAMP,
    series_count INTEGER,
    rows_loaded  INTEGER,
    status       VARCHAR   DEFAULT 'running',   -- running | success | failed
    error_msg    VARCHAR
);
