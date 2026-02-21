-- PostgreSQL 16
-- TABLE 1: raw_sensor_data
-- Stores every valid processed row from each incoming file
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMP NOT NULL,
    sensor_id       VARCHAR(50) NOT NULL,
    co              FLOAT,
    humidity        FLOAT,
    light_detected  BOOLEAN,
    lpg             FLOAT,
    motion_detected BOOLEAN,
    smoke           FLOAT,
    temperature     FLOAT,
    source_file     VARCHAR(255),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- TABLE 2: aggregated_metrics
-- Stores per-device min/max/mean/std stats per file
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id                  SERIAL PRIMARY KEY,
    sensor_id           VARCHAR(50) NOT NULL,
    co_min              FLOAT,
    co_max              FLOAT,
    co_mean             FLOAT,
    co_std              FLOAT,
    humidity_min        FLOAT,
    humidity_max        FLOAT,
    humidity_mean       FLOAT,
    humidity_std        FLOAT,
    lpg_min             FLOAT,
    lpg_max             FLOAT,
    lpg_mean            FLOAT,
    lpg_std             FLOAT,
    smoke_min           FLOAT,
    smoke_max           FLOAT,
    smoke_mean          FLOAT,
    smoke_std           FLOAT,
    temperature_min     FLOAT,
    temperature_max     FLOAT,
    temperature_mean    FLOAT,
    temperature_std     FLOAT,
    source_file         VARCHAR(255),
    processed_at        TIMESTAMP DEFAULT NOW()
);

-- Indexes on raw_sensor_data for fast querying
CREATE INDEX IF NOT EXISTS idx_raw_sensor_id ON raw_sensor_data(sensor_id);
CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_sensor_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_source_file ON raw_sensor_data(source_file);
CREATE INDEX IF NOT EXISTS idx_raw_sensor_time ON raw_sensor_data(sensor_id, timestamp);

-- Indexes on aggregated_metrics for fast querying
CREATE INDEX IF NOT EXISTS idx_agg_sensor_id ON aggregated_metrics(sensor_id);
CREATE INDEX IF NOT EXISTS idx_agg_source_file ON aggregated_metrics(source_file);