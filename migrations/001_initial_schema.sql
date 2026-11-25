-- Migration 001: Initial Schema
-- Creates the core tables for CityPulse pollution tracking

-- Pollution readings table with event deduplication
CREATE TABLE IF NOT EXISTS pollution_readings (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE,
    city VARCHAR(100) NOT NULL,
    pm25 DOUBLE PRECISION NOT NULL CHECK (pm25 >= 0),
    temperature DOUBLE PRECISION,
    status VARCHAR(20) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for readings
CREATE INDEX IF NOT EXISTS idx_pollution_city_time 
    ON pollution_readings (city, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_event_id 
    ON pollution_readings (event_id);
CREATE INDEX IF NOT EXISTS idx_pollution_status 
    ON pollution_readings (status);

-- Pollution alerts table with event deduplication and severity
CREATE TABLE IF NOT EXISTS pollution_alerts (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE,
    city VARCHAR(100) NOT NULL,
    pm25 DOUBLE PRECISION NOT NULL CHECK (pm25 >= 0),
    alert_message TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for alerts
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_created_at 
    ON pollution_alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_severity 
    ON pollution_alerts (severity);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_city 
    ON pollution_alerts (city);

-- Spark aggregates table
CREATE TABLE IF NOT EXISTS pollution_aggregates (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_pm25 DOUBLE PRECISION NOT NULL,
    max_pm25 DOUBLE PRECISION NOT NULL,
    reading_count BIGINT NOT NULL,
    avg_temperature DOUBLE PRECISION,
    status VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pollution_aggregates_city_window_start_window_end_key 
        UNIQUE (city, window_start, window_end)
);

-- Indexes for aggregates
CREATE INDEX IF NOT EXISTS idx_aggregates_city_window 
    ON pollution_aggregates (city, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_aggregates_status 
    ON pollution_aggregates (status);

-- Comments for documentation
COMMENT ON TABLE pollution_readings IS 'Raw pollution readings from Kafka with deduplication via event_id';
COMMENT ON TABLE pollution_alerts IS 'Pollution alerts generated for HIGH and CRITICAL readings';
COMMENT ON TABLE pollution_aggregates IS 'Spark 5-minute window aggregates with city-level statistics';
