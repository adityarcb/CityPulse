-- Migration 007: Pollution anomaly detection results
-- This migration is idempotent
--
-- Purpose: Store statistical anomaly detection results for pollution readings.
-- Anomalies are distinct from fixed HIGH/CRITICAL severity thresholds;
-- they flag readings that are unusual relative to a city's own baseline.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'pollution_anomalies'
    ) THEN
        CREATE TABLE pollution_anomalies (
            id BIGSERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            pm25 DOUBLE PRECISION NOT NULL CHECK (pm25 >= 0),
            baseline_mean DOUBLE PRECISION NOT NULL,
            baseline_stddev DOUBLE PRECISION NOT NULL,
            baseline_sample_count INTEGER NOT NULL,
            z_score DOUBLE PRECISION NOT NULL,
            is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
            anomaly_threshold DOUBLE PRECISION NOT NULL,
            event_id VARCHAR(64),
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pollution_anomalies_event_id_key UNIQUE (event_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pollution_anomalies_city 
            ON pollution_anomalies (city);
        CREATE INDEX IF NOT EXISTS idx_pollution_anomalies_recorded_at 
            ON pollution_anomalies (recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_pollution_anomalies_is_anomaly 
            ON pollution_anomalies (is_anomaly);

        COMMENT ON TABLE pollution_anomalies IS 'Statistical anomaly detection results for pollution readings';
        COMMENT ON COLUMN pollution_anomalies.baseline_mean IS 'Rolling mean of recent readings for this city';
        COMMENT ON COLUMN pollution_anomalies.baseline_stddev IS 'Rolling standard deviation of recent readings for this city';
        COMMENT ON COLUMN pollution_anomalies.baseline_sample_count IS 'Number of readings used to compute baseline';
        COMMENT ON COLUMN pollution_anomalies.z_score IS 'Standard deviations from baseline mean';
        COMMENT ON COLUMN pollution_anomalies.is_anomaly IS 'Whether this reading was flagged as an anomaly';
        COMMENT ON COLUMN pollution_anomalies.event_id IS 'References the pollution_readings event_id';
    END IF;
END $$;
