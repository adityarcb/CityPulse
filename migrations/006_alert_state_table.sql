-- Migration 006: Persistent alert state management
-- This migration is idempotent
--
-- Purpose: Track per-city alert state to implement:
--   - Initial alert on NORMAL → HIGH/CRITICAL transition
--   - Escalation alert on HIGH → CRITICAL transition
--   - Cooldown to prevent alert flooding
--   - De-escalation on CRITICAL → HIGH
--   - Recovery notification on HIGH → NORMAL
--
-- State survives container restarts via PostgreSQL persistence.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'city_alert_state'
    ) THEN
        CREATE TABLE city_alert_state (
            id BIGSERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL UNIQUE,
            current_severity VARCHAR(20) NOT NULL DEFAULT 'NORMAL',
            last_notification_at TIMESTAMPTZ,
            last_severity_change_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            alert_count INTEGER NOT NULL DEFAULT 0,
            escalation_count INTEGER NOT NULL DEFAULT 0,
            recovery_count INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_city_alert_state_city 
            ON city_alert_state (city);
        CREATE INDEX IF NOT EXISTS idx_city_alert_state_severity 
            ON city_alert_state (current_severity);
        CREATE INDEX IF NOT EXISTS idx_city_alert_state_updated_at 
            ON city_alert_state (updated_at DESC);

        COMMENT ON TABLE city_alert_state IS 'Persistent per-city alert state for lifecycle management';
        COMMENT ON COLUMN city_alert_state.current_severity IS 'Last known severity classification for the city';
        COMMENT ON COLUMN city_alert_state.last_notification_at IS 'Timestamp of last alert notification sent';
        COMMENT ON COLUMN city_alert_state.last_severity_change_at IS 'Timestamp of last severity transition';
        COMMENT ON COLUMN city_alert_state.alert_count IS 'Total number of alerts generated for this city';
        COMMENT ON COLUMN city_alert_state.escalation_count IS 'Number of HIGH→CRITICAL escalations';
        COMMENT ON COLUMN city_alert_state.recovery_count IS 'Number of HIGH→NORMAL recoveries';
    END IF;
END $$;

-- Backfill existing cities from pollution_readings into alert state
INSERT INTO city_alert_state (city, current_severity, last_severity_change_at, updated_at)
SELECT 
    city,
    CASE 
        WHEN MAX(pm25) >= 150 THEN 'CRITICAL'
        WHEN MAX(pm25) >= 100 THEN 'HIGH'
        ELSE 'NORMAL'
    END,
    MAX(recorded_at),
    MAX(recorded_at)
FROM pollution_readings
GROUP BY city
ON CONFLICT (city) DO NOTHING;
