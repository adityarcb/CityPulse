-- Migration 008: Cleanup test data, canonical city normalization, and single source of truth view
-- Idempotent script for CityPulse

-- 1. City aliases table
CREATE TABLE IF NOT EXISTS city_aliases (
    alias VARCHAR(100) PRIMARY KEY,
    canonical VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO city_aliases (alias, canonical)
VALUES ('Bangalore', 'Bengaluru')
ON CONFLICT (alias) DO UPDATE SET canonical = EXCLUDED.canonical;

-- 2. Add is_test column to pollution_readings if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_readings' AND column_name = 'is_test'
    ) THEN
        ALTER TABLE pollution_readings ADD COLUMN is_test BOOLEAN NOT NULL DEFAULT FALSE;
        CREATE INDEX idx_pollution_readings_is_test ON pollution_readings (is_test);
    END IF;
END $$;

-- 3. Add de_escalation_count to city_alert_state if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'city_alert_state' AND column_name = 'de_escalation_count'
    ) THEN
        ALTER TABLE city_alert_state ADD COLUMN de_escalation_count INTEGER NOT NULL DEFAULT 0;
    END IF;
END $$;

-- 4. Purge all test data across all tables
DELETE FROM pollution_readings WHERE city LIKE 'SmokeTest%';
DELETE FROM pollution_alerts WHERE city LIKE 'SmokeTest%';
DELETE FROM pollution_aggregates WHERE city LIKE 'SmokeTest%';
DELETE FROM pollution_anomalies WHERE city LIKE 'SmokeTest%';
DELETE FROM city_alert_state WHERE city LIKE 'SmokeTest%';

-- 5. Purge bogus repeat NORMAL alerts (alerts should only reflect transitions or threshold entries)
DELETE FROM pollution_alerts WHERE severity = 'NORMAL' AND alert_message LIKE 'Normal PM2.5 reading%';

-- 6. Canonicalize existing Bangalore entries to Bengaluru
UPDATE pollution_readings SET city = 'Bengaluru' WHERE city = 'Bangalore';
UPDATE pollution_alerts SET city = 'Bengaluru' WHERE city = 'Bangalore';
UPDATE pollution_aggregates SET city = 'Bengaluru' WHERE city = 'Bangalore';
UPDATE pollution_anomalies SET city = 'Bengaluru' WHERE city = 'Bangalore';

-- Merge city_alert_state for Bangalore and Bengaluru
DO $$
DECLARE
    blr_exists BOOLEAN;
    bgl_exists BOOLEAN;
BEGIN
    SELECT EXISTS (SELECT 1 FROM city_alert_state WHERE city = 'Bengaluru') INTO blr_exists;
    SELECT EXISTS (SELECT 1 FROM city_alert_state WHERE city = 'Bangalore') INTO bgl_exists;

    IF bgl_exists AND blr_exists THEN
        UPDATE city_alert_state
        SET alert_count = alert_count + (SELECT alert_count FROM city_alert_state WHERE city = 'Bangalore'),
            escalation_count = escalation_count + (SELECT escalation_count FROM city_alert_state WHERE city = 'Bangalore'),
            recovery_count = recovery_count + (SELECT recovery_count FROM city_alert_state WHERE city = 'Bangalore'),
            de_escalation_count = de_escalation_count + (SELECT de_escalation_count FROM city_alert_state WHERE city = 'Bangalore')
        WHERE city = 'Bengaluru';
        DELETE FROM city_alert_state WHERE city = 'Bangalore';
    ELSIF bgl_exists THEN
        UPDATE city_alert_state SET city = 'Bengaluru' WHERE city = 'Bangalore';
    END IF;
END $$;

-- 7. Sync current_severity in city_alert_state with the latest reading status for each canonical city
UPDATE city_alert_state s
SET current_severity = lr.status,
    updated_at = lr.recorded_at
FROM (
    SELECT DISTINCT ON (city) city, status, recorded_at
    FROM pollution_readings
    WHERE is_test = FALSE AND city NOT LIKE 'SmokeTest%'
    ORDER BY city, recorded_at DESC
) lr
WHERE s.city = lr.city;

-- 8. Single source of truth view: city_latest
CREATE OR REPLACE VIEW city_latest AS
SELECT DISTINCT ON (r.city)
    r.city,
    r.pm25,
    r.temperature,
    r.status,
    r.recorded_at,
    COALESCE(s.current_severity, r.status) AS current_severity,
    COALESCE(s.alert_count, 0) AS alert_count,
    COALESCE(s.escalation_count, 0) AS escalation_count,
    COALESCE(s.de_escalation_count, 0) AS de_escalation_count,
    COALESCE(s.recovery_count, 0) AS recovery_count,
    s.last_notification_at,
    s.last_severity_change_at
FROM pollution_readings r
LEFT JOIN city_alert_state s ON r.city = s.city
WHERE r.is_test = FALSE AND r.city NOT LIKE 'SmokeTest%'
ORDER BY r.city, r.recorded_at DESC;
