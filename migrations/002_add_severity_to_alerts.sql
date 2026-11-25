-- Migration 002: Add severity column to alerts if missing
-- This migration is idempotent

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_alerts' AND column_name = 'severity'
    ) THEN
        ALTER TABLE pollution_alerts ADD COLUMN severity VARCHAR(20) NOT NULL DEFAULT 'HIGH';
    END IF;
END $$;

-- Update all existing alert severities to match current thresholds
-- CRITICAL: PM2.5 >= 150, HIGH: PM2.5 >= 100 and < 150
UPDATE pollution_alerts
SET severity = 'CRITICAL'
WHERE pm25 >= 150 AND severity != 'CRITICAL';

UPDATE pollution_alerts
SET severity = 'HIGH'
WHERE pm25 >= 100 AND pm25 < 150 AND severity != 'HIGH';

UPDATE pollution_alerts
SET severity = 'NORMAL'
WHERE pm25 < 100 AND severity != 'NORMAL';

-- Create index on severity for efficient filtering
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_severity 
    ON pollution_alerts (severity);

-- Create index on event_id for deduplication
CREATE UNIQUE INDEX IF NOT EXISTS idx_pollution_alerts_event_id 
    ON pollution_alerts (event_id);

-- Add event_id column if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_alerts' AND column_name = 'event_id'
    ) THEN
        ALTER TABLE pollution_alerts ADD COLUMN event_id VARCHAR(64);
    END IF;
END $$;
