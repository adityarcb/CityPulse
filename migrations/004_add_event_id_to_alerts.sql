-- Migration 004: Add event_id column to pollution_alerts
-- This migration is idempotent

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_alerts' AND column_name = 'event_id'
    ) THEN
        ALTER TABLE pollution_alerts ADD COLUMN event_id VARCHAR(64);
        
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pollution_alerts_event_id ON pollution_alerts (event_id);
    END IF;
END $$;
