-- Migration 003: Add event_id column to pollution_readings
-- This migration is idempotent

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_readings' AND column_name = 'event_id'
    ) THEN
        ALTER TABLE pollution_readings ADD COLUMN event_id VARCHAR(64);
        
        -- Create unique index on event_id
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pollution_event_id ON pollution_readings (event_id);
        
        -- For existing rows, generate event_id from existing data
        -- Note: This is a best-effort approach for existing data
        UPDATE pollution_readings
        SET event_id = encode(sha256((city || '|' || pm25 || '|' || COALESCE(temperature::text, '') || '|' || recorded_at::text)::bytea), 'hex')::varchar(16)
        WHERE event_id IS NULL;
        
        -- Now make event_id unique (but not required for backward compatibility)
        -- New records will require event_id, old records can have NULL
    END IF;
END $$;
