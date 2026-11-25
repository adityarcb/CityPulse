-- Migration 005: Add Kafka metadata columns for stable event identity
-- This migration is idempotent
--
-- Purpose: Enable reliable deduplication by recording the Kafka topic,
-- partition, and offset for each message. This allows us to generate
-- stable event IDs for messages that don't have a producer-provided
-- event_id, without using city+pm25+temperature (which can collide
-- for identical readings at different times).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_readings' AND column_name = 'kafka_topic'
    ) THEN
        ALTER TABLE pollution_readings ADD COLUMN kafka_topic VARCHAR(100);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_readings' AND column_name = 'kafka_partition'
    ) THEN
        ALTER TABLE pollution_readings ADD COLUMN kafka_partition INTEGER;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pollution_readings' AND column_name = 'kafka_offset'
    ) THEN
        ALTER TABLE pollution_readings ADD COLUMN kafka_offset BIGINT;
    END IF;
END $$;

-- Create composite index for Kafka-based lookup
CREATE INDEX IF NOT EXISTS idx_pollution_kafka_offset 
    ON pollution_readings (kafka_topic, kafka_partition, kafka_offset);

-- Add comment for documentation
COMMENT ON COLUMN pollution_readings.event_id IS 
    'Producer-provided event_id if available, otherwise SHA-256 of kafka_topic|partition|offset';
COMMENT ON COLUMN pollution_readings.kafka_topic IS 'Kafka topic name for deduplication fallback';
COMMENT ON COLUMN pollution_readings.kafka_partition IS 'Kafka partition number for deduplication fallback';
COMMENT ON COLUMN pollution_readings.kafka_offset IS 'Kafka message offset for deduplication fallback';
