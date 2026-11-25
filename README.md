# CityPulse — Real-Time Urban Pollution Analytics

## Project Overview

CityPulse is a real-time urban pollution analytics system that ingests pollution readings via Apache Kafka, processes them through Python ingestion and Spark Structured Streaming, stores data in PostgreSQL, and visualizes insights through an interactive Streamlit dashboard.

The system includes:
- **Event deduplication** with stable identity (producer-provided event_id or content-based hash)
- **Alert lifecycle management** with state transitions, cooldowns, escalation, de-escalation, and recovery
- **Statistical anomaly detection** using per-city rolling baselines
- **Geographic visualization** with approximate city-centre coordinates
- **Safe data retention** with dry-run mode and explicit confirmation

## Architecture

```
┌─────────────────┐
│  Kafka Topic    │
│ pollution-      │
│ readings        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│ Python          │     │ Spark           │
│ Ingestion       │     │ Streaming       │
│ Service         │     │ (5-min windows) │
│ + Alert         │     │                 │
│ + Lifecycle     │     │                 │
│ + Anomaly       │     │                 │
│ Detection       │     │                 │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────────────────────┐
│                      PostgreSQL                          │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────┐ │
│  │ readings     │  │ aggregates     │  │ alerts      │ │
│  │ + event_id   │  │ + upserts      │  │ + severity  │ │
│  │ + kafka_meta │  │                │  │ + event_id  │ │
│  └──────────────┘  └────────────────┘  └─────────────┘ │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────┐ │
│  │ alert_state  │  │ anomalies      │  │             │ │
│  │ (per city)   │  │ (z-score)      │  │             │ │
│  └──────────────┘  └────────────────┘  └─────────────┘ │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
           ┌─────────────────┐
           │ Streamlit       │
           │ Dashboard       │
           │ + Map           │
           │ + Alert State   │
           │ + Anomalies     │
           └─────────────────┘
```

## Technology Stack

- **Apache Kafka 4.1.1** - Real-time event streaming
- **Python 3.12** - Ingestion service with alert lifecycle and anomaly detection
- **Apache Spark 3.5.7** - Stream processing with PySpark Structured Streaming
- **PostgreSQL 17** - Persistent storage
- **Streamlit** - Interactive analytics dashboard with geographic map
- **Docker Compose** - Container orchestration

## Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- macOS (Apple Silicon supported) or Linux

### Setup

1. Clone the repository:
```bash
cd ~/CityPulse
```

2. Create environment file (optional for development):
```bash
cp .env.example .env
```

3. Start all services:
```bash
docker compose up -d
```

4. Wait for services to be healthy:
```bash
docker compose ps
```

All services should show "Up" status.

5. Access the dashboard:
```
http://localhost:8502
```

### Sending Test Events

Send test pollution events to Kafka:

```bash
docker exec citypulse-kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic pollution-readings
```

Then paste JSON events (one per line):
```json
{"city":"Delhi","pm25":125.0,"temperature":32.0}
{"city":"Mumbai","pm25":85.0,"temperature":30.0}
{"city":"Chennai","pm25":175.0,"temperature":35.0}
```

With optional producer-provided event_id and timestamp:
```json
{"city":"Delhi","pm25":125.0,"temperature":32.0,"event_id":"evt-001","timestamp":"2026-10-21T10:00:00+00:00"}
```

Or use a script:
```bash
docker exec citypulse-kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic pollution-readings <<EOF
{"city":"Amritsar","pm25":175.0,"temperature":36.0}
{"city":"Ludhiana","pm25":125.0,"temperature":34.0}
{"city":"Jalandhar","pm25":85.0,"temperature":31.0}
EOF
```

## Database Schema

### pollution_readings
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| event_id | VARCHAR(64) | Unique deduplication key (producer-provided or content hash) |
| city | VARCHAR(100) | City name |
| pm25 | DOUBLE PRECISION | PM2.5 value (µg/m³) |
| temperature | DOUBLE PRECISION | Temperature (°C) |
| status | VARCHAR(20) | NORMAL/HIGH/CRITICAL |
| recorded_at | TIMESTAMPTZ | Recording timestamp |
| kafka_topic | VARCHAR(100) | Kafka topic (for reference) |
| kafka_partition | INTEGER | Kafka partition (for reference) |
| kafka_offset | BIGINT | Kafka offset (for reference) |

### pollution_alerts
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| event_id | VARCHAR(64) | Unique deduplication key |
| city | VARCHAR(100) | City name |
| pm25 | DOUBLE PRECISION | PM2.5 value |
| alert_message | TEXT | Human-readable alert message |
| severity | VARCHAR(20) | HIGH/CRITICAL |
| created_at | TIMESTAMPTZ | Alert creation time |

### pollution_aggregates
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| city | VARCHAR(100) | City name |
| window_start | TIMESTAMP | Window start time |
| window_end | TIMESTAMP | Window end time |
| avg_pm25 | DOUBLE PRECISION | Average PM2.5 |
| max_pm25 | DOUBLE PRECISION | Maximum PM2.5 |
| reading_count | BIGINT | Number of readings |
| avg_temperature | DOUBLE PRECISION | Average temperature |
| status | VARCHAR(20) | Window severity |
| updated_at | TIMESTAMP | Last update time |

### city_alert_state (NEW)
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| city | VARCHAR(100) | City name (unique) |
| current_severity | VARCHAR(20) | Last known severity |
| last_notification_at | TIMESTAMPTZ | Last alert notification time |
| last_severity_change_at | TIMESTAMPTZ | Last severity transition time |
| alert_count | INTEGER | Total alerts generated |
| escalation_count | INTEGER | HIGH→CRITICAL escalations |
| recovery_count | INTEGER | HIGH→NORMAL recoveries |
| updated_at | TIMESTAMPTZ | Last update time |

### pollution_anomalies (NEW)
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| city | VARCHAR(100) | City name |
| pm25 | DOUBLE PRECISION | PM2.5 value |
| baseline_mean | DOUBLE PRECISION | Rolling mean baseline |
| baseline_stddev | DOUBLE PRECISION | Rolling stddev baseline |
| baseline_sample_count | INTEGER | Number of readings in baseline |
| z_score | DOUBLE PRECISION | Standard deviations from baseline |
| is_anomaly | BOOLEAN | Whether flagged as anomaly |
| anomaly_threshold | DOUBLE PRECISION | Z-score threshold used |
| event_id | VARCHAR(64) | References reading event_id |
| recorded_at | TIMESTAMPTZ | Detection timestamp |

## Severity Classification

Consistent across all components (ingestion, Spark, dashboard):
- **NORMAL**: PM2.5 < 100 µg/m³
- **HIGH**: PM2.5 ≥ 100 and < 150 µg/m³
- **CRITICAL**: PM2.5 ≥ 150 µg/m³

## Alert Lifecycle

The system maintains persistent per-city alert state in PostgreSQL. State transitions:

| Transition | Alert Type | Example Message |
|------------|-----------|-----------------|
| NORMAL → HIGH | Initial | "High PM2.5 alert: 120.0 µg/m³ (≥100)" |
| NORMAL → CRITICAL | Initial | "Critical PM2.5 alert: 175.0 µg/m³ (≥150)" |
| HIGH → CRITICAL | Escalation | "Alert escalated to CRITICAL: PM2.5 160.0 µg/m³ (≥150, was HIGH)" |
| CRITICAL → HIGH | De-escalation | "Alert de-escalated to HIGH: PM2.5 120.0 µg/m³ (<150, was CRITICAL)" |
| HIGH → NORMAL | Recovery | "Air quality recovered to NORMAL: PM2.5 80.0 µg/m³ (<100)" |
| CRITICAL → NORMAL | Recovery | "Air quality recovered to NORMAL: PM2.5 50.0 µg/m³ (<100)" |
| HIGH → HIGH | Repeat | Suppressed by cooldown (configurable, default 300s) |
| CRITICAL → CRITICAL | Repeat | Suppressed by cooldown (configurable, default 300s) |

### Cooldown Policy

- Initial alerts, escalations, de-escalations, and recoveries **always** notify
- Repeat notifications at the same severity are subject to a cooldown period
- Default cooldown: 300 seconds (5 minutes)
- Configurable via `ALERT_COOLDOWN_SECONDS` environment variable
- State survives container restarts via PostgreSQL persistence

## Event Identity and Deduplication

Event IDs are generated with the following priority:

1. **Producer-provided `event_id`** from message JSON (most reliable, survives replays)
2. **Content-based hash** of `city|pm25|temperature|[timestamp]` (deduplicates identical replays)
3. **Legacy fallback**: same content hash for backward compatibility

Kafka topic|partition|offset is stored for reference but NOT used as the primary event ID, because replays receive different offsets.

Deduplication guarantees:
- Replaying the same Kafka record does not create duplicate readings or alerts
- Two distinct Kafka records with identical measurement values are stored as one reading (content-based dedup)
- Two readings with different values are always stored separately

## Anomaly Detection

Statistical anomaly detection flags unusually high pollution readings compared with a city's own recent history.

- **Method**: Z-score = |current - baseline_mean| / baseline_stddev
- **Baseline**: Rolling mean and standard deviation of recent readings per city
- **Minimum history**: 5 readings (configurable via `ANOMALY_MIN_HISTORY`)
- **Threshold**: Z-score ≥ 3.0 (configurable via `ANOMALY_Z_THRESHOLD`)
- **Zero variance**: Handled gracefully (readings differing from constant baseline are flagged)
- **Insufficient history**: Not flagged as anomaly (informational only)

**Important**: An anomaly is a statistical signal, NOT a confirmed public health emergency. It is distinct from fixed HIGH/CRITICAL severity classifications.

## Geographic Visualization

- Map displays approximate city-centre locations (NOT precise sensor positions)
- Coordinates sourced from public geographic reference data
- Markers color-coded by severity (green=NORMAL, orange=HIGH, red=CRITICAL)
- Anomalous readings shown with larger markers
- Integrated with city filters
- Missing coordinates handled gracefully

## Data Retention

Safe retention utility with dry-run mode (default):

```bash
# Dry-run (default - safe, shows what would be deleted)
python scripts/retention.py

# Dry-run with custom retention
python scripts/retention.py --readings-days 90 --alerts-days 365

# Actual deletion (requires explicit --confirm)
python scripts/retention.py --confirm --readings-days 90

# Verbose dry-run
python scripts/retention.py --dry-run --verbose
```

Default retention periods:
- pollution_readings: 365 days
- pollution_alerts: 730 days
- pollution_anomalies: 365 days
- pollution_aggregates: 730 days

**Protected tables** (never deleted): city_alert_state, pollution_aggregates (recent windows)

## Spark Streaming

- **Window**: 5-minute tumbling windows
- **Watermark**: 1 minute (handles late-arriving data)
- **Trigger**: 5-second processing interval
- **Output Mode**: Update (existing windows are updated via upsert)

Event time is extracted from the optional `timestamp` field in the JSON message. If not present, processing time is used.

## Dashboard Features

### Live Alerts
- Total, critical, and high alert KPIs
- Recent alert cards with severity indicators
- CSV export

### Current Analytics
- Cities monitored count
- Average PM2.5 across cities
- Cities with HIGH/CRITICAL status
- Latest reading per city table
- Active alert state per city (severity, alert count, escalations, recoveries)

### Geographic Map
- City-level pollution severity on map
- Approximate city-centre locations
- Anomaly indicators

### Anomaly Detection
- Recent anomaly detections with explanations
- Z-score, baseline statistics for each flagged reading

### Charts
- Latest PM2.5 by city (with threshold lines)
- Temperature by city
- Daily PM2.5 trends
- Hourly PM2.5 trends

### Spark Aggregates
- Recent 5-minute window summaries
- Reading counts per window

### CSV Export
- Filtered historical readings with applied filters

### System Health
- Data freshness indicator
- Total readings, alerts, anomalies
- Cities with elevated severity

## Testing

Run the test suite:

```bash
# Install test dependencies
pip install -r tests/requirements.txt
pip install kafka-python  # for consumer imports

# Run all tests (86 tests)
pytest tests/ -v

# Run specific test file
pytest tests/test_ingestion.py -v
pytest tests/test_database.py -v
pytest tests/test_anomaly.py -v
pytest tests/test_alert_lifecycle.py -v
pytest tests/test_spark.py -v
```

### End-to-End Smoke Test

```bash
./scripts/smoke_test.sh
```

Verifies: service health, Kafka topic, PostgreSQL connectivity, table existence, end-to-end event ingestion, alert generation, idempotency (no duplicates on replay), and Spark aggregates.

## Operations

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f ingestion
docker compose logs -f spark
docker compose logs -f kafka
docker compose logs -f postgres
```

### Check Service Health
```bash
docker compose ps
```

### Restart Services
```bash
docker compose restart
```

### Stop Services
```bash
docker compose down
```

### Reset Data (⚠️ Destructive)
```bash
docker compose down -v
docker compose up -d
```

## Verification Queries

Connect to PostgreSQL:
```bash
docker exec -it citypulse-postgres-1 psql -U citypulse -d citypulse
```

Check readings:
```sql
SELECT COUNT(*) FROM pollution_readings;
SELECT city, pm25, status, recorded_at FROM pollution_readings ORDER BY recorded_at DESC LIMIT 10;
```

Check alerts:
```sql
SELECT COUNT(*) FROM pollution_alerts;
SELECT city, pm25, severity, alert_message FROM pollution_alerts ORDER BY created_at DESC LIMIT 10;
```

Check alert state:
```sql
SELECT city, current_severity, alert_count, escalation_count, recovery_count FROM city_alert_state ORDER BY alert_count DESC;
```

Check anomalies:
```sql
SELECT city, pm25, z_score, is_anomaly, baseline_mean FROM pollution_anomalies WHERE is_anomaly = TRUE ORDER BY recorded_at DESC LIMIT 10;
```

Check aggregates:
```sql
SELECT city, window_start, avg_pm25, max_pm25, reading_count, status FROM pollution_aggregates ORDER BY window_start DESC LIMIT 10;
```

## Troubleshooting

### Kafka Connection Issues
```bash
# Check if Kafka is ready
docker exec citypulse-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### PostgreSQL Connection Issues
```bash
# Check if PostgreSQL is accepting connections
docker exec citypulse-postgres-1 pg_isready -U citypulse
```

### Spark Not Processing
- Check logs: `docker compose logs spark`
- Verify Kafka topic exists: `docker exec citypulse-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
- Check checkpoint directory permissions

### Dashboard Not Loading
- Verify PostgreSQL is running: `docker compose ps postgres`
- Check environment variables in dashboard/
- Verify DB password matches

### Ingestion Errors
- Check logs: `docker compose logs ingestion`
- Common errors: invalid JSON (logged and skipped), DB connection issues (retry automatically)

## Project Structure

```
CityPulse/
├── dashboard/
│   ├── app.py              # Streamlit dashboard
│   ├── city_coordinates.py # City coordinate definitions
│   ├── .streamlit/
│   │   └── config.toml     # Streamlit server config (port 8502)
│   └── requirements.txt
├── ingestion/
│   ├── consumer.py         # Kafka consumer with alert lifecycle + anomaly detection
│   ├── anomaly_detector.py # Statistical anomaly detection module
│   └── requirements.txt
├── spark_jobs/
│   └── kafka_stream.py     # Spark Structured Streaming
├── migrations/
│   ├── 001_initial_schema.sql
│   ├── 002_add_severity_to_alerts.sql
│   ├── 003_add_event_id_to_readings.sql
│   ├── 004_add_event_id_to_alerts.sql
│   ├── 005_kafka_metadata_columns.sql
│   ├── 006_alert_state_table.sql
│   └── 007_pollution_anomalies.sql
├── tests/
│   ├── test_ingestion.py
│   ├── test_database.py
│   ├── test_spark.py
│   ├── test_anomaly.py
│   └── test_alert_lifecycle.py
├── scripts/
│   ├── smoke_test.sh       # End-to-end smoke test
│   └── retention.py        # Safe data retention utility
├── docker-compose.yml
├── .env.example
├── .env
├── .gitignore
├── config.py               # Centralized severity thresholds
└── README.md
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_SERVERS | kafka:9092 | Kafka bootstrap servers |
| DATABASE_URL | postgresql://... | PostgreSQL connection URL |
| CITYPULSE_DB_PASSWORD | citypulse_dev_password | PostgreSQL password |
| HIGH_THRESHOLD | 100 | PM2.5 HIGH threshold |
| CRITICAL_THRESHOLD | 150 | PM2.5 CRITICAL threshold |
| ALERT_COOLDOWN_SECONDS | 300 | Cooldown between repeat alerts (seconds) |
| ANOMALY_Z_THRESHOLD | 3.0 | Z-score threshold for anomaly detection |
| ANOMALY_MIN_HISTORY | 5 | Minimum readings for baseline |
| READINGS_RETENTION_DAYS | 365 | Retention days for readings |
| ALERTS_RETENTION_DAYS | 730 | Retention days for alerts |
| ANOMALIES_RETENTION_DAYS | 365 | Retention days for anomalies |
| AGGREGATES_RETENTION_DAYS | 730 | Retention days for aggregates |

## Known Limitations

1. Single Kafka broker (not fault-tolerant)
2. Processing-time fallback when event timestamp not provided
3. No authentication on Kafka or PostgreSQL (development setup)
4. Geographic coordinates are approximate city centres, not precise sensor locations
5. Content-based deduplication means identical readings at different times are merged (when no producer event_id or timestamp)

## Future Improvements

1. Multi-broker Kafka cluster for fault tolerance
2. Authentication and TLS for Kafka and PostgreSQL
3. Precise sensor locations with device registration
4. Alert cooldown per severity level (different cooldowns for HIGH vs CRITICAL)
5. Machine learning-based anomaly detection
6. Mobile push notifications for critical alerts
