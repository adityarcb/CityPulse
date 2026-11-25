import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

from anomaly_detector import detect_anomaly, get_historical_readings_for_city, persist_anomaly_result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:9092")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://citypulse:citypulse_dev_password@postgres:5432/citypulse"
)
HIGH_THRESHOLD = float(os.getenv("HIGH_THRESHOLD", "100"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "150"))

shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


def classify_severity(pm25: float) -> str:
    if pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


def get_alert_message(pm25: float, severity: str) -> str:
    if severity == "CRITICAL":
        return f"Critical PM2.5 reading: {pm25:.1f} µg/m³ (≥{CRITICAL_THRESHOLD:.0f})"
    elif severity == "HIGH":
        return f"High PM2.5 reading: {pm25:.1f} µg/m³ (≥{HIGH_THRESHOLD:.0f})"
    return f"Normal PM2.5 reading: {pm25:.1f} µg/m³"


CITY_ALIASES = {
    "Bangalore": "Bengaluru",
}

VALID_CITIES = {
    "Amritsar", "Bengaluru", "Chennai", "Delhi", "Hyderabad",
    "Jalandhar", "Kolkata", "Ludhiana", "Mumbai", "Pune",
}

ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "300"))


def get_alert_transition_message(
    previous_severity: str,
    new_severity: str,
    pm25: float,
) -> tuple[str, str]:
    """
    Generate alert type and message based on severity transition.

    Returns (alert_type, alert_message) where alert_type is one of:
    - 'initial'     : First alert when entering HIGH/CRITICAL from NORMAL
    - 'escalation'  : HIGH → CRITICAL escalation
    - 'de-escalation': CRITICAL → HIGH reduction in severity
    - 'recovery'    : HIGH → NORMAL return to safe levels
    - 'repeat'      : Same severity as current (may be suppressed by cooldown)
    """
    if new_severity == "CRITICAL":
        if previous_severity == "NORMAL":
            return (
                "initial",
                f"Critical PM2.5 alert: {pm25:.1f} µg/m³ (≥{CRITICAL_THRESHOLD:.0f})",
            )
        elif previous_severity == "HIGH":
            return (
                "escalation",
                f"Alert escalated to CRITICAL: PM2.5 {pm25:.1f} µg/m³ "
                f"(≥{CRITICAL_THRESHOLD:.0f}, was HIGH)",
            )
        else:
            return (
                "repeat",
                f"Critical PM2.5 reading continues: {pm25:.1f} µg/m³ "
                f"(≥{CRITICAL_THRESHOLD:.0f})",
            )
    elif new_severity == "HIGH":
        if previous_severity == "NORMAL":
            return (
                "initial",
                f"High PM2.5 alert: {pm25:.1f} µg/m³ (≥{HIGH_THRESHOLD:.0f})",
            )
        elif previous_severity == "CRITICAL":
            return (
                "de-escalation",
                f"Alert de-escalated to HIGH: PM2.5 {pm25:.1f} µg/m³ "
                f"(<{CRITICAL_THRESHOLD:.0f}, was CRITICAL)",
            )
        else:
            return (
                "repeat",
                f"High PM2.5 reading continues: {pm25:.1f} µg/m³ "
                f"(≥{HIGH_THRESHOLD:.0f})",
            )
    else:  # NORMAL
        if previous_severity in ("HIGH", "CRITICAL"):
            return (
                "recovery",
                f"Air quality recovered to NORMAL: PM2.5 {pm25:.1f} µg/m³ "
                f"(<{HIGH_THRESHOLD:.0f})",
            )
        else:
            return (
                "repeat",
                f"Normal PM2.5 reading: {pm25:.1f} µg/m³ (<{HIGH_THRESHOLD:.0f})",
            )


def should_notify(
    previous_severity: str,
    new_severity: str,
    alert_type: str,
    last_notification_at,
    cooldown_seconds: int,
) -> bool:
    """
    Determine whether a notification should be sent based on alert type and cooldown.

    Rules:
    - Normal to normal repeat is never an alert
    - Initial alerts (NORMAL -> HIGH, NORMAL -> CRITICAL), escalations, de-escalations, and recoveries always notify
    - Repeat alerts for HIGH or CRITICAL are subject to cooldown
    """
    # Repeat normal is never an alert
    if new_severity == "NORMAL" and previous_severity == "NORMAL":
        return False

    # Always notify on meaningful severity transitions
    if alert_type in ("initial", "escalation", "de-escalation", "recovery"):
        return True

    # Repeat alerts for HIGH or CRITICAL: check cooldown
    if alert_type == "repeat":
        if new_severity not in ("HIGH", "CRITICAL"):
            return False
        if last_notification_at is None:
            return True
        from datetime import datetime, timezone

        # Handle both datetime objects and ISO format strings
        if isinstance(last_notification_at, datetime):
            last_time = last_notification_at
            if last_time.tzinfo is None:
                last_time = last_time.replace(tzinfo=timezone.utc)
        else:
            last_time = datetime.fromisoformat(
                str(last_notification_at).replace("Z", "+00:00")
            )
        now = datetime.now(timezone.utc)
        elapsed = (now - last_time).total_seconds()
        return elapsed >= cooldown_seconds

    return False


def get_city_alert_state(connection, city: str) -> dict:
    """
    Fetch current alert state for a city without mutating it.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_severity, last_notification_at, last_severity_change_at,
                   alert_count, escalation_count, de_escalation_count, recovery_count
            FROM city_alert_state
            WHERE city = %s
            """,
            (city,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "previous_severity": row[0],
                "last_notification_at": row[1],
                "last_severity_change_at": row[2],
                "alert_count": row[3],
                "escalation_count": row[4],
                "de_escalation_count": row[5] if len(row) > 5 and row[5] is not None else 0,
                "recovery_count": row[6] if len(row) > 6 and row[6] is not None else 0,
            }
        return {
            "previous_severity": "NORMAL",
            "last_notification_at": None,
            "last_severity_change_at": None,
            "alert_count": 0,
            "escalation_count": 0,
            "de_escalation_count": 0,
            "recovery_count": 0,
        }


def update_city_alert_state(
    connection,
    city: str,
    new_severity: str,
    alert_type: str,
    notified: bool,
    pm25: float = 0.0,
) -> dict | None:
    """
    Atomically update the city alert state.
    Returns dict with previous and current state fields.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_severity, last_notification_at, last_severity_change_at,
                   alert_count, escalation_count, de_escalation_count, recovery_count
            FROM city_alert_state
            WHERE city = %s
            FOR UPDATE
            """,
            (city,),
        )
        row = cursor.fetchone()

        if row is None:
            # First time seeing this city - insert initial state
            is_esc = 1 if alert_type == "escalation" else 0
            is_deesc = 1 if alert_type == "de-escalation" else 0
            is_rec = 1 if alert_type == "recovery" else 0
            alert_inc = 1 if notified else 0

            cursor.execute(
                """
                INSERT INTO city_alert_state
                    (city, current_severity, last_notification_at,
                     last_severity_change_at, alert_count, escalation_count,
                     de_escalation_count, recovery_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING current_severity, last_notification_at,
                          last_severity_change_at, alert_count, escalation_count,
                          de_escalation_count, recovery_count
                """,
                (
                    city,
                    new_severity,
                    now if notified else None,
                    now,
                    alert_inc,
                    is_esc,
                    is_deesc,
                    is_rec,
                    now,
                ),
            )
            result = cursor.fetchone()
            return {
                "previous_severity": "NORMAL",
                "current_severity": result[0],
                "last_notification_at": result[1],
                "alert_count": result[3],
                "escalation_count": result[4],
                "de_escalation_count": result[5],
                "recovery_count": result[6],
            }

        previous_severity = row[0]
        last_notification_at = row[1]
        alert_count = row[3]
        escalation_count = row[4]
        de_escalation_count = row[5] or 0
        recovery_count = row[6] or 0

        # Update the state
        new_alert_count = alert_count + (1 if notified else 0)
        new_escalation_count = escalation_count + (1 if alert_type == "escalation" else 0)
        new_de_escalation_count = de_escalation_count + (1 if alert_type == "de-escalation" else 0)
        new_recovery_count = recovery_count + (1 if alert_type == "recovery" else 0)
        new_last_change = now if previous_severity != new_severity else row[2]

        cursor.execute(
            """
            UPDATE city_alert_state
            SET current_severity = %s,
                last_notification_at = %s,
                last_severity_change_at = %s,
                alert_count = %s,
                escalation_count = %s,
                de_escalation_count = %s,
                recovery_count = %s,
                updated_at = %s
            WHERE city = %s
            RETURNING current_severity, last_notification_at,
                      last_severity_change_at, alert_count, escalation_count,
                      de_escalation_count, recovery_count
            """,
            (
                new_severity,
                now if notified else last_notification_at,
                new_last_change,
                new_alert_count,
                new_escalation_count,
                new_de_escalation_count,
                new_recovery_count,
                now,
                city,
            ),
        )
        result = cursor.fetchone()

        return {
            "previous_severity": previous_severity,
            "current_severity": result[0],
            "last_notification_at": result[1],
            "alert_count": result[3],
            "escalation_count": result[4],
            "de_escalation_count": result[5],
            "recovery_count": result[6],
        }


def generate_event_id(
    city: str,
    pm25: float,
    temperature: float,
    producer_event_id: str = None,
    kafka_topic: str = None,
    kafka_partition: int = None,
    kafka_offset: int = None,
    timestamp: str = None,
) -> str:
    """
    Generate a stable event ID for deduplication.

    Priority:
    1. Producer-provided event_id (from message JSON) - most reliable,
       survives replays because the producer sets it once
    2. City|pm25|temperature|timestamp - content-based, deduplicates
       identical replays when no producer event_id is available
    3. City|pm25|temperature - best-effort fallback for legacy events
       without timestamps (accepts that identical content is deduplicated)

    Kafka topic|partition|offset is stored for reference but NOT used as
    the primary event_id, because replays get different offsets.

    Never uses current timestamp as identity (would create new IDs on every replay).
    """
    if producer_event_id:
        return f"prod:{producer_event_id}"[:64]

    if timestamp:
        data = f"content:{city}|{pm25}|{temperature}|{timestamp}"
    else:
        data = f"content:{city}|{pm25}|{temperature}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def validate_reading(data: dict) -> tuple[bool, str | dict]:
    if not isinstance(data, dict):
        return False, "Invalid JSON: expected object"
    
    if "city" not in data or data["city"] is None:
        return False, "Missing required field: city"
    
    if not isinstance(data["city"], str) or not data["city"].strip():
        return False, "Invalid city: must be non-empty string"
    
    raw_city = data["city"].strip()
    city = CITY_ALIASES.get(raw_city, raw_city)
    is_test = bool(data.get("is_test", False)) or raw_city.startswith("SmokeTest_") or city.startswith("SmokeTest_")
    
    if "pm25" not in data or data["pm25"] is None:
        return False, "Missing required field: pm25"
    
    try:
        pm25 = float(data["pm25"])
        if pm25 < 0:
            return False, f"Invalid PM2.5 value: {pm25} (must be non-negative)"
    except (ValueError, TypeError):
        return False, f"Invalid PM2.5 value: {data.get('pm25')}"
    
    temperature = None
    if "temperature" in data and data["temperature"] is not None:
        try:
            temperature = float(data["temperature"])
            if temperature < -273.15:
                return False, f"Invalid temperature: {temperature} (below absolute zero)"
        except (ValueError, TypeError):
            return False, f"Invalid temperature value: {data.get('temperature')}"
    
    return True, {
        "city": city,
        "pm25": float(data["pm25"]),
        "temperature": temperature,
        "is_test": is_test,
    }


def process_batch(connection, readings: list) -> int:
    if not readings:
        return 0

    processed = 0
    with connection.cursor() as cursor:
        for reading in readings:
            event_id = reading["event_id"]
            city = reading["city"]
            pm25 = reading["pm25"]
            temperature = reading["temperature"]
            is_test = reading.get("is_test", False) or city.startswith("SmokeTest_")
            severity = classify_severity(pm25)
            kafka_topic = reading.get("kafka_topic")
            kafka_partition = reading.get("kafka_partition")
            kafka_offset = reading.get("kafka_offset")

            cursor.execute(
                """
                INSERT INTO pollution_readings
                    (event_id, city, pm25, temperature, status,
                     kafka_topic, kafka_partition, kafka_offset, is_test)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
                """,
                (
                    event_id, city, pm25, temperature, severity,
                    kafka_topic, kafka_partition, kafka_offset, is_test,
                ),
            )

            result = cursor.fetchone()

            if result:
                # If test data, isolate from production alert state & alerts table
                if is_test:
                    logger.debug(f"Test reading stored with is_test=True: {city}, event_id={event_id}")
                    processed += 1
                    continue

                # Alert lifecycle: query current state without mutating
                current_state = get_city_alert_state(connection, city)
                prev_severity = current_state["previous_severity"]

                alert_type, alert_message = get_alert_transition_message(
                    prev_severity, severity, pm25
                )

                notified = should_notify(
                    prev_severity,
                    severity,
                    alert_type,
                    current_state["last_notification_at"],
                    ALERT_COOLDOWN_SECONDS,
                )

                # Atomically update state exactly ONCE
                update_city_alert_state(
                    connection,
                    city,
                    severity,
                    alert_type=alert_type,
                    notified=notified,
                    pm25=pm25,
                )

                if notified:
                    cursor.execute(
                        """
                        INSERT INTO pollution_alerts
                            (city, pm25, alert_message, severity, event_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        (city, pm25, alert_message, severity, event_id),
                    )
                    logger.info(
                        f"Alert: {city} {prev_severity}→{severity} "
                        f"({alert_type}): {alert_message}"
                    )
                else:
                    logger.debug(
                        f"Alert suppressed (notified=False): {city} {severity}"
                    )

                processed += 1
                logger.debug(f"Saved: {city}, PM2.5={pm25}, status={severity}")

                # Anomaly detection: compare against city's historical baseline
                try:
                    historical = get_historical_readings_for_city(
                        connection, city, exclude_event_id=event_id
                    )
                    anomaly_result = detect_anomaly(pm25, historical)
                    if anomaly_result["is_anomaly"]:
                        logger.warning(
                            f"ANOMALY: {city} PM2.5={pm25:.1f} "
                            f"z_score={anomaly_result['z_score']:.2f} "
                            f"- {anomaly_result['reason']}"
                        )
                    persist_anomaly_result(
                        connection, city, pm25, anomaly_result, event_id=event_id
                    )
                except Exception as e:
                    logger.error(f"Anomaly detection error: {e}", exc_info=True)
            else:
                logger.debug(f"Duplicate event skipped: {event_id}")

    return processed


def create_consumer():
    return KafkaConsumer(
        "pollution-readings",
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda value: value,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="citypulse-postgres-writer",
        max_poll_records=100,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )


def wait_for_postgres(max_retries: int = 30, retry_delay: int = 2):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            logger.info("PostgreSQL connection established")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(f"PostgreSQL not ready (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
    return False


def ensure_schema(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pollution_readings (
                id BIGSERIAL PRIMARY KEY,
                event_id VARCHAR(64) UNIQUE,
                city VARCHAR(100) NOT NULL,
                pm25 DOUBLE PRECISION NOT NULL CHECK (pm25 >= 0),
                temperature DOUBLE PRECISION,
                status VARCHAR(20) NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pollution_city_time 
            ON pollution_readings (city, recorded_at DESC)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pollution_event_id 
            ON pollution_readings (event_id)
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pollution_alerts (
                id BIGSERIAL PRIMARY KEY,
                event_id VARCHAR(64) UNIQUE,
                city VARCHAR(100) NOT NULL,
                pm25 DOUBLE PRECISION NOT NULL,
                alert_message TEXT NOT NULL,
                severity VARCHAR(20) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pollution_alerts_created_at 
            ON pollution_alerts (created_at DESC)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pollution_alerts_severity 
            ON pollution_alerts (severity)
        """)
        
        cursor.execute("""
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
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_aggregates_city_window 
            ON pollution_aggregates (city, window_start DESC)
        """)
    
    connection.commit()
    logger.info("Database schema verified")


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("CityPulse Ingestion Service starting...")
    logger.info(f"Kafka servers: {KAFKA_SERVERS}")
    logger.info(f"High threshold: {HIGH_THRESHOLD}, Critical threshold: {CRITICAL_THRESHOLD}")
    
    if not wait_for_postgres():
        logger.error("Failed to connect to PostgreSQL after max retries")
        sys.exit(1)
    
    consumer = None
    connection = None
    
    while not shutdown_requested:
        try:
            if connection is None or connection.closed:
                connection = psycopg2.connect(DATABASE_URL)
                connection.autocommit = False
                ensure_schema(connection)
                logger.info("PostgreSQL connection established")
            
            if consumer is None:
                consumer = create_consumer()
                logger.info("Kafka consumer created")
            
            messages = consumer.poll(timeout_ms=1000, max_records=100)
            
            if not messages:
                continue
            
            valid_readings = []

            for records in messages.values():
                for message in records:
                    try:
                        raw_value = message.value
                        if isinstance(raw_value, bytes):
                            raw_value = raw_value.decode("utf-8")
                        try:
                            data = json.loads(raw_value)
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            logger.warning(f"Invalid JSON message skipped: {e}")
                            continue

                        # Extract producer-provided event_id if present
                        producer_event_id = data.get("event_id") if isinstance(data, dict) else None

                        is_valid, result = validate_reading(data)

                        if not is_valid:
                            logger.warning(f"Invalid message skipped: {result}")
                            continue

                        event_id = generate_event_id(
                            result["city"],
                            result["pm25"],
                            result["temperature"],
                            producer_event_id=producer_event_id,
                            kafka_topic=message.topic,
                            kafka_partition=message.partition,
                            kafka_offset=message.offset,
                        )

                        valid_readings.append({
                            "event_id": event_id,
                            "kafka_topic": message.topic,
                            "kafka_partition": message.partition,
                            "kafka_offset": message.offset,
                            **result,
                        })

                    except Exception as e:
                        logger.error(f"Error processing message: {e}", exc_info=True)
            
            if valid_readings:
                try:
                    processed = process_batch(connection, valid_readings)
                    connection.commit()
                    consumer.commit()
                    logger.info(f"Processed {processed}/{len(valid_readings)} readings")
                
                except psycopg2.OperationalError as e:
                    logger.error(f"Database connection lost: {e}")
                    connection.rollback()
                    try:
                        connection.close()
                    except:
                        pass
                    connection = None
                    time.sleep(5)
                
                except Exception as e:
                    logger.error(f"Error processing batch: {e}", exc_info=True)
                    if connection:
                        connection.rollback()
        
        except Exception as e:
            logger.error(f"Ingestion error: {e}", exc_info=True)
            if connection:
                try:
                    connection.close()
                except:
                    pass
                connection = None
            if consumer:
                try:
                    consumer.close()
                except:
                    pass
                consumer = None
            time.sleep(5)
    
    logger.info("Shutting down gracefully...")
    
    if consumer:
        try:
            consumer.close()
        except:
            pass
    
    if connection:
        try:
            connection.close()
        except:
            pass
    
    logger.info("Ingestion service stopped")


if __name__ == "__main__":
    main()
