import pytest
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import time

DB_CONFIG = {
    "host": os.getenv("CITYPULSE_DB_HOST", "localhost"),
    "port": int(os.getenv("CITYPULSE_DB_PORT", "5433")),
    "dbname": "citypulse",
    "user": "citypulse",
    "password": os.getenv("CITYPULSE_DB_PASSWORD", "citypulse_dev_password")
}

HIGH_THRESHOLD = 100.0
CRITICAL_THRESHOLD = 150.0


def classify_severity(pm25: float) -> str:
    if pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


@pytest.fixture(scope="module")
def db_connection():
    max_retries = 10
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            yield conn
            conn.close()
            return
        except psycopg2.OperationalError:
            if i < max_retries - 1:
                time.sleep(2)
            else:
                pytest.skip("Database not available")


class TestDatabaseSchema:
    def test_tables_exist(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = {row[0] for row in cursor.fetchall()}
        
        assert "pollution_readings" in tables
        assert "pollution_alerts" in tables
        assert "pollution_aggregates" in tables
    
    def test_readings_table_columns(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'pollution_readings'
            """)
            columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        assert "id" in columns
        assert "city" in columns
        assert "pm25" in columns
        assert "temperature" in columns
        assert "status" in columns
        assert "recorded_at" in columns
    
    def test_alerts_table_has_severity(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'pollution_alerts' AND column_name = 'severity'
            """)
            result = cursor.fetchone()
        
        assert result is not None
    
    def test_aggregates_unique_constraint(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT conname FROM pg_constraint 
                WHERE conrelid = 'pollution_aggregates'::regclass 
                AND contype = 'u'
            """)
            constraints = [row[0] for row in cursor.fetchall()]
        
        assert any("city" in c and "window" in c for c in constraints)


class TestReadingPersistence:
    def test_insert_valid_reading(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_readings (city, pm25, temperature, status)
                VALUES ('TestCity_Insert', 75.0, 25.0, 'NORMAL')
                RETURNING id
            """)
            reading_id = cursor.fetchone()[0]
            db_connection.commit()
        
        assert reading_id is not None
        
        with db_connection.cursor() as cursor:
            cursor.execute("DELETE FROM pollution_readings WHERE id = %s", (reading_id,))
            db_connection.commit()
    
    def test_deduplication_by_event_id(self, db_connection):
        event_id = "test_dedup_12345"
        
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_readings (event_id, city, pm25, temperature, status)
                VALUES (%s, 'TestCity_Dedup', 80.0, 26.0, 'NORMAL')
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
            """, (event_id,))
            result1 = cursor.fetchone()
            db_connection.commit()
        
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_readings (event_id, city, pm25, temperature, status)
                VALUES (%s, 'TestCity_Dedup', 80.0, 26.0, 'NORMAL')
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
            """, (event_id,))
            result2 = cursor.fetchone()
            db_connection.commit()
        
        assert result1 is not None
        assert result2 is None
        
        with db_connection.cursor() as cursor:
            cursor.execute("DELETE FROM pollution_readings WHERE event_id = %s", (event_id,))
            db_connection.commit()


class TestAlertPersistence:
    def test_insert_alert_with_severity(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_alerts (city, pm25, alert_message, severity)
                VALUES ('TestCity_Alert', 175.0, 'Critical PM2.5 reading', 'CRITICAL')
                RETURNING id
            """)
            alert_id = cursor.fetchone()[0]
            db_connection.commit()
        
        assert alert_id is not None
        
        with db_connection.cursor() as cursor:
            cursor.execute("DELETE FROM pollution_alerts WHERE id = %s", (alert_id,))
            db_connection.commit()
    
    def test_alert_deduplication(self, db_connection):
        event_id = "test_alert_dedup_12345"
        
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_alerts (event_id, city, pm25, alert_message, severity)
                VALUES (%s, 'TestCity_AlertDedup', 125.0, 'High PM2.5', 'HIGH')
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
            """, (event_id,))
            result1 = cursor.fetchone()
            db_connection.commit()
        
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_alerts (event_id, city, pm25, alert_message, severity)
                VALUES (%s, 'TestCity_AlertDedup', 125.0, 'High PM2.5', 'HIGH')
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
            """, (event_id,))
            result2 = cursor.fetchone()
            db_connection.commit()
        
        assert result1 is not None
        assert result2 is None
        
        with db_connection.cursor() as cursor:
            cursor.execute("DELETE FROM pollution_alerts WHERE event_id = %s", (event_id,))
            db_connection.commit()


class TestAggregateUpsert:
    def test_aggregate_insert_and_update(self, db_connection):
        from datetime import datetime
        
        window_start = datetime(2026, 10, 19, 12, 0, 0)
        window_end = datetime(2026, 10, 19, 12, 5, 0)
        
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_aggregates 
                (city, window_start, window_end, avg_pm25, max_pm25, reading_count, status)
                VALUES ('TestCity_Agg', %s, %s, 100.0, 120.0, 5, 'HIGH')
                ON CONFLICT (city, window_start, window_end)
                DO UPDATE SET
                    avg_pm25 = EXCLUDED.avg_pm25,
                    max_pm25 = EXCLUDED.max_pm25,
                    reading_count = EXCLUDED.reading_count,
                    status = EXCLUDED.status
                RETURNING id
            """, (window_start, window_end))
            result1 = cursor.fetchone()
            db_connection.commit()
        
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pollution_aggregates 
                (city, window_start, window_end, avg_pm25, max_pm25, reading_count, status)
                VALUES ('TestCity_Agg', %s, %s, 110.0, 130.0, 8, 'HIGH')
                ON CONFLICT (city, window_start, window_end)
                DO UPDATE SET
                    avg_pm25 = EXCLUDED.avg_pm25,
                    max_pm25 = EXCLUDED.max_pm25,
                    reading_count = EXCLUDED.reading_count,
                    status = EXCLUDED.status
                RETURNING id
            """, (window_start, window_end))
            result2 = cursor.fetchone()
            db_connection.commit()
        
        assert result1 is not None
        assert result2 is not None
        assert result1[0] == result2[0]
        
        with db_connection.cursor() as cursor:
            cursor.execute("DELETE FROM pollution_aggregates WHERE city = 'TestCity_Agg'")
            db_connection.commit()


class TestQueryCorrectness:
    def test_latest_reading_per_city(self, db_connection):
        with db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT DISTINCT ON (city) city, pm25, recorded_at
                FROM pollution_readings
                ORDER BY city, recorded_at DESC
            """)
            results = cursor.fetchall()
        
        cities = [r["city"] for r in results]
        assert len(cities) == len(set(cities))
    
    def test_severity_consistency(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT pm25, severity FROM pollution_alerts
                WHERE severity IS NOT NULL
                LIMIT 20
            """)
            alerts = cursor.fetchall()
        
        for pm25, severity in alerts:
            expected = classify_severity(pm25)
            assert severity == expected, f"PM2.5={pm25} expected {expected}, got {severity}"

    def test_no_smoketest_in_live_tables(self, db_connection):
        """Bug B1: No SmokeTest records in any live production tables."""
        with db_connection.cursor() as cursor:
            for table in ["pollution_readings", "pollution_alerts", "pollution_aggregates", "pollution_anomalies", "city_alert_state"]:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE city LIKE 'SmokeTest%'")
                count = cursor.fetchone()[0]
                assert count == 0, f"Found {count} SmokeTest records in {table}"

    def test_bangalore_unified_to_bengaluru(self, db_connection):
        """Bug B2: Bangalore and Bengaluru unified under Bengaluru."""
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pollution_readings WHERE city = 'Bangalore'")
            assert cursor.fetchone()[0] == 0

            cursor.execute("SELECT COUNT(*) FROM city_alert_state WHERE city = 'Bangalore'")
            assert cursor.fetchone()[0] == 0

            cursor.execute("SELECT COUNT(*) FROM city_alert_state WHERE city = 'Bengaluru'")
            assert cursor.fetchone()[0] == 1

    def test_city_latest_view_invariants(self, db_connection):
        """Bug B3: NORMAL + HIGH + CRITICAL = total_monitored_cities (exactly 10 canonical cities)."""
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE current_severity = 'NORMAL') as normal,
                    COUNT(*) FILTER (WHERE current_severity = 'HIGH') as high,
                    COUNT(*) FILTER (WHERE current_severity = 'CRITICAL') as critical
                FROM city_latest
            """)
            total, normal, high, critical = cursor.fetchone()
            assert total == 10
            assert normal + high + critical == total

    def test_no_repeat_normal_alerts(self, db_connection):
        """Bug B5: No repeat normal readings stored as alerts."""
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM pollution_alerts
                WHERE severity = 'NORMAL' AND alert_message LIKE 'Normal PM2.5 reading%'
            """)
            count = cursor.fetchone()[0]
            assert count == 0, f"Found {count} bogus repeat normal alerts"
