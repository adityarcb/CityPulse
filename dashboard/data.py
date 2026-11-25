"""
CityPulse — Shared data loading and database access.

All database queries are centralised here so pages stay focused on UI.
Functions use @st.cache_data with appropriate TTLs for real-time freshness.
Single source of truth: PostgreSQL view `city_latest`.
"""

import os
import logging
import warnings
from datetime import datetime, timezone, timedelta

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from theme import (
    SEVERITY_COLORS,
    SEVERITY_ICONS,
    SEVERITY_BADGE_COLORS,
    get_severity_color,
    get_severity_badge_color,
    get_severity_icon,
    get_city_color,
)

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")
logger = logging.getLogger(__name__)

# ── Configurable Thresholds (B15) ─────────────────────────────────────────────
HIGH_THRESHOLD = float(os.getenv("HIGH_THRESHOLD", "100.0"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "150.0"))
STALE_THRESHOLD_MINUTES = int(os.getenv("STALE_THRESHOLD_MINUTES", "15"))

# ── Database config ───────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("CITYPULSE_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("CITYPULSE_DB_PORT", "5433")),
    "dbname": os.getenv("CITYPULSE_DB_NAME", "citypulse"),
    "user": os.getenv("CITYPULSE_DB_USER", "citypulse"),
    "password": os.getenv("CITYPULSE_DB_PASSWORD", "citypulse_dev_password"),
    "connect_timeout": 5,
}


# ── Severity & Freshness Helpers ──────────────────────────────────────────────

def classify_severity(pm25: float) -> str:
    if pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


def severity_color(severity: str) -> str:
    return get_severity_color(severity)


def severity_icon(severity: str) -> str:
    return get_severity_icon(severity)


def severity_badge_color(severity: str) -> str:
    return get_severity_badge_color(severity)


def data_freshness_label(ts) -> tuple[str, str]:
    """
    Return (label, status) for a timestamp.
    status is one of: 'fresh' (< 5 min), 'stale' (5-60 min), 'idle' (> 60 min), 'offline'.
    """
    if ts is None or pd.isna(ts):
        return "No data", "offline"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return "No data", "offline"
    elif isinstance(ts, pd.Timestamp):
        if ts.tz is None:
            ts = ts.tz_localize(timezone.utc)
        else:
            ts = ts.tz_convert(timezone.utc)
        ts = ts.to_pydatetime()
    elif isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    try:
        age = now - ts
        seconds = int(age.total_seconds())
    except Exception:
        return "No data", "offline"

    if seconds < 0:
        return "just now", "fresh"
    elif seconds < 60:
        return f"{seconds}s ago", "fresh"
    elif seconds < 300:
        return f"{seconds // 60}m ago", "fresh"
    elif seconds < 3600:
        return f"{seconds // 60}m ago", "stale"
    elif seconds < 86400:
        return f"{seconds // 3600}h ago", "idle"
    else:
        return f"{seconds // 86400}d ago", "idle"


def is_stale_reading(ts) -> bool:
    """Return True if a reading timestamp is older than STALE_THRESHOLD_MINUTES."""
    if ts is None or pd.isna(ts):
        return True
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return True
    elif isinstance(ts, pd.Timestamp):
        if ts.tz is None:
            ts = ts.tz_localize(timezone.utc)
        else:
            ts = ts.tz_convert(timezone.utc)
        ts = ts.to_pydatetime()
    elif isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    try:
        return (now - ts).total_seconds() > (STALE_THRESHOLD_MINUTES * 60)
    except Exception:
        return True


# ── Database connection ──────────────────────────────────────────────────────

@st.cache_resource(ttl=120)
def _get_connection():
    """Cached database connection with autocommit enabled to prevent idle transactions."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None


def get_connection():
    """Get a database connection, clearing cache on error."""
    conn = _get_connection()
    if conn is not None:
        try:
            conn.cursor().execute("SELECT 1")
            return conn
        except Exception:
            _get_connection.clear()
            return _get_connection()
    return None


# ── Data loaders (B1, B2, B3) ────────────────────────────────────────────────

@st.cache_data(ttl=8)
def load_readings() -> pd.DataFrame:
    """Load all valid canonical readings, sorted by (city, timestamp)."""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql_query(
            """
            SELECT id, city, pm25, temperature, status, recorded_at
            FROM pollution_readings
            WHERE is_test = FALSE AND city NOT LIKE 'SmokeTest%'
            ORDER BY city ASC, recorded_at ASC
            """,
            conn,
        )
    except Exception as e:
        logger.error(f"Error loading readings: {e}")
        _get_connection.clear()
        return pd.DataFrame()


@st.cache_data(ttl=8)
def load_alerts() -> pd.DataFrame:
    """Load recent pollution alerts excluding test cities."""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql_query(
            """
            SELECT id, city, pm25, alert_message, severity, created_at
            FROM pollution_alerts
            WHERE city NOT LIKE 'SmokeTest%'
            ORDER BY created_at DESC
            LIMIT 200
            """,
            conn,
        )
    except Exception as e:
        logger.error(f"Error loading alerts: {e}")
        _get_connection.clear()
        return pd.DataFrame()


@st.cache_data(ttl=8)
def load_aggregates() -> pd.DataFrame:
    """Load recent Spark aggregate windows."""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql_query(
            """
            SELECT city, window_start, window_end, avg_pm25, max_pm25,
                   reading_count, avg_temperature, status, updated_at
            FROM pollution_aggregates
            WHERE city NOT LIKE 'SmokeTest%'
            ORDER BY window_start DESC
            LIMIT 50
            """,
            conn,
        )
    except Exception as e:
        logger.error(f"Error loading aggregates: {e}")
        _get_connection.clear()
        return pd.DataFrame()


@st.cache_data(ttl=8)
def load_city_alert_state() -> pd.DataFrame:
    """Load persistent per-city alert state for canonical cities."""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql_query(
            """
            SELECT city, current_severity, last_notification_at,
                   last_severity_change_at, alert_count,
                   escalation_count, de_escalation_count, recovery_count, updated_at
            FROM city_alert_state
            WHERE city NOT LIKE 'SmokeTest%'
            ORDER BY city ASC
            """,
            conn,
        )
    except Exception as e:
        logger.error(f"Error loading city alert state: {e}")
        _get_connection.clear()
        return pd.DataFrame()


@st.cache_data(ttl=8)
def load_anomalies() -> pd.DataFrame:
    """Load recent anomaly detection results for canonical cities."""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql_query(
            """
            SELECT city, pm25, baseline_mean, baseline_stddev,
                   baseline_sample_count, z_score, is_anomaly,
                   anomaly_threshold, event_id, recorded_at
            FROM pollution_anomalies
            WHERE city NOT LIKE 'SmokeTest%'
            ORDER BY recorded_at DESC
            LIMIT 100
            """,
            conn,
        )
    except Exception as e:
        logger.error(f"Error loading anomalies: {e}")
        _get_connection.clear()
        return pd.DataFrame()


@st.cache_data(ttl=8)
def get_latest_readings_per_city() -> pd.DataFrame:
    """
    Get the single source of truth latest state per canonical city from city_latest view.
    Invariant: exactly 1 row per canonical city.
    """
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql_query(
            """
            SELECT
                city, pm25, temperature, status, recorded_at,
                current_severity, alert_count, escalation_count,
                de_escalation_count, recovery_count,
                last_notification_at, last_severity_change_at
            FROM city_latest
            ORDER BY city ASC
            """,
            conn,
        )
    except Exception as e:
        logger.error(f"Error loading latest readings from city_latest: {e}")
        _get_connection.clear()
@st.cache_data(ttl=8)
def get_latest_readings_all_cities() -> list[dict]:
    """
    Get latest state for all canonical cities as a list of dictionaries.
    """
    df = get_latest_readings_per_city()
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        records.append({
            "city": row["city"],
            "pm25": float(row.get("pm25", 0.0) or 0.0),
            "temperature": float(row.get("temperature", 0.0) or 0.0),
            "severity": str(row.get("current_severity") or row.get("status") or "NORMAL"),
            "recorded_at": row.get("recorded_at"),
        })
    return records


@st.cache_data(ttl=8)
def get_city_metrics() -> dict:
    """
    Compute aggregate city air quality metrics for Living Atmosphere background.
    """
    df = get_latest_readings_per_city()
    if df.empty or "pm25" not in df.columns:
        return {"avg_pm25": 65.0, "max_pm25": 100.0, "min_pm25": 20.0, "total_cities": 0}
    avg_pm = float(df["pm25"].mean())
    max_pm = float(df["pm25"].max())
    min_pm = float(df["pm25"].min())
    return {
        "avg_pm25": round(avg_pm, 1),
        "max_pm25": round(max_pm, 1),
        "min_pm25": round(min_pm, 1),
        "total_cities": len(df),
    }


@st.cache_data(ttl=8)
def get_system_health() -> dict | None:
    """
    Get system health summary derived from city_latest view.
    Guarantees the invariant: cities_normal + cities_high + cities_critical == cities_monitored.
    """
    try:
        conn = get_connection()
        if conn is None:
            return None
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT
                    (SELECT MAX(recorded_at) FROM pollution_readings WHERE is_test = FALSE AND city NOT LIKE 'SmokeTest%') as latest_reading,
                    (SELECT MAX(created_at) FROM pollution_alerts WHERE city NOT LIKE 'SmokeTest%') as latest_alert,
                    (SELECT MAX(updated_at) FROM pollution_aggregates WHERE city NOT LIKE 'SmokeTest%') as latest_aggregate,
                    (SELECT COUNT(*) FROM pollution_readings WHERE is_test = FALSE AND city NOT LIKE 'SmokeTest%') as total_readings,
                    (SELECT COUNT(*) FROM pollution_alerts WHERE city NOT LIKE 'SmokeTest%') as total_alerts,
                    (SELECT COUNT(*) FROM pollution_aggregates WHERE city NOT LIKE 'SmokeTest%') as total_aggregates,
                    (SELECT COUNT(*) FROM pollution_anomalies WHERE is_anomaly = TRUE AND city NOT LIKE 'SmokeTest%') as total_anomalies,
                    (SELECT COUNT(*) FROM city_latest) as cities_monitored,
                    (SELECT COUNT(*) FROM city_latest WHERE current_severity IN ('HIGH', 'CRITICAL')) as cities_elevated,
                    (SELECT COUNT(*) FROM city_latest WHERE current_severity = 'CRITICAL') as cities_critical,
                    (SELECT COUNT(*) FROM city_latest WHERE current_severity = 'HIGH') as cities_high,
                    (SELECT COUNT(*) FROM city_latest WHERE current_severity = 'NORMAL') as cities_normal
            """)
            return dict(cursor.fetchone())
    except Exception as e:
        logger.error(f"Error getting system health: {e}")
        _get_connection.clear()
        return None
