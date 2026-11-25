"""
Pollution anomaly detection module.

Uses a rolling baseline (mean + standard deviation) per city to flag
readings that are statistically unusual relative to the city's own
history. Distinct from fixed HIGH/CRITICAL severity thresholds.

Anomaly ≠ public health emergency. It is an analytical signal that
may require further review.
"""

import logging
import math
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

ANOMALY_Z_THRESHOLD = float(os.getenv("ANOMALY_Z_THRESHOLD", "3.0"))
ANOMALY_MIN_HISTORY = int(os.getenv("ANOMALY_MIN_HISTORY", "5"))


def compute_baseline(readings: list[float]) -> dict:
    """
    Compute baseline statistics from a list of historical readings.

    Returns dict with mean, stddev, sample_count.
    Handles empty lists and zero variance gracefully.
    """
    n = len(readings)
    if n == 0:
        return {"mean": 0.0, "stddev": 0.0, "sample_count": 0}

    mean = sum(readings) / n

    if n < 2:
        return {"mean": mean, "stddev": 0.0, "sample_count": n}

    variance = sum((x - mean) ** 2 for x in readings) / (n - 1)
    stddev = math.sqrt(variance)

    return {"mean": mean, "stddev": stddev, "sample_count": n}


def detect_anomaly(
    current_pm25: float,
    historical_readings: list[float],
    z_threshold: float = None,
    min_history: int = None,
) -> dict:
    """
    Determine if a reading is a statistical anomaly relative to city history.

    Uses z-score: |current - baseline_mean| / baseline_stddev

    Returns dict with:
        - is_anomaly: bool
        - z_score: float or None
        - baseline_mean: float
        - baseline_stddev: float
        - baseline_sample_count: int
        - reason: str (explanation)
    """
    if z_threshold is None:
        z_threshold = ANOMALY_Z_THRESHOLD
    if min_history is None:
        min_history = ANOMALY_MIN_HISTORY

    result = {
        "is_anomaly": False,
        "status": "NORMAL",
        "z_score": 0.0,
        "baseline_mean": 0.0,
        "baseline_stddev": 0.0,
        "baseline_sample_count": 0,
        "reason": "",
    }

    # Not enough history to establish a baseline
    if len(historical_readings) < min_history:
        result["status"] = "INSUFFICIENT_DATA"
        result["reason"] = (
            f"Insufficient history: {len(historical_readings)} readings "
            f"(need ≥{min_history}) to establish baseline"
        )
        return result

    baseline = compute_baseline(historical_readings)
    result["baseline_mean"] = baseline["mean"]
    result["baseline_stddev"] = baseline["stddev"]
    result["baseline_sample_count"] = baseline["sample_count"]

    # Zero variance: all historical readings are identical
    if baseline["stddev"] == 0.0:
        if current_pm25 != baseline["mean"]:
            result["is_anomaly"] = True
            result["status"] = "ANOMALY"
            result["z_score"] = 999.0  # sentinel for zero-variance anomaly
            result["reason"] = (
                f"Zero-variance baseline (all {baseline['sample_count']} "
                f"historical readings = {baseline['mean']:.1f}), "
                f"current reading {current_pm25:.1f} differs"
            )
        else:
            result["is_anomaly"] = False
            result["status"] = "NORMAL"
            result["z_score"] = 0.0
            result["reason"] = (
                f"Zero-variance baseline, current reading matches "
                f"historical value {baseline['mean']:.1f}"
            )
        return result

    # Compute z-score
    z_score = abs(current_pm25 - baseline["mean"]) / baseline["stddev"]
    result["z_score"] = round(z_score, 4)

    if z_score >= z_threshold:
        result["is_anomaly"] = True
        result["status"] = "ANOMALY"
        direction = "above" if current_pm25 > baseline["mean"] else "below"
        result["reason"] = (
            f"PM2.5 {current_pm25:.1f} is {z_score:.2f}σ {direction} "
            f"city baseline mean {baseline['mean']:.1f} "
            f"(σ={baseline['stddev']:.2f}, n={baseline['sample_count']})"
        )
    else:
        result["is_anomaly"] = False
        result["status"] = "NORMAL"
        result["reason"] = (
            f"PM2.5 {current_pm25:.1f} is {z_score:.2f}σ from city "
            f"baseline mean {baseline['mean']:.1f} "
            f"(threshold={z_threshold}, not anomalous)"
        )

    return result


def get_historical_readings_for_city(
    connection, city: str, lookback_hours: int = 168, limit: int = 100, exclude_event_id: str = None
) -> list[float]:
    """
    Retrieve recent historical PM2.5 readings for a canonical city from the database.
    Excludes test readings (is_test=FALSE) and excludes the current reading being evaluated.
    """
    with connection.cursor() as cursor:
        if exclude_event_id:
            cursor.execute(
                """
                SELECT pm25 FROM pollution_readings
                WHERE city = %s
                  AND is_test = FALSE
                  AND event_id != %s
                  AND recorded_at >= NOW() - INTERVAL %s
                ORDER BY recorded_at DESC
                LIMIT %s
                """,
                (city, exclude_event_id, f"{lookback_hours} hours", limit),
            )
        else:
            cursor.execute(
                """
                SELECT pm25 FROM pollution_readings
                WHERE city = %s
                  AND is_test = FALSE
                  AND recorded_at >= NOW() - INTERVAL %s
                ORDER BY recorded_at DESC
                LIMIT %s
                """,
                (city, f"{lookback_hours} hours", limit),
            )
        rows = cursor.fetchall()
        return [float(row[0]) for row in rows]


def persist_anomaly_result(
    connection,
    city: str,
    pm25: float,
    anomaly_result: dict,
    event_id: str = None,
    recorded_at: datetime = None,
) -> bool:
    """
    Persist anomaly detection result to the database.

    Returns True if inserted, False if duplicate (event_id conflict).
    """
    if recorded_at is None:
        recorded_at = datetime.now(timezone.utc)

    with connection.cursor() as cursor:
        try:
            cursor.execute(
                """
                INSERT INTO pollution_anomalies
                    (city, pm25, baseline_mean, baseline_stddev,
                     baseline_sample_count, z_score, is_anomaly,
                     anomaly_threshold, event_id, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
                """,
                (
                    city,
                    pm25,
                    anomaly_result["baseline_mean"],
                    anomaly_result["baseline_stddev"],
                    anomaly_result["baseline_sample_count"],
                    anomaly_result["z_score"],
                    anomaly_result["is_anomaly"],
                    ANOMALY_Z_THRESHOLD,
                    event_id,
                    recorded_at,
                ),
            )
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Error persisting anomaly result: {e}")
            return False