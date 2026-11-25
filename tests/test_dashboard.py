"""
Tests for CityPulse Environmental Intelligence Dashboard.
Covers data helpers, chart generation, and page rendering via Streamlit AppTest.
"""

import os
import sys
from datetime import datetime, timezone, timedelta
import pandas as pd
import pytest

# Add dashboard to sys.path
DASHBOARD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")
if DASHBOARD_DIR not in sys.path:
    sys.path.insert(0, DASHBOARD_DIR)

from data import (
    classify_severity,
    severity_color,
    severity_icon,
    data_freshness_label,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from charts import (
    pm25_bar_chart,
    temperature_bar_chart,
    daily_trend_chart,
    hourly_trend_chart,
    pm25_timeseries_chart,
    pollution_map_figure,
)
from city_coordinates import get_city_coordinates, get_cities_with_coordinates, CITY_COORDINATES
from theme import SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_NORMAL


class TestDataHelpers:
    """Test severity classification and data formatting."""

    def test_classify_severity_normal(self):
        assert classify_severity(25.0) == "NORMAL"
        assert classify_severity(99.9) == "NORMAL"

    def test_classify_severity_high(self):
        assert classify_severity(100.0) == "HIGH"
        assert classify_severity(149.9) == "HIGH"

    def test_classify_severity_critical(self):
        assert classify_severity(150.0) == "CRITICAL"
        assert classify_severity(350.0) == "CRITICAL"

    def test_severity_colors(self):
        assert severity_color("CRITICAL") == SEVERITY_CRITICAL
        assert severity_color("HIGH") == SEVERITY_HIGH
        assert severity_color("NORMAL") == SEVERITY_NORMAL
        assert severity_color("UNKNOWN") == "#94A3B8"

    def test_severity_icons(self):
        assert ":material/" in severity_icon("CRITICAL")
        assert ":material/" in severity_icon("HIGH")
        assert ":material/" in severity_icon("NORMAL")

    def test_data_freshness_none(self):
        label, status = data_freshness_label(None)
        assert status == "offline"

    def test_data_freshness_recent(self):
        now = datetime.now(timezone.utc)
        label, status = data_freshness_label(now - timedelta(seconds=20))
        assert status == "fresh"
        assert "s ago" in label

    def test_data_freshness_stale(self):
        now = datetime.now(timezone.utc)
        label, status = data_freshness_label(now - timedelta(minutes=15))
        assert status == "stale"
        assert "m ago" in label

    def test_data_freshness_idle(self):
        now = datetime.now(timezone.utc)
        label, status = data_freshness_label(now - timedelta(hours=3))
        assert status == "idle"
        assert "3h ago" in label

    def test_city_coordinates(self):
        coords_delhi = get_city_coordinates("Delhi")
        assert coords_delhi is not None
        assert "lat" in coords_delhi and "lon" in coords_delhi

        # Bug B8 test: Bengaluru is supported and canonical
        coords_blr = get_city_coordinates("Bengaluru")
        assert coords_blr is not None
        assert abs(coords_blr["lat"] - 12.9716) < 0.001

        # Bangalore alias returns identical coordinates
        coords_bgl = get_city_coordinates("Bangalore")
        assert coords_bgl == coords_blr

        # Canonical cities list has exactly 10 cities
        canonical_list = get_cities_with_coordinates()
        assert len(canonical_list) == 10
        assert "Bengaluru" in canonical_list

        assert get_city_coordinates("NonExistentCityXYZ") is None


class TestChartHelpers:
    """Test Plotly chart generators."""

    @pytest.fixture
    def sample_readings(self):
        now = datetime.now(timezone.utc)
        return pd.DataFrame([
            {"city": "Delhi", "pm25": 165.0, "temperature": 32.5, "status": "CRITICAL", "recorded_at": now - timedelta(minutes=5)},
            {"city": "Mumbai", "pm25": 85.0, "temperature": 29.0, "status": "NORMAL", "recorded_at": now - timedelta(minutes=4)},
            {"city": "Kolkata", "pm25": 115.0, "temperature": 31.0, "status": "HIGH", "recorded_at": now - timedelta(minutes=3)},
        ])

    def test_pm25_bar_chart(self, sample_readings):
        fig = pm25_bar_chart(sample_readings)
        assert fig is not None
        assert len(fig.data) == len(sample_readings)

    def test_temperature_bar_chart(self, sample_readings):
        fig = temperature_bar_chart(sample_readings)
        assert fig is not None
        assert len(fig.data) > 0

    def test_timeseries_chart(self, sample_readings):
        fig = pm25_timeseries_chart(sample_readings)
        assert fig is not None
        assert len(fig.data) == len(sample_readings["city"].unique())

    def test_pollution_map_figure(self):
        map_df = pd.DataFrame([
            {"city": "Delhi", "lat": 28.6139, "lon": 77.2090, "pm25": 165.0, "severity": "CRITICAL", "is_anomaly": True},
            {"city": "Mumbai", "lat": 19.0760, "lon": 72.8777, "pm25": 85.0, "severity": "NORMAL", "is_anomaly": False},
        ])
        fig = pollution_map_figure(map_df)
        assert fig is not None
        # 2 severity traces (NORMAL, CRITICAL) + 1 anomaly overlay trace (B8)
        assert len(fig.data) == 3
        assert any("Anomaly Alert" in str(trace.name) for trace in fig.data)


class TestAppRendering:
    """Test Streamlit AppTest execution."""

    def test_app_entrypoint_runs(self):
        from streamlit.testing.v1 import AppTest
        app_path = os.path.join(DASHBOARD_DIR, "app.py")
        at = AppTest.from_file(app_path).run()
        assert not at.exception, f"App execution raised exception: {at.exception}"

    @pytest.mark.parametrize("page_name", [
        "dashboard.py",
        "live_monitoring.py",
        "pollution_map.py",
        "historical.py",
        "alerts_centre.py",
        "anomaly_detection.py",
        "spark_sql.py",
        "ml_lab.py",
        "graph_analytics.py",
        "kafka_pipeline.py",
        "rdd_lab.py",
        "scala_job.py",
        "system_health.py",
    ])
    def test_page_renders_without_crashing(self, page_name):
        from streamlit.testing.v1 import AppTest
        page_path = os.path.join(DASHBOARD_DIR, "app_pages", page_name)
        at = AppTest.from_file(page_path, default_timeout=15).run()
        assert not at.exception, f"Page {page_name} raised exception: {at.exception}"
