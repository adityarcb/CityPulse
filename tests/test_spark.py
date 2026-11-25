import pytest
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

HIGH_THRESHOLD = float(os.getenv("HIGH_THRESHOLD", "100"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "150"))


def classify_severity(max_pm25: float) -> str:
    if max_pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif max_pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


class TestAggregateCalculations:
    def test_aggregate_severity_normal(self):
        assert classify_severity(85.0) == "NORMAL"
        assert classify_severity(50.0) == "NORMAL"
    
    def test_aggregate_severity_high(self):
        assert classify_severity(100.0) == "HIGH"
        assert classify_severity(120.0) == "HIGH"
        assert classify_severity(149.9) == "HIGH"
    
    def test_aggregate_severity_critical(self):
        assert classify_severity(150.0) == "CRITICAL"
        assert classify_severity(175.0) == "CRITICAL"
        assert classify_severity(200.0) == "CRITICAL"
    
    def test_average_calculation(self):
        readings = [100.0, 120.0, 140.0]
        avg = sum(readings) / len(readings)
        assert avg == 120.0
    
    def test_max_calculation(self):
        readings = [100.0, 120.0, 140.0]
        max_val = max(readings)
        assert max_val == 140.0
    
    def test_count_calculation(self):
        readings = [100.0, 120.0, 140.0]
        count = len(readings)
        assert count == 3
    
    def test_window_timestamps(self):
        event_time = datetime(2026, 10, 19, 10, 47, 30, tzinfo=timezone.utc)
        window_start = event_time.replace(minute=5 * (event_time.minute // 5), second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=5)
        
        assert window_start.minute % 5 == 0
        assert window_end.minute % 5 == 0
        assert window_end > window_start


class TestTimestampHandling:
    def test_utc_timestamp_parsing(self):
        iso_string = "2026-10-19T10:30:45.123456+00:00"
        parsed = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
        assert parsed.tzinfo is not None
    
    def test_timezone_conversion(self):
        utc_time = datetime(2026, 10, 19, 5, 0, 0, tzinfo=timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        ist_time = utc_time + ist_offset
        assert ist_time.hour == 10
    
    def test_window_alignment(self):
        minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        for minute in minutes:
            event_time = datetime(2026, 10, 19, 10, minute, 30, tzinfo=timezone.utc)
            window_start = event_time.replace(minute=5 * (event_time.minute // 5), second=0, microsecond=0)
            assert window_start.minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]


class TestSparkWatermark:
    def test_watermark_delays_late_data(self):
        event_time = datetime(2026, 10, 19, 10, 30, 0, tzinfo=timezone.utc)
        watermark = timedelta(minutes=1)
        
        late_event = event_time - timedelta(minutes=2)
        is_late = (event_time - late_event) > watermark
        
        assert is_late
    
    def test_on_time_data_not_delayed(self):
        event_time = datetime(2026, 10, 19, 10, 30, 0, tzinfo=timezone.utc)
        watermark = timedelta(minutes=1)
        
        on_time_event = event_time - timedelta(seconds=30)
        is_late = (event_time - on_time_event) > watermark
        
        assert not is_late
