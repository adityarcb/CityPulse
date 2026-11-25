import pytest
import json
import hashlib
from datetime import datetime, timezone


HIGH_THRESHOLD = 100.0
CRITICAL_THRESHOLD = 150.0


def classify_severity(pm25: float) -> str:
    if pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


def validate_reading(data: dict) -> tuple:
    if not isinstance(data, dict):
        return False, "Invalid JSON: expected object"
    
    if "city" not in data or data["city"] is None:
        return False, "Missing required field: city"
    
    if not isinstance(data["city"], str) or not data["city"].strip():
        return False, "Invalid city: must be non-empty string"
    
    city = data["city"].strip()
    
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
        "pm25": pm25,
        "temperature": temperature
    }


def generate_event_id(city: str, pm25: float, temperature: float, timestamp: str = None) -> str:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    data = f"{city}|{pm25}|{temperature}|{timestamp}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


class TestSeverityClassification:
    def test_normal_below_high_threshold(self):
        assert classify_severity(50.0) == "NORMAL"
        assert classify_severity(0.0) == "NORMAL"
        assert classify_severity(99.9) == "NORMAL"
    
    def test_high_at_threshold(self):
        assert classify_severity(100.0) == "HIGH"
        assert classify_severity(125.0) == "HIGH"
        assert classify_severity(149.9) == "HIGH"
    
    def test_critical_at_threshold(self):
        assert classify_severity(150.0) == "CRITICAL"
        assert classify_severity(175.0) == "CRITICAL"
        assert classify_severity(200.0) == "CRITICAL"
    
    def test_boundary_values(self):
        assert classify_severity(99.9) == "NORMAL"
        assert classify_severity(100.0) == "HIGH"
        assert classify_severity(149.9) == "HIGH"
        assert classify_severity(150.0) == "CRITICAL"


class TestReadingValidation:
    def test_valid_reading(self):
        data = {"city": "Delhi", "pm25": 120.5, "temperature": 32.0}
        is_valid, result = validate_reading(data)
        assert is_valid
        assert result["city"] == "Delhi"
        assert result["pm25"] == 120.5
        assert result["temperature"] == 32.0
    
    def test_valid_reading_without_temperature(self):
        data = {"city": "Mumbai", "pm25": 85.0}
        is_valid, result = validate_reading(data)
        assert is_valid
        assert result["temperature"] is None
    
    def test_missing_city(self):
        data = {"pm25": 100.0, "temperature": 25.0}
        is_valid, result = validate_reading(data)
        assert not is_valid
        assert "city" in result.lower()
    
    def test_missing_pm25(self):
        data = {"city": "Chennai", "temperature": 28.0}
        is_valid, result = validate_reading(data)
        assert not is_valid
        assert "pm25" in result.lower()
    
    def test_null_city(self):
        data = {"city": None, "pm25": 50.0}
        is_valid, result = validate_reading(data)
        assert not is_valid
    
    def test_empty_city(self):
        data = {"city": "", "pm25": 50.0}
        is_valid, result = validate_reading(data)
        assert not is_valid
    
    def test_negative_pm25(self):
        data = {"city": "Kolkata", "pm25": -10.0}
        is_valid, result = validate_reading(data)
        assert not is_valid
        assert "non-negative" in result.lower()
    
    def test_invalid_pm25_string(self):
        data = {"city": "Bangalore", "pm25": "invalid"}
        is_valid, result = validate_reading(data)
        assert not is_valid
    
    def test_invalid_temperature(self):
        data = {"city": "Hyderabad", "pm25": 75.0, "temperature": "hot"}
        is_valid, result = validate_reading(data)
        assert not is_valid
    
    def test_temperature_below_absolute_zero(self):
        data = {"city": "Pune", "pm25": 60.0, "temperature": -300.0}
        is_valid, result = validate_reading(data)
        assert not is_valid
    
    def test_not_a_dict(self):
        is_valid, result = validate_reading("not a dict")
        assert not is_valid
        assert "object" in result.lower()
    
    def test_city_whitespace_stripped(self):
        data = {"city": "  Delhi  ", "pm25": 100.0}
        is_valid, result = validate_reading(data)
        assert is_valid
        assert result["city"] == "Delhi"


class TestEventIdGeneration:
    def test_event_id_is_consistent(self):
        timestamp = "2026-01-01T12:00:00+00:00"
        id1 = generate_event_id("Delhi", 100.0, 25.0, timestamp)
        id2 = generate_event_id("Delhi", 100.0, 25.0, timestamp)
        assert id1 == id2
    
    def test_event_id_different_for_different_cities(self):
        timestamp = "2026-01-01T12:00:00+00:00"
        id1 = generate_event_id("Delhi", 100.0, 25.0, timestamp)
        id2 = generate_event_id("Mumbai", 100.0, 25.0, timestamp)
        assert id1 != id2
    
    def test_event_id_different_for_different_pm25(self):
        timestamp = "2026-01-01T12:00:00+00:00"
        id1 = generate_event_id("Delhi", 100.0, 25.0, timestamp)
        id2 = generate_event_id("Delhi", 150.0, 25.0, timestamp)
        assert id1 != id2
    
    def test_event_id_is_16_chars(self):
        event_id = generate_event_id("Delhi", 100.0, 25.0)
        assert len(event_id) == 16


class TestAlertMessageGeneration:
    def get_alert_message(self, pm25: float, severity: str) -> str:
        if severity == "CRITICAL":
            return f"Critical PM2.5 reading: {pm25:.1f} µg/m³ (≥{CRITICAL_THRESHOLD:.0f})"
        elif severity == "HIGH":
            return f"High PM2.5 reading: {pm25:.1f} µg/m³ (≥{HIGH_THRESHOLD:.0f})"
        return f"Normal PM2.5 reading: {pm25:.1f} µg/m³"
    
    def test_critical_message(self):
        msg = self.get_alert_message(175.0, "CRITICAL")
        assert "Critical" in msg
        assert "175" in msg
        assert "150" in msg
    
    def test_high_message(self):
        msg = self.get_alert_message(125.0, "HIGH")
        assert "High" in msg
        assert "125" in msg
        assert "100" in msg
    
    def test_normal_message(self):
        msg = self.get_alert_message(50.0, "NORMAL")
        assert "Normal" in msg
        assert "50" in msg
