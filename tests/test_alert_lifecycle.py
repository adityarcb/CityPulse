"""
Tests for alert lifecycle and event identity/deduplication.
"""

import pytest
import hashlib
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))

from consumer import (
    classify_severity,
    generate_event_id,
    get_alert_transition_message,
    should_notify,
    update_city_alert_state,
)
from anomaly_detector import detect_anomaly


HIGH_THRESHOLD = 100.0
CRITICAL_THRESHOLD = 150.0


class TestEventIdentity:
    """Tests for stable event identity and deduplication."""

    def test_producer_event_id_priority(self):
        """Producer-provided event_id takes priority."""
        eid = generate_event_id("Delhi", 100.0, 25.0, producer_event_id="my-unique-id-123")
        assert eid == "prod:my-unique-id-123"

    def test_producer_event_id_truncated(self):
        """Long producer event_ids are truncated to 64 chars."""
        long_id = "a" * 100
        eid = generate_event_id("Delhi", 100.0, 25.0, producer_event_id=long_id)
        assert len(eid) == 64
        assert eid == "prod:" + "a" * 59

    def test_content_based_dedup_same_message(self):
        """Same content produces same event_id (replay deduplication)."""
        eid1 = generate_event_id("Delhi", 100.0, 25.0)
        eid2 = generate_event_id("Delhi", 100.0, 25.0)
        assert eid1 == eid2

    def test_content_based_dedup_with_timestamp(self):
        """Same content with same timestamp produces same event_id."""
        ts = "2026-01-01T12:00:00+00:00"
        eid1 = generate_event_id("Delhi", 100.0, 25.0, timestamp=ts)
        eid2 = generate_event_id("Delhi", 100.0, 25.0, timestamp=ts)
        assert eid1 == eid2

    def test_different_content_different_id(self):
        """Different readings get different event_ids."""
        eid1 = generate_event_id("Delhi", 100.0, 25.0)
        eid2 = generate_event_id("Delhi", 150.0, 25.0)
        assert eid1 != eid2

    def test_different_city_different_id(self):
        """Different cities get different event_ids."""
        eid1 = generate_event_id("Delhi", 100.0, 25.0)
        eid2 = generate_event_id("Mumbai", 100.0, 25.0)
        assert eid1 != eid2

    def test_legacy_message_backward_compatible(self):
        """Legacy messages without event_id still get stable IDs."""
        eid = generate_event_id("Amritsar", 175.0, 36.0)
        assert len(eid) == 16
        # Same call produces same ID
        eid2 = generate_event_id("Amritsar", 175.0, 36.0)
        assert eid == eid2

    def test_no_current_timestamp_as_identity(self):
        """Event IDs don't use current time, so replays produce same ID."""
        eid1 = generate_event_id("Delhi", 100.0, 25.0)
        # Simulate time passing - should still produce same ID
        eid2 = generate_event_id("Delhi", 100.0, 25.0)
        assert eid1 == eid2


class TestAlertLifecycle:
    """Tests for alert state transitions and lifecycle."""

    def test_normal_to_high_initial_alert(self):
        alert_type, msg = get_alert_transition_message("NORMAL", "HIGH", 120.0)
        assert alert_type == "initial"
        assert "High PM2.5 alert" in msg
        assert "120" in msg

    def test_normal_to_critical_initial_alert(self):
        alert_type, msg = get_alert_transition_message("NORMAL", "CRITICAL", 175.0)
        assert alert_type == "initial"
        assert "Critical PM2.5 alert" in msg
        assert "175" in msg

    def test_high_to_critical_escalation(self):
        alert_type, msg = get_alert_transition_message("HIGH", "CRITICAL", 160.0)
        assert alert_type == "escalation"
        assert "escalated to CRITICAL" in msg
        assert "160" in msg

    def test_critical_to_high_deescalation(self):
        alert_type, msg = get_alert_transition_message("CRITICAL", "HIGH", 120.0)
        assert alert_type == "de-escalation"
        assert "de-escalated to HIGH" in msg

    def test_high_to_normal_recovery(self):
        alert_type, msg = get_alert_transition_message("HIGH", "NORMAL", 80.0)
        assert alert_type == "recovery"
        assert "recovered to NORMAL" in msg

    def test_critical_to_normal_recovery(self):
        alert_type, msg = get_alert_transition_message("CRITICAL", "NORMAL", 50.0)
        assert alert_type == "recovery"
        assert "recovered to NORMAL" in msg

    def test_high_repeat_same_severity(self):
        alert_type, msg = get_alert_transition_message("HIGH", "HIGH", 120.0)
        assert alert_type == "repeat"

    def test_critical_repeat_same_severity(self):
        alert_type, msg = get_alert_transition_message("CRITICAL", "CRITICAL", 175.0)
        assert alert_type == "repeat"

    def test_normal_repeat(self):
        alert_type, msg = get_alert_transition_message("NORMAL", "NORMAL", 50.0)
        assert alert_type == "repeat"


class TestShouldNotify:
    """Tests for cooldown-based notification logic."""

    def test_initial_always_notifies(self):
        assert should_notify("NORMAL", "HIGH", "initial", None, 300) is True

    def test_escalation_always_notifies(self):
        assert should_notify("HIGH", "CRITICAL", "escalation", None, 300) is True

    def test_deescalation_always_notifies(self):
        assert should_notify("CRITICAL", "HIGH", "de-escalation", None, 300) is True

    def test_recovery_always_notifies(self):
        assert should_notify("HIGH", "NORMAL", "recovery", None, 300) is True

    def test_repeat_first_time_notifies(self):
        assert should_notify("HIGH", "HIGH", "repeat", None, 300) is True

    def test_repeat_within_cooldown_suppressed(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        # within cooldown (300s = 5 min)
        assert should_notify("HIGH", "HIGH", "repeat", now.isoformat(), 300) is False

    def test_repeat_after_cooldown_notifies(self):
        from datetime import datetime, timezone, timedelta
        past = datetime.now(timezone.utc) - timedelta(minutes=10)
        # 10 minutes > 5 minute cooldown
        assert should_notify("HIGH", "HIGH", "repeat", past.isoformat(), 300) is True

    def test_normal_repeat_never_notifies(self):
        """Bug B5 fix: repeat normal readings must NEVER trigger alerts."""
        from datetime import datetime, timezone, timedelta
        past = datetime.now(timezone.utc) - timedelta(days=5)
        # Even with no prior notification or expired cooldown, NORMAL->NORMAL is suppressed
        assert should_notify("NORMAL", "NORMAL", "repeat", None, 300) is False
        assert should_notify("NORMAL", "NORMAL", "repeat", past.isoformat(), 300) is False


class TestCanonicalCitiesAndAllowlist:
    """Tests for Bug B1 & B2 canonicalization and allowlists."""

    def test_bangalore_normalized_to_bengaluru(self):
        from consumer import validate_reading, CITY_ALIASES
        assert CITY_ALIASES.get("Bangalore") == "Bengaluru"
        ok, res = validate_reading({"city": "Bangalore", "pm25": 42.5})
        assert ok is True
        assert res["city"] == "Bengaluru"

    def test_smoke_test_flagged_as_test(self):
        from consumer import validate_reading
        ok, res = validate_reading({"city": "SmokeTest_Debug", "pm25": 140.0})
        assert ok is True
        assert res["is_test"] is True

    def test_valid_city_allowlist(self):
        from consumer import VALID_CITIES
        assert len(VALID_CITIES) == 10
        assert "Bengaluru" in VALID_CITIES
        assert "Bangalore" not in VALID_CITIES
        assert "Delhi" in VALID_CITIES
        assert "Mumbai" in VALID_CITIES


class TestAnomalyIntegration:
    """Tests for anomaly detection integration with alert lifecycle."""

    def test_anomaly_distinct_from_severity(self):
        """An anomaly is not the same as a HIGH/CRITICAL severity."""
        # Normal reading with high z-score (anomaly but not severity alert)
        result = detect_anomaly(50.0, [10.0, 12.0, 11.0, 13.0, 9.0],
                                z_threshold=3.0, min_history=5)
        severity = classify_severity(50.0)
        assert severity == "NORMAL"
        # 50.0 is very anomalous compared to baseline of ~11
        assert result["is_anomaly"] is True

    def test_high_reading_not_necessarily_anomaly(self):
        """A HIGH severity reading may not be an anomaly if consistent with baseline."""
        result = detect_anomaly(120.0, [110.0, 115.0, 125.0, 118.0, 122.0],
                                z_threshold=3.0, min_history=5)
        severity = classify_severity(120.0)
        assert severity == "HIGH"
        # 120 is consistent with baseline of ~118, so not anomalous
        assert result["is_anomaly"] is False

    def test_severity_and_anomaly_both(self):
        """A reading can be both CRITICAL severity AND an anomaly."""
        result = detect_anomaly(200.0, [50.0, 55.0, 48.0, 52.0, 51.0],
                                z_threshold=3.0, min_history=5)
        severity = classify_severity(200.0)
        assert severity == "CRITICAL"
        assert result["is_anomaly"] is True  # 200 is way above ~51 baseline
