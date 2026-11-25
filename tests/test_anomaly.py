"""
Tests for anomaly detection module.
"""

import pytest
import math
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))

from anomaly_detector import detect_anomaly, compute_baseline


class TestComputeBaseline:
    def test_empty_readings(self):
        result = compute_baseline([])
        assert result["mean"] == 0.0
        assert result["stddev"] == 0.0
        assert result["sample_count"] == 0

    def test_single_reading(self):
        result = compute_baseline([50.0])
        assert result["mean"] == 50.0
        assert result["stddev"] == 0.0
        assert result["sample_count"] == 1

    def test_multiple_readings(self):
        result = compute_baseline([10.0, 20.0, 30.0])
        assert result["mean"] == 20.0
        assert result["sample_count"] == 3
        assert abs(result["stddev"] - 10.0) < 0.01

    def test_identical_readings(self):
        result = compute_baseline([50.0, 50.0, 50.0])
        assert result["mean"] == 50.0
        assert result["stddev"] == 0.0
        assert result["sample_count"] == 3


class TestDetectAnomaly:
    def test_insufficient_history(self):
        result = detect_anomaly(100.0, [10.0, 20.0], min_history=5)
        assert result["is_anomaly"] is False
        assert result["status"] == "INSUFFICIENT_DATA"
        assert result["z_score"] == 0.0
        assert "Insufficient history" in result["reason"]

    def test_normal_reading_not_anomaly(self):
        readings = [10.0, 12.0, 11.0, 13.0, 9.0]
        result = detect_anomaly(11.0, readings, z_threshold=3.0, min_history=5)
        assert result["is_anomaly"] is False
        assert result["status"] == "NORMAL"
        assert result["baseline_sample_count"] == 5

    def test_significant_spike_is_anomaly(self):
        readings = [10.0, 12.0, 11.0, 13.0, 9.0]
        result = detect_anomaly(100.0, readings, z_threshold=3.0, min_history=5)
        assert result["is_anomaly"] is True
        assert result["status"] == "ANOMALY"
        assert result["z_score"] > 3.0
        assert "anomaly" in result["reason"].lower() or "σ" in result["reason"]

    def test_zero_variance_different_value(self):
        readings = [50.0, 50.0, 50.0, 50.0, 50.0]
        result = detect_anomaly(100.0, readings, z_threshold=3.0, min_history=5)
        assert result["is_anomaly"] is True
        assert result["z_score"] == 999.0  # sentinel for zero variance
        assert "Zero-variance" in result["reason"]

    def test_zero_variance_same_value(self):
        readings = [50.0, 50.0, 50.0, 50.0, 50.0]
        result = detect_anomaly(50.0, readings, z_threshold=3.0, min_history=5)
        assert result["is_anomaly"] is False
        assert "Zero-variance" in result["reason"]

    def test_below_baseline_is_anomaly(self):
        readings = [100.0, 102.0, 98.0, 105.0, 95.0]
        result = detect_anomaly(10.0, readings, z_threshold=3.0, min_history=5)
        assert result["is_anomaly"] is True
        assert result["z_score"] > 3.0

    def test_multiple_cities_different_baselines(self):
        # City A has low pollution baseline
        result_a = detect_anomaly(50.0, [10.0, 12.0, 11.0, 13.0, 9.0],
                                   z_threshold=3.0, min_history=5)
        assert result_a["is_anomaly"] is True  # 50 is way above ~11

        # City B has high pollution baseline
        result_b = detect_anomaly(150.0, [140.0, 142.0, 145.0, 138.0, 144.0],
                                   z_threshold=3.0, min_history=5)
        assert result_b["is_anomaly"] is False  # 150 is within normal range for ~142

    def test_configurable_threshold(self):
        readings = [10.0, 12.0, 11.0, 13.0, 9.0]
        # With low threshold, 20 might be anomalous
        result_low = detect_anomaly(20.0, readings, z_threshold=1.0, min_history=5)
        # With high threshold, 20 might not be anomalous
        result_high = detect_anomaly(20.0, readings, z_threshold=5.0, min_history=5)

        assert result_low["z_score"] == result_high["z_score"]
        # They may differ in is_anomaly depending on actual z-score

    def test_reasonable_explanation(self):
        readings = [10.0, 12.0, 11.0, 13.0, 9.0]
        result = detect_anomaly(100.0, readings, z_threshold=3.0, min_history=5)
        assert "reason" in result
        assert len(result["reason"]) > 0
        assert "100" in result["reason"] or "100.0" in result["reason"]
