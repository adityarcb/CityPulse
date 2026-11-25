import os

HIGH_THRESHOLD = float(os.getenv("HIGH_THRESHOLD", "100"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "150"))


def classify_severity(pm25: float) -> str:
    if pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


def get_alert_message(pm25: float, severity: str) -> str:
    if severity == "CRITICAL":
        return f"Critical PM2.5 reading: {pm25:.1f} µg/m³ (≥{CRITICAL_THRESHOLD})"
    elif severity == "HIGH":
        return f"High PM2.5 reading: {pm25:.1f} µg/m³ (≥{HIGH_THRESHOLD})"
    return f"Normal PM2.5 reading: {pm25:.1f} µg/m³"
