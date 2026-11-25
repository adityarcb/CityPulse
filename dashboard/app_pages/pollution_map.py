"""
CityPulse — Pollution map page

Interactive geographic visualization showing city-level PM2.5 readings
on a dark-themed map with severity-colored markers and anomaly detection.
Addresses Bug B8:
- All 10 canonical cities (including Bengaluru) appear on map and table.
- Auto-fit bounds over markers with padding so no city is cropped.
- Accurate caption explaining marker size (PM2.5) and violet rings (statistical anomalies).
- Clutter-free layout.
"""

import streamlit as st
import pandas as pd

from data import (
    get_latest_readings_per_city,
    load_city_alert_state,
    load_anomalies,
    classify_severity,
    severity_color,
    data_freshness_label,
    is_stale_reading,
    get_system_health,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from city_coordinates import get_city_coordinates
from charts import pollution_map_figure
from theme import get_severity_badge_color, get_city_color


# ── Header ────────────────────────────────────────────────────────────────────
st.header("Geographic pollution intelligence", icon=":material/map:")
st.caption(
    "Live atmospheric monitoring station network across India. "
    "Markers reflect city-centre monitoring stations with real-time severity classification."
)

health = get_system_health()
if health:
    latest_ts = health.get("latest_reading")
    label, status = data_freshness_label(latest_ts)
    if is_stale_reading(latest_ts):
        st.badge(f"Stale data ({label}) · Ingestion idle", icon=":material/schedule:", color="orange")
    elif status == "fresh":
        st.badge(f"Live telemetry ({label})", icon=":material/radio_button_checked:", color="green")
    else:
        st.badge(f"Telemetry {label}", icon=":material/history:", color="blue")

# ── Load data (Single source of truth: city_latest) ──────────────────────────
latest = get_latest_readings_per_city()

if latest.empty:
    st.info("No monitoring station readings available in the database yet.", icon=":material/hourglass_empty:")
    st.stop()

latest["recorded_at"] = pd.to_datetime(latest["recorded_at"], utc=True)
anomalies = load_anomalies()

# ── Severity Filter ──────────────────────────────────────────────────────────
with st.container(horizontal=True):
    selected_severity = st.pills(
        "Filter by severity",
        options=["All", "NORMAL", "HIGH", "CRITICAL"],
        default="All",
        key="map_severity_filter",
    )

# ── Build map data ───────────────────────────────────────────────────────────
map_rows = []
for _, row in latest.iterrows():
    city = row["city"]
    coords = get_city_coordinates(city)
    if coords is None:
        continue

    severity = classify_severity(row["pm25"])

    if selected_severity != "All" and severity != selected_severity:
        continue

    # Anomaly status for this city
    is_anomaly = False
    z_score = 0.0
    if not anomalies.empty:
        city_anoms = anomalies[anomalies["city"] == city]
        if not city_anoms.empty:
            latest_anom = city_anoms.iloc[0]
            is_anomaly = bool(latest_anom.get("is_anomaly", False))
            z_score = float(latest_anom.get("z_score", 0.0))

    age_label, _ = data_freshness_label(row["recorded_at"])
    stale = is_stale_reading(row["recorded_at"])

    map_rows.append({
        "city": city,
        "lat": coords["lat"],
        "lon": coords["lon"],
        "pm25": round(float(row["pm25"]), 1),
        "temperature": round(float(row["temperature"]), 1) if pd.notna(row.get("temperature")) else None,
        "severity": severity,
        "is_anomaly": is_anomaly,
        "z_score": z_score,
        "freshness": f"{age_label} (STALE)" if stale else age_label,
        "recorded_at": row["recorded_at"],
    })

if not map_rows:
    st.info(
        "No monitoring stations match the selected severity filter.",
        icon=":material/location_off:",
    )
    st.stop()

map_df = pd.DataFrame(map_rows)

# ── Map Rendering ────────────────────────────────────────────────────────────
with st.container(border=True):
    fig = pollution_map_figure(map_df)
    st.plotly_chart(
        fig,
        key="pollution_map",
        alt="Interactive map showing PM2.5 levels across Indian cities",
        width="stretch",
    )

# ── Legend & Attribution (B8, B15) ───────────────────────────────────────────
with st.container(horizontal=True):
    st.badge(f"NORMAL (<{HIGH_THRESHOLD:.0f})", color="green", icon=":material/check_circle:")
    st.badge(f"HIGH ({HIGH_THRESHOLD:.0f}-{CRITICAL_THRESHOLD:.0f})", color="orange", icon=":material/warning:")
    st.badge(f"CRITICAL (≥{CRITICAL_THRESHOLD:.0f})", color="red", icon=":material/emergency:")
    st.badge("ANOMALY (≥3.0σ)", color="violet", icon=":material/query_stats:")

st.caption(
    "**Visual Encoding:** Marker size is proportional to PM2.5 concentration. "
    "Outer violet rings highlight active statistical anomalies (≥3.0σ from rolling baseline). "
    "Coordinates represent central ambient monitoring stations."
)

# ── Station Telemetry Table (B8) ─────────────────────────────────────────────
st.subheader(f"Monitored station telemetry ({len(map_df)} cities)", icon=":material/table_chart:")

display_df = map_df[["city", "pm25", "severity", "temperature", "is_anomaly", "freshness"]].copy()
display_df = display_df.rename(columns={
    "city": "Station / City",
    "pm25": "PM2.5 (µg/m³)",
    "severity": "Severity Level",
    "temperature": "Temperature (°C)",
    "is_anomaly": "Anomaly Flag",
    "freshness": "Data Freshness",
})

st.dataframe(
    display_df.sort_values("PM2.5 (µg/m³)", ascending=False),
    hide_index=True,
    width="stretch",
    alt="Table of all monitored cities with real-time PM2.5 levels and coordinates",
)
