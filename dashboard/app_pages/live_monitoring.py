"""
CityPulse — Live monitoring page

Detailed real-time telemetry view with per-city readings, live PM2.5 time-series,
city selector, temperature comparison, and heartbeat connection indicator.
Fixes:
- B6: Centralized severity token mapping.
- B9: Timestamps strictly sorted with gap breaks.
- B10: Unclipped outside bar labels.
- B13: Staleness handled; stale cards dimmed with STALE status badges.
- B14: Consistent 1-decimal formatting and full words for Escalations/Recoveries.
"""

import streamlit as st
import pandas as pd

from data import (
    load_readings,
    load_city_alert_state,
    get_latest_readings_per_city,
    get_system_health,
    classify_severity,
    severity_color,
    severity_icon,
    data_freshness_label,
    is_stale_reading,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from charts import pm25_timeseries_chart, temperature_bar_chart, pm25_bar_chart
from theme import get_severity_badge_color, get_city_color


# ── Header & Stream Heartbeat (B13) ───────────────────────────────────────────
st.header("Real-time telemetry stream", icon=":material/monitor_heart:")

health = get_system_health()
if health:
    latest_ts = health.get("latest_reading")
    label, status = data_freshness_label(latest_ts)
    is_stale = is_stale_reading(latest_ts)

    with st.container(horizontal=True):
        if is_stale:
            st.badge(f"Historical / Stale ({label})", color="orange", icon=":material/schedule:")
            st.caption("Telemetry stream is currently idle. Displaying latest recorded sensor readings.")
        elif status == "fresh":
            st.badge(f"Live telemetry ({label})", color="green", icon=":material/radio_button_checked:")
            st.caption("Active Kafka streaming pipeline receiving live readings.")
        else:
            st.badge(f"Telemetry {label}", color="blue", icon=":material/history:")
            st.caption("Showing last received state.")
else:
    st.error("Cannot connect to the PostgreSQL database.", icon=":material/cloud_off:")
    st.stop()


# ── Load data (Single source of truth: city_latest) ──────────────────────────
latest = get_latest_readings_per_city()
if latest.empty:
    st.info("No readings available in the database yet. Waiting for streaming data…", icon=":material/hourglass_empty:")
    st.stop()

latest["recorded_at"] = pd.to_datetime(latest["recorded_at"], utc=True)
latest["severity"] = latest["pm25"].apply(classify_severity)

readings = load_readings()
if not readings.empty:
    readings["recorded_at"] = pd.to_datetime(readings["recorded_at"], utc=True)

alert_state = load_city_alert_state()
cities = sorted(latest["city"].unique())

# ── City selector with sliding pills ─────────────────────────────────────────
selected_city = st.pills(
    "Filter by monitoring station",
    options=["All stations"] + cities,
    default="All stations",
    key="live_city_filter",
)

# ── Per-city reading cards (B6, B13, B14) ────────────────────────────────────
st.subheader("Station telemetry feeds", icon=":material/sensors:")

display_latest = latest if selected_city == "All stations" else latest[latest["city"] == selected_city]
sorted_latest = display_latest.sort_values("pm25", ascending=False)

card_cols = st.columns(2)
for idx, (_, row) in enumerate(sorted_latest.iterrows()):
    sev = row["severity"]
    age_label, _ = data_freshness_label(row["recorded_at"])
    stale = is_stale_reading(row["recorded_at"])
    temp_str = f"{row['temperature']:.1f}°C" if pd.notna(row.get("temperature")) else "—"

    # Match state machine record
    alert_info = {
        "alerts": int(row.get("alert_count", 0)),
        "escalations": int(row.get("escalation_count", 0)),
        "de_escalations": int(row.get("de_escalation_count", 0)),
        "recoveries": int(row.get("recovery_count", 0)),
    }

    badge_color = get_severity_badge_color(sev)

    with card_cols[idx % 2]:
        with st.container(border=True):
            with st.container(horizontal=True):
                st.badge(sev, color=badge_color, icon=severity_icon(sev))
                if stale:
                    st.badge("STALE", color="gray", icon=":material/schedule:")
                st.markdown(f"### {row['city']}")

            with st.container(horizontal=True):
                st.metric("PM2.5", f"{row['pm25']:.1f} µg/m³")
                st.metric("Temperature", temp_str)
                st.metric("Freshness", age_label)

            # Full words with tooltips (B14)
            st.caption(
                f"**Alert History:** {alert_info['alerts']} Alerts · "
                f"{alert_info['escalations']} Escalations · "
                f"{alert_info['de_escalations']} De-escalations · "
                f"{alert_info['recoveries']} Recoveries"
            )


# ── Live PM2.5 time-series chart (B9) ────────────────────────────────────────
st.subheader("Time-series concentration trend", icon=":material/show_chart:")
st.caption("Ordered chronologically with gap breaks across inactive periods.")

if not readings.empty:
    with st.container(border=True):
        fig = pm25_timeseries_chart(
            readings,
            city=selected_city if selected_city != "All stations" else None,
        )
        st.plotly_chart(
            fig,
            key="live_ts_chart",
            alt="Time series PM2.5 chart across monitored cities",
            width="stretch",
        )
else:
    st.caption("No time-series data available.")


# ── City comparison and temperature (B10) ────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.subheader("PM2.5 station comparison", icon=":material/bar_chart:")
        fig = pm25_bar_chart(latest)
        st.plotly_chart(
            fig,
            key="live_pm25_bar",
            alt="PM2.5 bar chart comparison across all cities",
            width="stretch",
        )

with col2:
    with st.container(border=True):
        st.subheader("Ambient temperature", icon=":material/thermostat:")
        temp_data = latest.dropna(subset=["temperature"])
        if not temp_data.empty:
            fig = temperature_bar_chart(temp_data)
            st.plotly_chart(
                fig,
                key="live_temp_bar",
                alt="Temperature bar chart comparison across cities",
                width="stretch",
            )
        else:
            st.caption("No temperature data available.")
