"""
CityPulse — Dashboard home page

Hero section + KPI cards + city stations + key charts.
Addresses:
- B3: Derived directly from city_latest single source of truth.
- B6: Centralized severity token mapping.
- B13: Stale indicators and handling.
- B14: Consistent 1-decimal formatting.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timezone

from data import (
    get_system_health,
    get_latest_readings_per_city,
    load_alerts,
    load_readings,
    classify_severity,
    severity_color,
    severity_icon,
    data_freshness_label,
    is_stale_reading,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from charts import pm25_bar_chart, pm25_timeseries_chart
from theme import get_severity_badge_color, get_city_color


# ── Hero Section ──────────────────────────────────────────────────────────────
st.header("Urban environmental intelligence", icon=":material/air:")
st.caption(
    "Real-time PM2.5 monitoring, atmospheric anomaly detection, and city-level telemetry."
)

health = get_system_health()

if health:
    latest_ts = health.get("latest_reading")
    label, status = data_freshness_label(latest_ts)
    cities_count = health.get("cities_monitored", 0)
    is_stale = is_stale_reading(latest_ts)

    with st.container(horizontal=True):
        if is_stale:
            st.badge(f"Historical telemetry · {label}", color="orange", icon=":material/schedule:")
            st.caption(f"**{cities_count}** canonical cities monitored · Stream currently idle")
        elif status == "fresh":
            st.badge("Live streaming", color="green", icon=":material/radio_button_checked:")
            st.caption(f"**{cities_count}** canonical cities monitored · Updated **{label}**")
        else:
            st.badge(f"Telemetry {label}", color="blue", icon=":material/history:")
            st.caption(f"**{cities_count}** canonical cities monitored")
else:
    st.info(
        "Cannot establish connection to PostgreSQL. Ensure Docker container is running.",
        icon=":material/cloud_off:",
    )
    st.stop()

# ── KPI Cards (B3: Single source of truth from city_latest) ───────────────────
latest_readings = get_latest_readings_per_city()
alerts_df = load_alerts()

if latest_readings.empty:
    st.info(
        "No pollution readings stored yet. Ingest streaming events to begin.",
        icon=":material/hourglass_empty:",
    )
    st.stop()

avg_pm25 = latest_readings["pm25"].mean()
max_pm25 = latest_readings["pm25"].max()
max_city = latest_readings.loc[latest_readings["pm25"].idxmax(), "city"]
cities_monitored = health.get("cities_monitored", 0)
total_readings = health.get("total_readings", 0)
total_alerts = health.get("total_alerts", 0)
cities_normal = health.get("cities_normal", 0)
cities_high = health.get("cities_high", 0)
cities_critical = health.get("cities_critical", 0)

# Primary KPI Row
with st.container(horizontal=True):
    st.metric(
        "Cities monitored",
        cities_monitored,
        border=True,
        help="Unique canonical Indian cities actively monitored",
    )
    st.metric(
        "Average PM2.5",
        f"{avg_pm25:.1f} µg/m³",
        border=True,
        help="Mean ambient concentration across all stations",
    )
    st.metric(
        "Total readings",
        f"{total_readings:,}",
        border=True,
        help="Total deduplicated readings in database",
    )
    st.metric(
        "Total alerts",
        f"{total_alerts:,}",
        border=True,
        help="Total state-transition alert events triggered",
    )

# Station Severity Breakdown (Guarantees NORMAL + HIGH + CRITICAL = Monitored)
with st.container(horizontal=True):
    st.metric(
        "Peak reading",
        f"{max_pm25:.1f} µg/m³",
        delta_description=max_city,
        border=True,
    )
    st.metric(
        "Cities NORMAL",
        cities_normal,
        border=True,
        help="Cities with PM2.5 < HIGH threshold",
    )
    st.metric(
        "Cities HIGH",
        cities_high,
        border=True,
        help="Cities with HIGH ≤ PM2.5 < CRITICAL",
    )
    st.metric(
        "Cities CRITICAL",
        cities_critical,
        border=True,
        help="Cities with PM2.5 ≥ CRITICAL threshold",
    )

# ── Visual Intelligence: Comparative charts ──────────────────────────────────
st.header("Comparative air intelligence", icon=":material/analytics:")

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.subheader("Current PM2.5 by station", icon=":material/bar_chart:")
        fig = pm25_bar_chart(latest_readings)
        st.plotly_chart(
            fig,
            key="dash_pm25_bar",
            alt="PM2.5 comparison bar chart across monitored cities",
            width="stretch",
        )

with col2:
    with st.container(border=True):
        st.subheader("Recent PM2.5 trend", icon=":material/show_chart:")
        readings_df = load_readings()
        if not readings_df.empty:
            readings_df["recorded_at"] = pd.to_datetime(readings_df["recorded_at"], utc=True)
            fig = pm25_timeseries_chart(readings_df)
            st.plotly_chart(
                fig,
                key="dash_pm25_ts",
                alt="PM2.5 time series for all monitored cities",
                width="stretch",
            )
        else:
            st.caption("No time-series data available yet.")

# ── Latest readings by station ───────────────────────────────────────────────
st.header("Monitored air stations", icon=":material/location_city:")

latest_readings["recorded_at"] = pd.to_datetime(latest_readings["recorded_at"], utc=True)
latest_readings["severity"] = latest_readings["pm25"].apply(classify_severity)

city_cols = st.columns(2)
sorted_readings = latest_readings.sort_values("pm25", ascending=False)

for idx, (_, row) in enumerate(sorted_readings.iterrows()):
    sev = row["severity"]
    icon = severity_icon(sev)
    badge_color = get_severity_badge_color(sev)
    age_label, _ = data_freshness_label(row["recorded_at"])
    stale = is_stale_reading(row["recorded_at"])
    temp_str = f"{row['temperature']:.1f}°C" if pd.notna(row.get("temperature")) else "—"

    with city_cols[idx % 2]:
        with st.container(border=True):
            with st.container(horizontal=True):
                st.badge(sev, color=badge_color, icon=icon)
                if stale:
                    st.badge("STALE", color="gray", icon=":material/schedule:")
                st.markdown(f"### {row['city']}")
            with st.container(horizontal=True):
                st.metric("PM2.5", f"{row['pm25']:.1f} µg/m³")
                st.metric("Temperature", temp_str)
                st.metric("Freshness", age_label)

# ── Recent alerts preview (B6) ───────────────────────────────────────────────
if not alerts_df.empty:
    st.header("Recent alert transitions", icon=":material/notifications:")
    alerts_df["created_at"] = pd.to_datetime(alerts_df["created_at"], utc=True)

    for _, alert in alerts_df.head(5).iterrows():
        sev = alert.get("severity", classify_severity(alert["pm25"]))
        badge_color = get_severity_badge_color(sev)
        age, _ = data_freshness_label(alert["created_at"])

        with st.container(border=True):
            with st.container(horizontal=True):
                st.badge(sev, color=badge_color, icon=severity_icon(sev))
                st.markdown(
                    f"**{alert['city']}** — `{alert['pm25']:.1f} µg/m³` · {age}"
                )
            st.caption(f"_{alert['alert_message']}_")
