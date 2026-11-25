"""
CityPulse — Historical analytics page

Daily trends, hourly trends, city-wise summary, Spark aggregates, and CSV export.
All charts use configurable date ranges and city filters.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from data import (
    load_readings,
    load_aggregates,
    classify_severity,
    severity_color,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from charts import daily_trend_chart, hourly_trend_chart


# ── Header ────────────────────────────────────────────────────────────────────
st.header("Historical analytics", icon=":material/timeline:")
st.caption("Daily and hourly pollution trends, city comparisons, and data export.")

# ── Load data ────────────────────────────────────────────────────────────────
readings = load_readings()

if readings.empty:
    st.info("No readings available yet.", icon=":material/hourglass_empty:")
    st.stop()

readings["recorded_at"] = pd.to_datetime(readings["recorded_at"], utc=True)
readings["recorded_at_local"] = readings["recorded_at"].dt.tz_convert("Asia/Kolkata")
readings["date"] = readings["recorded_at_local"].dt.date
readings["hour"] = readings["recorded_at_local"].dt.floor("h")

cities = sorted(readings["city"].dropna().unique())
min_date = readings["date"].min()
max_date = readings["date"].max()

# ── Filters ──────────────────────────────────────────────────────────────────
with st.container(horizontal=True):
    selected_cities = st.multiselect(
        "Cities",
        options=cities,
        default=cities,
        key="hist_cities",
    )
    date_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="hist_dates",
    )

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

filtered = readings[
    readings["city"].isin(selected_cities)
    & readings["date"].between(start_date, end_date)
].copy()

if filtered.empty:
    st.warning("No readings match the selected filters.", icon=":material/filter_alt_off:")
    st.stop()

# ── Daily PM2.5 trend ────────────────────────────────────────────────────────
with st.container(border=True):
    st.subheader("Daily PM2.5 trend", icon=":material/calendar_month:")
    fig = daily_trend_chart(filtered, selected_cities)
    st.plotly_chart(fig, key="hist_daily", alt="Daily average PM2.5 trend by city")

# ── Hourly PM2.5 trend ──────────────────────────────────────────────────────
with st.container(border=True):
    st.subheader("Hourly PM2.5 trend", icon=":material/schedule:")
    fig = hourly_trend_chart(filtered, selected_cities)
    st.plotly_chart(fig, key="hist_hourly", alt="Hourly average PM2.5 trend by city")

# ── City-wise pollution summary (B4: Mutually Exclusive Buckets) ─────────────
st.subheader("City-wise pollution summary", icon=":material/table_chart:")

summary = (
    filtered.groupby("city", as_index=False)
    .agg(
        average_pm25=("pm25", lambda x: round(float(x.mean()), 1)),
        maximum_pm25=("pm25", lambda x: round(float(x.max()), 1)),
        minimum_pm25=("pm25", lambda x: round(float(x.min()), 1)),
        average_temperature=("temperature", lambda x: round(float(x.mean()), 1) if pd.notna(x.mean()) else 0.0),
        total_readings=("id", "count"),
        normal_readings=("pm25", lambda x: int((x < HIGH_THRESHOLD).sum())),
        high_readings=("pm25", lambda x: int(((x >= HIGH_THRESHOLD) & (x < CRITICAL_THRESHOLD)).sum())),
        critical_readings=("pm25", lambda x: int((x >= CRITICAL_THRESHOLD).sum())),
    )
)

# Rename for clean presentation and clear threshold attribution
display_summary = summary.rename(columns={
    "city": "City",
    "average_pm25": "Avg PM2.5 (µg/m³)",
    "maximum_pm25": "Max PM2.5",
    "minimum_pm25": "Min PM2.5",
    "average_temperature": "Avg Temp (°C)",
    "total_readings": "Total Readings",
    "normal_readings": f"Normal (<{HIGH_THRESHOLD:.0f})",
    "high_readings": f"High ({HIGH_THRESHOLD:.0f}-{CRITICAL_THRESHOLD:.0f})",
    "critical_readings": f"Critical (≥{CRITICAL_THRESHOLD:.0f})",
})

st.dataframe(
    display_summary,
    hide_index=True,
    width="stretch",
    alt="City-wise pollution summary table with mutually exclusive severity counts",
)

# ── Spark aggregates (B11: 5-minute windows with IST & UTC) ──────────────────
st.subheader("Spark Streaming aggregates (5-minute windows)", icon=":material/view_timeline:")
st.caption("Calculated in real-time via Apache Spark Structured Streaming with 5-minute tumbling windows and event-time watermarking.")

aggregates = load_aggregates()

if not aggregates.empty:
    agg_display = aggregates[aggregates["city"].isin(selected_cities)].copy()

    if agg_display.empty:
        st.caption("No aggregates match the selected cities.")
    else:
        # Convert window timestamps to both UTC and IST
        agg_display["window_start_utc"] = pd.to_datetime(agg_display["window_start"], utc=True)
        agg_display["window_start_ist"] = agg_display["window_start_utc"].dt.tz_convert("Asia/Kolkata")
        
        table_df = pd.DataFrame({
            "City": agg_display["city"],
            "Severity": agg_display["status"],
            "Avg PM2.5": agg_display["avg_pm25"].round(1),
            "Max PM2.5": agg_display["max_pm25"].round(1),
            "Reading Count": agg_display["reading_count"],
            "Window Start (IST)": agg_display["window_start_ist"].dt.strftime("%Y-%m-%d %H:%M IST"),
            "Window Start (UTC)": agg_display["window_start_utc"].dt.strftime("%Y-%m-%d %H:%M UTC"),
        })
        st.dataframe(
            table_df,
            hide_index=True,
            width="stretch",
            alt="Spark 5-minute window aggregates table",
        )
else:
    st.caption("No Spark aggregates available yet.")

# ── Export ────────────────────────────────────────────────────────────────────
st.subheader("Export data", icon=":material/download:")

csv_data = (
    filtered
    .drop(columns=["date", "hour", "recorded_at_local"], errors="ignore")
    .to_csv(index=False)
)

st.download_button(
    "Download filtered readings (CSV)",
    data=csv_data,
    file_name=f"citypulse_readings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
    icon=":material/download:",
)
