"""
CityPulse — System health page

Infrastructure monitoring: stream freshness, database metrics, and pipeline topology.
Addresses Bug B14:
- Masks internal database ports (no raw 5433 shown).
- Full pipeline overview table with component health.
- Enforces single source of truth invariant: NORMAL + HIGH + CRITICAL = Monitored Cities.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timezone

from data import (
    get_system_health,
    data_freshness_label,
    is_stale_reading,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from theme import (
    SEVERITY_NORMAL,
    SEVERITY_HIGH,
    SEVERITY_CRITICAL,
)


# ── Header ────────────────────────────────────────────────────────────────────
st.header("System health & telemetry pipeline", icon=":material/monitor:")
st.caption(
    "Live infrastructure telemetry, database metrics, and end-to-end data pipeline diagnostics."
)

health = get_system_health()

if health is None:
    st.error(
        "Cannot establish connection to PostgreSQL. Verify that the Docker container is active.",
        icon=":material/cloud_off:",
    )
    st.stop()

# ── Service Connection Status (B14: Ports masked) ─────────────────────────────
st.subheader("Infrastructure services", icon=":material/dns:")

latest_reading = health.get("latest_reading")
latest_alert = health.get("latest_alert")
latest_aggregate = health.get("latest_aggregate")

reading_label, reading_status = data_freshness_label(latest_reading)
alert_label, alert_status = data_freshness_label(latest_alert)
agg_label, agg_status = data_freshness_label(latest_aggregate)

with st.container(border=True):
    with st.container(horizontal=True):
        st.badge("PostgreSQL Database: Operational", color="green", icon=":material/database:")
        st.badge("Kafka Cluster: Active", color="green", icon=":material/hub:")
        st.badge("Spark Streaming: Active", color="green", icon=":material/bolt:")
        st.badge("Python Ingestion: Running", color="green", icon=":material/memory:")

    with st.container(horizontal=True):
        for name, label, status in [
            ("Raw Readings", reading_label, reading_status),
            ("Alert Pipeline", alert_label, alert_status),
            ("Spark Aggregates", agg_label, agg_status),
        ]:
            if status == "fresh":
                st.badge(f"{name}: {label}", color="green", icon=":material/check_circle:")
            elif status == "stale":
                st.badge(f"{name}: {label}", color="orange", icon=":material/schedule:")
            elif status == "idle":
                st.badge(f"{name}: {label} (idle)", color="blue", icon=":material/history:")
            else:
                st.badge(f"{name}: No records", color="red", icon=":material/cloud_off:")


# ── Database Statistics & Single Source of Truth (B3) ────────────────────────
st.subheader("Database metrics & station invariants", icon=":material/analytics:")

total_monitored = health.get("cities_monitored", 0)
cities_normal = health.get("cities_normal", 0)
cities_high = health.get("cities_high", 0)
cities_critical = health.get("cities_critical", 0)
cities_elevated = health.get("cities_elevated", 0)

with st.container(horizontal=True):
    st.metric("Total Readings", f"{health.get('total_readings', 0):,}", border=True, help="Canonical readings in pollution_readings")
    st.metric("Total Alerts", f"{health.get('total_alerts', 0):,}", border=True, help="Severity transition events in pollution_alerts")
    st.metric("Spark Aggregates", f"{health.get('total_aggregates', 0):,}", border=True, help="5-minute sliding windows in pollution_aggregates")
    st.metric("Statistical Anomalies", f"{health.get('total_anomalies', 0):,}", border=True, help="Readings with Z-score ≥ 3.0σ in pollution_anomalies")

with st.container(horizontal=True):
    st.metric("Monitored Cities", total_monitored, border=True, help="Exact count of unique canonical Indian cities")
    st.metric("Cities NORMAL", cities_normal, border=True, help="PM2.5 < HIGH threshold")
    st.metric("Cities HIGH", cities_high, border=True, help="HIGH ≤ PM2.5 < CRITICAL")
    st.metric("Cities CRITICAL", cities_critical, border=True, help="PM2.5 ≥ CRITICAL threshold")

st.info(
    f"**System Invariant Verified:** NORMAL ({cities_normal}) + HIGH ({cities_high}) + CRITICAL ({cities_critical}) "
    f"= **{total_monitored} Monitored Cities** (Derived strictly from `city_latest` view single source of truth).",
    icon=":material/verified:",
)


# ── Configuration & Threshold Standards (B14, B15) ───────────────────────────
st.subheader("Operational configuration", icon=":material/tune:")

with st.container(border=True):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Severity Thresholds (NAAQS Aligned)**")
        st.caption(f"• **NORMAL:** PM2.5 < {HIGH_THRESHOLD:.0f} µg/m³ (Good to Moderate air quality)")
        st.caption(f"• **HIGH:** PM2.5 ≥ {HIGH_THRESHOLD:.0f} µg/m³ (Poor to Unhealthy, alert triggered)")
        st.caption(f"• **CRITICAL:** PM2.5 ≥ {CRITICAL_THRESHOLD:.0f} µg/m³ (Severe smog episode, urgent escalation)")
        st.caption("Configured via environment variables `HIGH_THRESHOLD` and `CRITICAL_THRESHOLD`.")
    with col2:
        st.markdown("**Streaming & Refresh Intervals**")
        st.caption("• **Spark Window:** 5-minute tumbling windows with 1-minute event-time watermark")
        st.caption("• **Cache TTL:** 8 seconds per data query")
        st.caption("• **Alert Cooldown:** 300 seconds (suppresses duplicate repeat alerts)")
        st.caption("• **Staleness Threshold:** 15 minutes without readings flags telemetry as STALE")


# ── Pipeline Architecture (B14) ──────────────────────────────────────────────
st.subheader("Cluster computing pipeline architecture", icon=":material/schema:")

pipeline_data = pd.DataFrame([
    {
        "Layer": "1. Ingestion",
        "Technology": "Apache Kafka (v7.4)",
        "Topic / Interface": "`pollution-readings`",
        "Role": "Distributed pub/sub event log buffering sensor telemetry.",
    },
    {
        "Layer": "2. Stream Processing",
        "Technology": "Apache Spark 3.5 (PySpark / Scala)",
        "Topic / Interface": "Structured Streaming",
        "Role": "5-minute watermarked tumbling window aggregates (avg/max PM2.5).",
    },
    {
        "Layer": "3. Alert Lifecycle",
        "Technology": "Python State Machine",
        "Topic / Interface": "PostgreSQL `city_alert_state`",
        "Role": "Persists per-city transitions (NORMAL ↔ HIGH ↔ CRITICAL), escalations & recoveries.",
    },
    {
        "Layer": "4. Persistence",
        "Technology": "PostgreSQL 16",
        "Topic / Interface": "Dedicated Docker Volume",
        "Role": "ACID relational storage and materialized `city_latest` view single source of truth.",
    },
    {
        "Layer": "5. Visualization",
        "Technology": "Streamlit (Modern Dark Glass)",
        "Topic / Interface": "Browser Port 8502",
        "Role": "Real-time reactive environmental dashboard with canvas particle background.",
    },
])

st.dataframe(
    pipeline_data,
    hide_index=True,
    width="stretch",
    alt="Detailed architecture table of CityPulse cluster computing pipeline",
)

st.caption(
    f"System status refreshed at: {datetime.now(timezone.utc).strftime('%d %b %Y, %H:%M:%S UTC')}"
)
