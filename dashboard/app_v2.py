"""
CityPulse — Environmental Intelligence Dashboard

Multi-page Streamlit application with sidebar navigation, real-time
auto-refresh, and consistent dark theme.
"""

import os
import sys

# Ensure dashboard directory is on the path for local imports
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
from streamlit_autorefresh import st_autorefresh
from data import get_system_health, data_freshness_label

# ── Page config (must be first Streamlit command) ─────────────────────────────
st.set_page_config(
    page_title="CityPulse — Environmental Intelligence",
    page_icon=":material/air:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Auto-refresh every 10 seconds ────────────────────────────────────────────
st_autorefresh(interval=10000, key="citypulse_refresh")

# ── Navigation ────────────────────────────────────────────────────────────────
page = st.navigation(
    {
        "": [
            st.Page("app_pages/dashboard.py", title="Dashboard", icon=":material/dashboard:", default=True),
            st.Page("app_pages/live_monitoring.py", title="Live monitoring", icon=":material/monitor_heart:"),
        ],
        "Analytics": [
            st.Page("app_pages/pollution_map.py", title="Pollution map", icon=":material/map:"),
            st.Page("app_pages/historical.py", title="Historical analytics", icon=":material/timeline:"),
        ],
        "Alerts": [
            st.Page("app_pages/alerts_centre.py", title="Alerts centre", icon=":material/notifications_active:"),
            st.Page("app_pages/anomaly_detection.py", title="Anomaly detection", icon=":material/query_stats:"),
        ],
        "System": [
            st.Page("app_pages/system_health.py", title="System health", icon=":material/monitor:"),
        ],
    },
    position="sidebar",
)

# ── Logo ──────────────────────────────────────────────────────────────────────
_dir = os.path.dirname(os.path.abspath(__file__))
_logo = os.path.join(_dir, "assets", "citypulse_logo.svg")
_icon = os.path.join(_dir, "assets", "citypulse_icon.svg")
if os.path.exists(_logo):
    st.logo(_logo, icon_image=_icon if os.path.exists(_icon) else None)
else:
    st.logo(":material/air:", icon_image=None)

# ── Sidebar footer — live status ──────────────────────────────────────────────
with st.sidebar:
    st.space("large")

    health = get_system_health()
    if health:
        latest = health.get("latest_reading")
        label, status = data_freshness_label(latest)

        if status == "fresh":
            st.badge(f"Live · updated {label}", icon=":material/radio_button_checked:", color="green")
        elif status == "stale":
            st.badge(f"Stale · {label}", icon=":material/schedule:", color="orange")
        else:
            st.badge("Offline", icon=":material/cloud_off:", color="red")

        cities = health.get("cities_monitored", 0)
        st.caption(f"{cities} cities monitored · {health.get('total_readings', 0):,} readings")
    else:
        st.badge("Connecting…", icon=":material/sync:", color="orange")

# ── Run the selected page ────────────────────────────────────────────────────
page.run()
