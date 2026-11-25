"""
CityPulse — Environmental Intelligence Dashboard (Cluster Computing)

Multi-page Streamlit application with living atmospheric design system,
HTML5 canvas particle dynamics, sidebar navigation, real-time auto-refresh,
and dedicated cluster computing modules.
"""

import os
import sys

# Ensure dashboard directory is on the path for local imports
_dashboard_dir = os.path.dirname(os.path.abspath(__file__))
if _dashboard_dir not in sys.path:
    sys.path.insert(0, _dashboard_dir)

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from data import get_system_health, data_freshness_label, get_city_metrics
from components.styles import inject_custom_styles
from components.particle_background import render_particle_background

# ── Page config (must be first Streamlit command) ─────────────────────────────
st.set_page_config(
    page_title="CityPulse — Environmental Intelligence",
    page_icon=":material/air:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Inject Living Atmosphere Design System Styles ─────────────────────────────
inject_custom_styles()

# ── Fetch real-time health for smart refresh & atmosphere particle dynamics ───
health = get_system_health()
metrics = get_city_metrics()
avg_pm25 = float(metrics.get("avg_pm25", 65.0)) if metrics else 65.0

# ── Render dynamic living atmosphere canvas particle layer ────────────────────
render_particle_background(avg_pm25=avg_pm25)

# ── Smart Auto-Refresh (B12): throttles when idle or offline ──────────────────
refresh_interval = 10000  # 10s default
if health:
    _, fresh_status = data_freshness_label(health.get("latest_reading"))
    if fresh_status in ("idle", "stale", "offline"):
        refresh_interval = 30000  # Throttle to 30s when inactive

st_autorefresh(interval=refresh_interval, key="citypulse_refresh")

# ── Navigation Architecture (13 Pages) ────────────────────────────────────────
page = st.navigation(
    {
        "Overview": [
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/dashboard.py"),
                title="Dashboard",
                icon=":material/dashboard:",
                default=True,
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/live_monitoring.py"),
                title="Live monitoring",
                icon=":material/monitor_heart:",
            ),
        ],
        "Spatial & Temporal": [
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/pollution_map.py"),
                title="Pollution map",
                icon=":material/map:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/historical.py"),
                title="Historical analytics",
                icon=":material/timeline:",
            ),
        ],
        "Alerts & Intelligence": [
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/alerts_centre.py"),
                title="Alerts centre",
                icon=":material/notifications_active:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/anomaly_detection.py"),
                title="Anomaly detection",
                icon=":material/query_stats:",
            ),
        ],
        "Cluster Computing": [
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/spark_sql.py"),
                title="Spark SQL & DataFrames",
                icon=":material/database:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/ml_lab.py"),
                title="Spark MLlib Lab",
                icon=":material/model_training:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/graph_analytics.py"),
                title="Graph Analytics (GraphX)",
                icon=":material/hub:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/kafka_pipeline.py"),
                title="Kafka Streaming Pipeline",
                icon=":material/stream:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/rdd_lab.py"),
                title="Architecture & RDD Lab",
                icon=":material/account_tree:",
            ),
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/scala_job.py"),
                title="Scala Analytics Job",
                icon=":material/terminal:",
            ),
        ],
        "Operations": [
            st.Page(
                os.path.join(_dashboard_dir, "app_pages/system_health.py"),
                title="System health",
                icon=":material/monitor:",
            ),
        ],
    },
    position="sidebar",
)

# ── Logo Branding ─────────────────────────────────────────────────────────────
_logo = os.path.join(_dashboard_dir, "assets", "citypulse_logo.svg")
_icon = os.path.join(_dashboard_dir, "assets", "citypulse_icon.svg")
if os.path.exists(_logo):
    st.logo(_logo, icon_image=_icon if os.path.exists(_icon) else None)
else:
    st.logo(":material/air:", icon_image=None)

# ── Sidebar Footer — Live Status & Academic Tag ───────────────────────────────
with st.sidebar:
    st.space("medium")

    if health:
        latest = health.get("latest_reading")
        label, status = data_freshness_label(latest)

        if status == "fresh":
            st.badge(f"Live · updated {label}", icon=":material/radio_button_checked:", color="green")
        elif status == "stale":
            st.badge(f"Stale · {label}", icon=":material/schedule:", color="orange")
        elif status == "idle":
            st.badge(f"Idle · {label}", icon=":material/history:", color="blue")
        else:
            st.badge("Offline", icon=":material/cloud_off:", color="red")

        cities = health.get("cities_monitored", 0)
        st.caption(f"{cities} cities monitored · {health.get('total_readings', 0):,} readings")
    else:
        st.badge("Connecting…", icon=":material/sync:", color="orange")

    st.markdown(
        """
        <div style="margin-top: 18px; padding: 10px 12px; border-radius: 8px; background: rgba(45, 212, 191, 0.08); border: 1px solid rgba(45, 212, 191, 0.2);">
            <div style="font-size: 11px; font-weight: 600; color: #2DD4BF; letter-spacing: 0.05em; text-transform: uppercase;">Course Project</div>
            <div style="font-size: 12px; font-weight: 500; color: #F1F5F9; margin-top: 2px;">Cluster Computing</div>
            <div style="font-size: 10px; color: #94A3B8; margin-top: 2px;">Spark · Kafka · GraphX · MLlib</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ── Run the selected page ─────────────────────────────────────────────────────
page.run()
