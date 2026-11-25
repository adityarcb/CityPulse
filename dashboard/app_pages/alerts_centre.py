"""
CityPulse — Alerts centre page

Comprehensive alert management with severity filtering, per-city alert state machine,
and CSV export. Clear visual distinction between NORMAL, HIGH, CRITICAL,
escalations, de-escalations, and recoveries.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from data import (
    load_alerts,
    load_city_alert_state,
    classify_severity,
    severity_color,
    severity_icon,
    severity_badge_color,
    data_freshness_label,
    get_system_health,
    HIGH_THRESHOLD,
    CRITICAL_THRESHOLD,
)
from theme import (
    get_severity_badge_color,
    get_city_color,
    SEVERITY_NORMAL,
    SEVERITY_HIGH,
    SEVERITY_CRITICAL,
)


# ── Header ────────────────────────────────────────────────────────────────────
st.header("Alerts centre", icon=":material/notifications_active:")
st.caption(
    "Event-driven pollution alerts triggered strictly on severity transitions. "
    f"HIGH ≥ {HIGH_THRESHOLD:.0f} µg/m³ · CRITICAL ≥ {CRITICAL_THRESHOLD:.0f} µg/m³"
)

# ── Load data ────────────────────────────────────────────────────────────────
alerts = load_alerts()
alert_state = load_city_alert_state()
health = get_system_health()

# ── KPI row ──────────────────────────────────────────────────────────────────
if not alerts.empty:
    alerts["created_at"] = pd.to_datetime(alerts["created_at"], utc=True)

    if "severity" not in alerts.columns:
        alerts["severity"] = alerts["pm25"].apply(classify_severity)

    total = len(alerts)
    critical = int((alerts["severity"] == "CRITICAL").sum())
    high = int((alerts["severity"] == "HIGH").sum())
    highest = alerts["pm25"].max()

    with st.container(horizontal=True):
        st.metric("Total alerts", f"{total:,}", border=True, help="Total state transition events recorded")
        st.metric("Critical alerts", critical, border=True, help="Alerts with PM2.5 ≥ CRITICAL threshold")
        st.metric("High alerts", high, border=True, help="Alerts with PM2.5 ≥ HIGH threshold")
        st.metric("Peak PM2.5", f"{highest:.1f} µg/m³", border=True, help="Highest recorded value across all alerts")
else:
    st.info("No active pollution alerts recorded yet.", icon=":material/check_circle:")

# ── Filters ──────────────────────────────────────────────────────────────────
if not alerts.empty:
    with st.container(horizontal=True):
        severity_filter = st.pills(
            "Filter by severity",
            options=["All", "CRITICAL", "HIGH", "NORMAL"],
            default="All",
            key="alerts_severity",
        )

        cities = sorted(alerts["city"].unique())
        city_filter = st.multiselect(
            "Monitored cities",
            options=cities,
            default=cities,
            key="alerts_cities",
        )

    # Apply filters
    filtered = alerts[alerts["city"].isin(city_filter)]
    if severity_filter != "All":
        filtered = filtered[filtered["severity"] == severity_filter]

    if filtered.empty:
        st.info(
            "No alerts match the selected filters.",
            icon=":material/filter_alt_off:",
        )
    else:
        # ── Alert Feed ───────────────────────────────────────────────────
        st.subheader(f"Event transition feed ({len(filtered)})", icon=":material/list:")

        for _, alert in filtered.head(40).iterrows():
            sev = alert["severity"]
            # B6: Single centralized severity token map
            badge_color = get_severity_badge_color(sev)
            age, _ = data_freshness_label(alert["created_at"])

            # Detect transition type from message
            msg = str(alert.get("alert_message", ""))
            type_label, type_color = "Threshold Event", "orange"
            if "de-escalated" in msg.lower():
                type_label, type_color = "De-escalation ↘", "blue"
            elif "escalated" in msg.lower():
                type_label, type_color = "Escalation ↗", "violet"
            elif "recovered" in msg.lower():
                type_label, type_color = "Recovery ✓", "green"
            elif sev == "CRITICAL":
                type_label, type_color = "Critical Breach", "red"

            with st.container(border=True):
                with st.container(horizontal=True):
                    st.badge(sev, color=badge_color, icon=severity_icon(sev))
                    st.badge(type_label, color=type_color)
                    st.markdown(f"**{alert['city']}** · `{alert['pm25']:.1f} µg/m³`")
                    st.caption(f"{age} ({alert['created_at'].strftime('%b %d, %H:%M:%S UTC')})")
                st.markdown(f"_{msg}_")

        # ── CSV export ───────────────────────────────────────────────────
        st.download_button(
            "Download alert history (CSV)",
            data=filtered.to_csv(index=False),
            file_name=f"citypulse_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            icon=":material/download:",
        )

# ── Per-city alert state machine (B5, B14) ───────────────────────────────────
st.divider()
st.header("City alert state machine", icon=":material/hub:")
st.caption("Persistent state per canonical city tracking real-time status and transition counters (NORMAL ↔ HIGH ↔ CRITICAL).")

if not alert_state.empty:
    cols = st.columns(2)
    for idx, (_, row) in enumerate(alert_state.iterrows()):
        sev = row["current_severity"]
        badge_color = get_severity_badge_color(sev)
        city_name = row["city"]

        with cols[idx % 2]:
            with st.container(border=True):
                with st.container(horizontal=True):
                    st.badge(sev, color=badge_color, icon=severity_icon(sev))
                    st.markdown(f"### {city_name}")

                # Mini state progression indicator
                state_pills = []
                for s in ["NORMAL", "HIGH", "CRITICAL"]:
                    if s == sev:
                        state_pills.append(f"**[{s}]**")
                    else:
                        state_pills.append(f"<span style='opacity:0.4'>{s}</span>")
                st.markdown(" → ".join(state_pills), unsafe_allow_html=True)

                # Full words with tooltips (B14)
                with st.container(horizontal=True):
                    st.metric("Total Alerts", int(row["alert_count"]), help="Total notifications sent for this city")
                    st.metric("Escalations", int(row["escalation_count"]), help="HIGH → CRITICAL transitions")
                    st.metric("De-escalations", int(row.get("de_escalation_count", 0)), help="CRITICAL → HIGH transitions")
                    st.metric("Recoveries", int(row["recovery_count"]), help="HIGH/CRITICAL → NORMAL return to clean air")

                last_notif = row.get("last_notification_at")
                if pd.notna(last_notif):
                    ts = pd.to_datetime(last_notif, utc=True)
                    age, _ = data_freshness_label(ts)
                    st.caption(f"Last transition: {age} ({ts.strftime('%b %d, %H:%M UTC')})")
                else:
                    st.caption("No alert transitions triggered yet.")
else:
    st.caption("No alert state data available yet.")
