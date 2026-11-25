"""
CityPulse — Anomaly detection page

Statistical anomaly analysis using rolling baseline (mean & standard deviation per city).
Addresses Bug B7:
- Distinguishes INSUFFICIENT_DATA (<5 baseline readings) from NORMAL variation.
- Accurate banner showing how many cities have adequate historical baseline.
- Filter chips: All / Anomalies only / Insufficient baseline / Normal.
- Z-score scatter plot with safe variation band.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from data import (
    load_anomalies,
    data_freshness_label,
    get_system_health,
)
from theme import (
    SEVERITY_ANOMALY,
    SEVERITY_NORMAL,
    SEVERITY_INSUFFICIENT,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    BG_CARD,
    get_city_color,
)


# ── Header ────────────────────────────────────────────────────────────────────
st.header("Statistical anomaly detection", icon=":material/query_stats:")
st.caption(
    "Rolling baseline analysis evaluating whether sudden spikes are statistically unusual "
    "relative to a city's own historical distribution (Z-score = |x - µ| / σ, threshold ≥ 3.0σ)."
)

st.info(
    "Statistical anomaly ≠ public health emergency. A reading can be clean yet statistically unusual "
    "(e.g., 50 µg/m³ in a 10 µg/m³ baseline), or severe yet normal for that city (e.g., 140 µg/m³ in Delhi).",
    icon=":material/info:",
)

# ── Load data ────────────────────────────────────────────────────────────────
anomalies = load_anomalies()

if anomalies.empty:
    st.info("No anomaly records available in the database yet.", icon=":material/hourglass_empty:")
    st.stop()

anomalies["recorded_at"] = pd.to_datetime(anomalies["recorded_at"], utc=True)
anomalies["baseline_sample_count"] = anomalies["baseline_sample_count"].fillna(0).astype(int)

# Categorize into mutually exclusive statistical statuses (B7)
def determine_status(row):
    if bool(row["is_anomaly"]):
        return "ANOMALY"
    elif row["baseline_sample_count"] < 5:
        return "INSUFFICIENT_DATA"
    return "NORMAL"

anomalies["stat_status"] = anomalies.apply(determine_status, axis=1)

anomaly_rows = anomalies[anomalies["stat_status"] == "ANOMALY"]
insufficient_rows = anomalies[anomalies["stat_status"] == "INSUFFICIENT_DATA"]
normal_rows = anomalies[anomalies["stat_status"] == "NORMAL"]

total_monitored_cities = anomalies["city"].nunique()
cities_with_sufficient_history = anomalies[anomalies["baseline_sample_count"] >= 5]["city"].nunique()

# ── KPI row ──────────────────────────────────────────────────────────────────
with st.container(horizontal=True):
    st.metric("Total evaluated", len(anomalies), border=True, help="Total anomaly evaluations performed")
    st.metric("Anomalies detected", len(anomaly_rows), border=True, help="Readings with Z-score ≥ 3.0")
    st.metric(
        "Baseline readiness",
        f"{cities_with_sufficient_history}/{total_monitored_cities} cities",
        border=True,
        help="Cities with ≥5 prior readings to establish valid mean and standard deviation",
    )
    if not anomaly_rows.empty:
        st.metric(
            "Peak Z-score",
            f"{anomaly_rows['z_score'].max():.2f}σ",
            border=True,
        )

# ── Status Banner (B7) ───────────────────────────────────────────────────────
if not anomaly_rows.empty:
    st.error(
        f"🚨 **{len(anomaly_rows)} statistical anomaly** flagged across {anomaly_rows['city'].nunique()} city station(s). "
        f"These readings deviate significantly from established historical norms.",
        icon=":material/emergency:",
    )
elif cities_with_sufficient_history == 0:
    st.warning(
        f"⚠️ **Baseline forming:** None of the {total_monitored_cities} monitored cities currently have "
        "≥5 historical readings. Status marked as **Insufficient baseline** until more readings arrive.",
        icon=":material/hourglass_top:",
    )
else:
    st.success(
        f"✅ **Baselines stable:** {cities_with_sufficient_history} of {total_monitored_cities} monitored cities "
        "have sufficient history (≥5 readings), and zero statistical anomalies are currently detected.",
        icon=":material/verified:",
    )

# ── Filter Chips (B7) ────────────────────────────────────────────────────────
filter_options = ["All readings", "Anomalies only", "Insufficient baseline", "Normal baseline"]
view_filter = st.pills(
    "Filter records",
    options=filter_options,
    default="All readings",
    key="anomaly_filter",
)

if view_filter == "Anomalies only":
    filtered_data = anomaly_rows
elif view_filter == "Insufficient baseline":
    filtered_data = insufficient_rows
elif view_filter == "Normal baseline":
    filtered_data = normal_rows
else:
    filtered_data = anomalies

# City selector
cities = sorted(anomalies["city"].unique())
selected_cities = st.multiselect(
    "Monitored cities",
    options=cities,
    default=cities,
    key="anomaly_cities",
)

filtered_data = filtered_data[filtered_data["city"].isin(selected_cities)]

# ── Z-Score Scatter Chart ────────────────────────────────────────────────────
if not anomalies.empty:
    with st.container(border=True):
        st.subheader("Statistical Z-score distribution", icon=":material/scatter_plot:")
        scatter_df = anomalies[anomalies["city"].isin(selected_cities)].copy()

        fig = go.Figure()

        # Safe band ±3.0σ
        fig.add_hrect(
            y0=-3.0, y1=3.0,
            fillcolor="rgba(45, 212, 191, 0.08)",
            line_width=0,
            annotation_text="Expected variation (|Z| < 3.0σ)",
            annotation_position="top right",
            annotation_font=dict(color="rgba(45, 212, 191, 0.7)", size=10),
        )

        for city in sorted(scatter_df["city"].unique()):
            city_pts = scatter_df[scatter_df["city"] == city]
            color = get_city_color(city)

            fig.add_trace(go.Scatter(
                x=city_pts["recorded_at"],
                y=city_pts["z_score"],
                mode="markers",
                name=city,
                marker=dict(
                    size=8,
                    color=color,
                    line=dict(
                        color=SEVERITY_ANOMALY,
                        width=city_pts["is_anomaly"].apply(lambda a: 3 if a else 0),
                    ),
                    symbol=city_pts["is_anomaly"].apply(lambda a: "diamond" if a else "circle"),
                ),
                customdata=city_pts[["city", "pm25", "baseline_mean", "baseline_sample_count"]].values,
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Z-score: %{y:.2f}σ<br>"
                    "PM2.5: %{customdata[1]:.1f} µg/m³<br>"
                    "Baseline Mean: %{customdata[2]:.1f}<br>"
                    "Samples: %{customdata[3]}<extra></extra>"
                ),
            ))

        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
            margin=dict(l=45, r=40, t=30, b=40),
            height=320,
            xaxis=dict(gridcolor="rgba(148, 163, 184, 0.12)", title="Recorded Timestamp"),
            yaxis=dict(gridcolor="rgba(148, 163, 184, 0.12)", title="Z-Score (σ)"),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                font=dict(color=TEXT_SECONDARY, size=10),
            ),
        )
        st.plotly_chart(fig, key="anomaly_scatter", alt="Z-score distribution across monitored cities")

# ── Anomaly evaluation cards ─────────────────────────────────────────────────
st.subheader(f"Evaluation logs ({len(filtered_data)})", icon=":material/list:")

if filtered_data.empty:
    st.info("No records match the current filter selection.", icon=":material/filter_alt_off:")
else:
    for _, row in filtered_data.head(25).iterrows():
        status = row["stat_status"]
        age, _ = data_freshness_label(row["recorded_at"])

        if status == "ANOMALY":
            badge_text = "Statistical anomaly"
            badge_color = "violet"
            badge_icon = ":material/emergency:"
        elif status == "INSUFFICIENT_DATA":
            badge_text = "Insufficient baseline (<5)"
            badge_color = "gray"
            badge_icon = ":material/hourglass_top:"
        else:
            badge_text = "Normal variation"
            badge_color = "green"
            badge_icon = ":material/check_circle:"

        with st.container(border=True):
            with st.container(horizontal=True):
                st.badge(badge_text, color=badge_color, icon=badge_icon)
                st.markdown(f"**{row['city']}** · `{row['pm25']:.1f} µg/m³`")
                st.caption(f"{age} ({row['recorded_at'].strftime('%b %d, %H:%M UTC')})")

            with st.container(horizontal=True):
                st.metric("PM2.5", f"{row['pm25']:.1f} µg/m³")
                st.metric("Baseline Mean", f"{row['baseline_mean']:.1f}")
                st.metric("Std Dev (σ)", f"{row['baseline_stddev']:.2f}")
                st.metric("Z-score", f"{row['z_score']:.2f}σ")
                st.metric("Baseline Samples", int(row["baseline_sample_count"]))

            # Clear analytical explanation
            if status == "ANOMALY":
                dev = row["pm25"] - row["baseline_mean"]
                dir_str = "above" if dev > 0 else "below"
                st.markdown(
                    f"⚠️ Reading is **{abs(dev):.1f} µg/m³ {dir_str}** the city's historical mean "
                    f"({row['baseline_mean']:.1f} µg/m³). Z-score of **{row['z_score']:.2f}σ** exceeds the 3.0σ threshold."
                )
            elif status == "INSUFFICIENT_DATA":
                st.caption(
                    f"ℹ️ City has only **{int(row['baseline_sample_count'])} baseline sample(s)** (minimum 5 required). "
                    "Cannot compute a reliable Z-score or standard deviation yet."
                )
            else:
                st.caption(
                    f"✓ Reading is within **{row['z_score']:.2f}σ** of city baseline mean ({row['baseline_mean']:.1f} µg/m³). "
                    "Consistent with normal historical variance."
                )
