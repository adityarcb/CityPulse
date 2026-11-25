import os
import logging
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
from streamlit_autorefresh import st_autorefresh

# Add dashboard directory to path for local imports
import sys
sys.path.insert(0, os.path.dirname(__file__))
from city_coordinates import get_city_coordinates, get_cities_with_coordinates, CITY_COORDINATES

st.set_page_config(
    page_title="CityPulse — Real-Time Urban Pollution Analytics",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HIGH_THRESHOLD = float(os.getenv("HIGH_THRESHOLD", "100"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "150"))

DB_CONFIG = {
    "host": os.getenv("CITYPULSE_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("CITYPULSE_DB_PORT", "5433")),
    "dbname": "citypulse",
    "user": "citypulse",
    "password": os.getenv("CITYPULSE_DB_PASSWORD", "citypulse_dev_password"),
    "connect_timeout": 5
}

st_autorefresh(interval=10000, key="refresh")

st.title("🌍 CityPulse")
st.caption("Real-Time Urban Pollution Analytics")


@st.cache_resource(ttl=60)
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=5)
def load_readings():
    try:
        with get_connection() as conn:
            return pd.read_sql_query(
                """
                SELECT id, city, pm25, temperature, status, recorded_at
                FROM pollution_readings
                ORDER BY recorded_at DESC
                """,
                conn
            )
    except Exception as e:
        logger.error(f"Error loading readings: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_alerts():
    try:
        with get_connection() as conn:
            return pd.read_sql_query(
                """
                SELECT id, city, pm25, alert_message, severity, created_at
                FROM pollution_alerts
                ORDER BY created_at DESC
                LIMIT 100
                """,
                conn
            )
    except Exception as e:
        logger.error(f"Error loading alerts: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_aggregates():
    try:
        with get_connection() as conn:
            return pd.read_sql_query(
                """
                SELECT city, window_start, window_end, avg_pm25, max_pm25,
                       reading_count, avg_temperature, status, updated_at
                FROM pollution_aggregates
                ORDER BY window_start DESC
                LIMIT 50
                """,
                conn
            )
    except Exception as e:
        logger.error(f"Error loading aggregates: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_city_alert_state():
    """Load persistent per-city alert state."""
    try:
        with get_connection() as conn:
            return pd.read_sql_query(
                """
                SELECT city, current_severity, last_notification_at,
                       last_severity_change_at, alert_count,
                       escalation_count, recovery_count, updated_at
                FROM city_alert_state
                ORDER BY updated_at DESC
                """,
                conn
            )
    except Exception as e:
        logger.error(f"Error loading city alert state: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_anomalies():
    """Load recent anomaly detection results."""
    try:
        with get_connection() as conn:
            return pd.read_sql_query(
                """
                SELECT city, pm25, baseline_mean, baseline_stddev,
                       baseline_sample_count, z_score, is_anomaly,
                       anomaly_threshold, event_id, recorded_at
                FROM pollution_anomalies
                ORDER BY recorded_at DESC
                LIMIT 100
                """,
                conn
            )
    except Exception as e:
        logger.error(f"Error loading anomalies: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=5)
def get_system_health():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        (SELECT MAX(recorded_at) FROM pollution_readings) as latest_reading,
                        (SELECT MAX(created_at) FROM pollution_alerts) as latest_alert,
                        (SELECT MAX(updated_at) FROM pollution_aggregates) as latest_aggregate,
                        (SELECT COUNT(*) FROM pollution_readings) as total_readings,
                        (SELECT COUNT(*) FROM pollution_alerts) as total_alerts,
                        (SELECT COUNT(*) FROM pollution_aggregates) as total_aggregates,
                        (SELECT COUNT(*) FROM pollution_anomalies WHERE is_anomaly = TRUE) as total_anomalies,
                        (SELECT COUNT(*) FROM city_alert_state WHERE current_severity IN ('HIGH', 'CRITICAL')) as cities_elevated
                """)
                return cursor.fetchone()
    except Exception as e:
        logger.error(f"Error getting system health: {e}")
        return None


def classify_severity(pm25):
    if pm25 >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif pm25 >= HIGH_THRESHOLD:
        return "HIGH"
    return "NORMAL"


def get_severity_color(severity):
    colors = {
        "CRITICAL": "#dc2626",
        "HIGH": "#f97316",
        "NORMAL": "#16a34a"
    }
    return colors.get(severity, "#6b7280")


def get_severity_icon(severity):
    icons = {
        "CRITICAL": "🔴",
        "HIGH": "🟠",
        "NORMAL": "🟢"
    }
    return icons.get(severity, "⚪")


with st.sidebar:
    st.header("System Health")
    health = get_system_health()
    
    if health:
        latest_reading = health.get("latest_reading")
        if latest_reading:
            if isinstance(latest_reading, str):
                latest_reading = datetime.fromisoformat(latest_reading.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - latest_reading
            if age < timedelta(minutes=5):
                st.success(f"✓ Data fresh ({age.seconds // 60}m ago)")
            else:
                st.warning(f"⚠ Data stale ({age.seconds // 60}m ago)")
        
    st.metric("Total Readings", health.get("total_readings", 0))
    st.metric("Total Alerts", health.get("total_alerts", 0))
    st.metric("Cities Elevated", health.get("cities_elevated", 0))
    st.metric("Anomalies", health.get("total_anomalies", 0))
    
    st.divider()
    st.header("Analytics Filters")


try:
    df = load_readings()
except Exception as error:
    st.error(f"Could not load PostgreSQL data: {error}")
    st.stop()

if df.empty:
    st.info("No pollution readings are stored yet. Waiting for data...")
    st.stop()

df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)
df["recorded_at_local"] = df["recorded_at"].dt.tz_convert("Asia/Kolkata")
df["date"] = df["recorded_at_local"].dt.date

cities = sorted(df["city"].dropna().unique())
selected_cities = st.sidebar.multiselect(
    "Select cities",
    cities,
    default=cities
)

min_date = df["date"].min()
max_date = df["date"].max()

date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

filtered = df[
    df["city"].isin(selected_cities)
    & df["date"].between(start_date, end_date)
].copy()


st.divider()
st.header("🚨 Live Pollution Alerts")

try:
    alerts = load_alerts()
    
    if not alerts.empty:
        alerts["created_at"] = (
            pd.to_datetime(alerts["created_at"], utc=True)
            .dt.tz_convert("Asia/Kolkata")
        )
        
        if "severity" not in alerts.columns:
            alerts["severity"] = alerts["pm25"].apply(classify_severity)
        
        alerts["date"] = alerts["created_at"].dt.date
        
        filtered_alerts = alerts[
            alerts["city"].isin(selected_cities)
            & alerts["date"].between(start_date, end_date)
        ].copy()
        
        total_alerts = len(filtered_alerts)
        critical_alerts = int((filtered_alerts["severity"] == "CRITICAL").sum()) if "severity" in filtered_alerts.columns else 0
        high_alerts = int((filtered_alerts["severity"] == "HIGH").sum()) if "severity" in filtered_alerts.columns else 0
        highest_pm25 = filtered_alerts["pm25"].max() if total_alerts else 0
        
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Total Alerts", total_alerts)
        a2.metric("Critical Alerts", critical_alerts)
        a3.metric("High Alerts", high_alerts)
        a4.metric("Highest PM2.5", f"{highest_pm25:.1f}" if highest_pm25 else "N/A")
        
        if filtered_alerts.empty:
            st.success("No alerts match the selected filters.")
        else:
            display_alerts = filtered_alerts.head(10)
            
            for _, alert in display_alerts.iterrows():
                severity = alert.get("severity", classify_severity(alert["pm25"]))
                color = get_severity_color(severity)
                icon = get_severity_icon(severity)
                
                with st.container():
                    st.markdown(
                        f"""
                        <div style="
                            background: linear-gradient(135deg, {color}15, {color}05);
                            border-left: 4px solid {color};
                            padding: 12px 16px;
                            border-radius: 8px;
                            margin-bottom: 8px;
                        ">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div>
                                    <span style="font-size: 18px; font-weight: 600;">{icon} {severity}</span>
                                    <span style="margin-left: 8px; font-size: 16px;">{alert['city']}</span>
                                </div>
                                <div style="font-size: 24px; font-weight: 700; color: {color};">
                                    {alert['pm25']:.1f}
                                </div>
                            </div>
                            <div style="margin-top: 4px; font-size: 14px; color: #666;">
                                {alert['alert_message']}
                            </div>
                            <div style="margin-top: 4px; font-size: 12px; color: #888;">
                                {alert['created_at'].strftime('%d %b %Y, %I:%M:%S %p IST')}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            
            st.download_button(
                "Download Alerts (CSV)",
                data=filtered_alerts.drop(columns=["date"], errors="ignore").to_csv(index=False),
                file_name=f"citypulse_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    else:
        st.info("No pollution alerts recorded yet.")

except Exception as error:
    st.error(f"Could not load pollution alerts: {error}")


st.divider()
st.header("📈 Current Pollution Analytics")

if filtered.empty:
    st.warning("No readings match the selected filters.")
    st.stop()

latest = (
    filtered.sort_values("recorded_at")
    .drop_duplicates("city", keep="last")
)

latest["current_severity"] = latest["pm25"].apply(classify_severity)
high_cities = int((latest["current_severity"].isin(["HIGH", "CRITICAL"])).sum())
critical_cities = int((latest["current_severity"] == "CRITICAL").sum())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Cities Monitored", latest["city"].nunique())
c2.metric("Average PM2.5", f"{latest['pm25'].mean():.1f}")
c3.metric("Cities with HIGH Status", high_cities)
c4.metric("Cities CRITICAL", critical_cities)
c5.metric("Total Readings", len(filtered))

st.subheader("Latest City Readings")
latest_display = latest[["city", "pm25", "temperature", "status", "recorded_at_local"]].copy()
latest_display = latest_display.sort_values("pm25", ascending=False)
latest_display["recorded_at_local"] = latest_display["recorded_at_local"].dt.strftime("%d %b %Y, %I:%M:%S %p")

st.dataframe(
    latest_display,
    use_container_width=True,
    hide_index=True
)

left, right = st.columns(2)

with left:
    st.subheader("Latest PM2.5 by City")
    fig = go.Figure()
    
    for _, row in latest.iterrows():
        severity = classify_severity(row["pm25"])
        color = get_severity_color(severity)
        fig.add_trace(go.Bar(
            x=[row["city"]],
            y=[row["pm25"]],
            name=row["city"],
            marker_color=color,
            text=f"{row['pm25']:.1f}",
            textposition="auto"
        ))
    
    fig.add_hline(y=HIGH_THRESHOLD, line_dash="dash", line_color="orange",
                  annotation_text=f"HIGH ({HIGH_THRESHOLD})")
    fig.add_hline(y=CRITICAL_THRESHOLD, line_dash="dash", line_color="red",
                  annotation_text=f"CRITICAL ({CRITICAL_THRESHOLD})")
    
    fig.update_layout(
        showlegend=False,
        yaxis_title="PM2.5 (µg/m³)",
        xaxis_title="City"
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Temperature by City")
    fig = px.bar(
        latest,
        x="city",
        y="temperature",
        labels={"temperature": "Temperature (°C)", "city": "City"},
        color="temperature",
        color_continuous_scale="RdYlBu_r"
    )
    st.plotly_chart(fig, use_container_width=True)


st.divider()
st.header("📊 Spark Aggregates (5-Minute Windows)")

try:
    aggregates = load_aggregates()
    
    if not aggregates.empty:
        st.subheader("Recent Aggregate Windows")
        
        agg_display = aggregates[aggregates["city"].isin(selected_cities)].head(10)
        
        for _, row in agg_display.iterrows():
            severity = row.get("status", classify_severity(row["max_pm25"]))
            color = get_severity_color(severity)
            
            st.markdown(
                f"""
                <div style="
                    background: {color}10;
                    border-left: 3px solid {color};
                    padding: 8px 12px;
                    border-radius: 6px;
                    margin-bottom: 6px;
                    font-size: 14px;
                ">
                    <strong>{row['city']}</strong> | 
                    Window: {row['window_start']} to {row['window_end']} |
                    Avg PM2.5: <strong>{row['avg_pm25']:.1f}</strong> |
                    Max: <strong>{row['max_pm25']:.1f}</strong> |
                    Count: {row['reading_count']} |
                    Status: <strong style="color: {color}">{severity}</strong>
                </div>
                """,
                unsafe_allow_html=True
            )
    else:
        st.info("No Spark aggregates available yet.")

except Exception as error:
    st.warning(f"Could not load aggregates: {error}")


st.divider()
st.header("🗺️ Geographic Pollution Map")

try:
    alert_state = load_city_alert_state()
    anomalies = load_anomalies()

    # Build latest readings per city with coordinates
    latest = (
        df.sort_values("recorded_at")
        .drop_duplicates("city", keep="last")
    )

    map_rows = []
    for _, row in latest.iterrows():
        city = row["city"]
        coords = get_city_coordinates(city)
        if coords is None:
            continue

        # Get alert state for this city
        city_state = None
        if not alert_state.empty:
            state_match = alert_state[alert_state["city"] == city]
            if not state_match.empty:
                city_state = state_match.iloc[0]

        # Get latest anomaly for this city
        city_anomaly = None
        if not anomalies.empty:
            anomaly_match = anomalies[anomalies["city"] == city]
            if not anomaly_match.empty:
                city_anomaly = anomaly_match.iloc[0]

        map_rows.append({
            "city": city,
            "lat": coords["lat"],
            "lon": coords["lon"],
            "pm25": row["pm25"],
            "severity": classify_severity(row["pm25"]),
            "recorded_at": row["recorded_at_local"],
            "alert_state": city_state["current_severity"] if city_state is not None else classify_severity(row["pm25"]),
            "is_anomaly": city_anomaly["is_anomaly"] if city_anomaly is not None else False,
            "z_score": city_anomaly["z_score"] if city_anomaly is not None else None,
        })

    if map_rows:
        map_df = pd.DataFrame(map_rows)
        map_df["color"] = map_df["severity"].map({
            "CRITICAL": "#dc2626",
            "HIGH": "#f97316",
            "NORMAL": "#16a34a",
        })
        map_df["size"] = map_df["pm25"].clip(lower=10, upper=200) / 5

        # Add anomaly indicator to size
        map_df.loc[map_df["is_anomaly"] == True, "size"] = map_df.loc[
            map_df["is_anomaly"] == True, "size"
        ] * 1.5

        fig = px.scatter_mapbox(
            map_df,
            lat="lat",
            lon="lon",
            hover_name="city",
            hover_data={
                "lat": False,
                "lon": False,
                "pm25": ":.1f",
                "severity": True,
                "alert_state": True,
                "is_anomaly": True,
                "z_score": ":.2f",
                "recorded_at": True,
            },
            color="severity",
            color_discrete_map={
                "CRITICAL": "#dc2626",
                "HIGH": "#f97316",
                "NORMAL": "#16a34a",
            },
            size="size",
            size_max=25,
            zoom=5,
            mapbox_style="open-street-map",
        )

        fig.update_layout(
            title="City-Level Pollution Severity (Approximate City-Centres)",
            height=500,
            margin={"r": 0, "t": 40, "l": 0, "b": 0},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "Map markers show approximate city-centre locations, not precise sensor positions. "
            "Marker colour indicates severity (green=NORMAL, orange=HIGH, red=CRITICAL). "
            "Larger markers with pulse effect indicate statistical anomalies."
        )

        # Legend for anomaly indicator
        anomaly_cities = map_df[map_df["is_anomaly"] == True]
        if not anomaly_cities.empty:
            st.warning(
                f"⚠️ {len(anomaly_cities)} city(-ies) showing statistical anomalies: "
                + ", ".join(anomaly_cities["city"].tolist())
            )
    else:
        st.info("No cities with known coordinates available for map display.")

except Exception as error:
    st.warning(f"Could not load geographic map: {error}")


st.divider()
st.header("🏙️ Active Alert State by City")

try:
    if not alert_state.empty:
        state_display = alert_state[alert_state["city"].isin(selected_cities)].copy()

        for _, row in state_display.iterrows():
            severity = row["current_severity"]
            color = get_severity_color(severity)
            icon = get_severity_icon(severity)

            with st.container():
                col1, col2, col3, col4 = st.columns([2, 2, 2, 3])
                with col1:
                    st.markdown(f"{icon} **{row['city']}**")
                with col2:
                    st.markdown(f"<span style='color:{color};font-weight:bold'>{severity}</span>", unsafe_allow_html=True)
                with col3:
                    st.metric("Alerts", int(row["alert_count"]))
                with col4:
                    if row["last_notification_at"]:
                        ts = pd.to_datetime(row["last_notification_at"], utc=True)
                        st.caption(f"Last alert: {ts.strftime('%d %b, %H:%M')}")
                    else:
                        st.caption("No alerts yet")

                if row["escalation_count"] > 0 or row["recovery_count"] > 0:
                    st.caption(
                        f"Escalations: {row['escalation_count']} | "
                        f"Recoveries: {row['recovery_count']}"
                    )
    else:
        st.info("No alert state data available yet.")

except Exception as error:
    st.warning(f"Could not load alert state: {error}")


st.divider()
st.header("⚠️ Recent Anomaly Detections")

try:
    if not anomalies.empty:
        anomaly_display = anomalies[anomalies["city"].isin(selected_cities)].head(10)

        for _, row in anomaly_display.iterrows():
            is_anomaly = row["is_anomaly"]
            bg_color = "#fef2f2" if is_anomaly else "#f0fdf4"
            border_color = "#dc2626" if is_anomaly else "#16a34a"

            with st.container():
                st.markdown(
                    f"""
                    <div style="
                        background: {bg_color};
                        border-left: 3px solid {border_color};
                        padding: 10px 14px;
                        border-radius: 6px;
                        margin-bottom: 8px;
                        font-size: 14px;
                    ">
                        <strong>{row['city']}</strong> | PM2.5: {row['pm25']:.1f} µg/m³ |
                        Baseline mean: {row['baseline_mean']:.1f} |
                        Z-score: {row['z_score']:.2f} |
                        Samples: {row['baseline_sample_count']}
                        <br>
                        <span style="color: {'#dc2626' if is_anomaly else '#16a34a'}; font-weight: bold;">
                            {'🚨 ANOMALY DETECTED' if is_anomaly else '✓ Normal variation'}
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        anomaly_only = anomalies[anomalies["is_anomaly"] == True]
        if not anomaly_only.empty:
            st.info(
                f"📊 {len(anomaly_only)} anomalous reading(s) detected. "
                "Anomalies are statistical signals, not confirmed health emergencies."
            )
    else:
        st.info("No anomaly data available yet.")

except Exception as error:
    st.warning(f"Could not load anomalies: {error}")


st.divider()
st.header("📈 Historical Analytics")

st.subheader("Daily PM2.5 Trend")
daily = (
    filtered.groupby(["date", "city"], as_index=False)
    .agg(
        average_pm25=("pm25", "mean"),
        maximum_pm25=("pm25", "max"),
        readings=("pm25", "count")
    )
)

fig = px.line(
    daily,
    x="date",
    y="average_pm25",
    color="city",
    markers=True,
    labels={
        "date": "Date",
        "average_pm25": "Average PM2.5 (µg/m³)",
        "city": "City"
    }
)
fig.add_hline(y=HIGH_THRESHOLD, line_dash="dash", line_color="orange",
              annotation_text="HIGH threshold")
fig.add_hline(y=CRITICAL_THRESHOLD, line_dash="dash", line_color="red",
              annotation_text="CRITICAL threshold")
st.plotly_chart(fig, use_container_width=True)

st.subheader("City-wise Pollution Summary")
summary = (
    filtered.groupby("city", as_index=False)
    .agg(
        average_pm25=("pm25", "mean"),
        maximum_pm25=("pm25", "max"),
        minimum_pm25=("pm25", "min"),
        average_temperature=("temperature", "mean"),
        total_readings=("id", "count"),
        high_readings=("status", lambda s: (s == "HIGH").sum()),
        critical_readings=("pm25", lambda x: (x >= CRITICAL_THRESHOLD).sum())
    )
)

st.dataframe(
    summary.round(2),
    use_container_width=True,
    hide_index=True
)

st.subheader("Hourly PM2.5 Trend")
filtered["hour"] = filtered["recorded_at_local"].dt.floor("h")
hourly = (
    filtered.groupby(["hour", "city"], as_index=False)
    .agg(average_pm25=("pm25", "mean"))
)

fig = px.line(
    hourly,
    x="hour",
    y="average_pm25",
    color="city",
    markers=True,
    labels={
        "hour": "Time",
        "average_pm25": "Average PM2.5 (µg/m³)",
        "city": "City"
    }
)
st.plotly_chart(fig, use_container_width=True)


st.subheader("Export Historical Data")
csv_data = filtered.drop(columns=["date", "hour", "recorded_at_local"], errors="ignore").to_csv(index=False)

st.download_button(
    "Download Filtered Readings (CSV)",
    data=csv_data,
    file_name=f"citypulse_pollution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv"
)

st.divider()
st.caption(
    f"Dashboard refreshes every 10 seconds. "
    f"Severity thresholds: HIGH ≥ {HIGH_THRESHOLD}, CRITICAL ≥ {CRITICAL_THRESHOLD}"
)
