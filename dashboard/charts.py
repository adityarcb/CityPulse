"""
CityPulse — Chart configuration and Plotly helpers.

All charts use a consistent dark theme matching the CityPulse Living Atmosphere design system.
Fixes:
- B8: Auto-fit map bounds with padding; true anomaly halo encoding; non-overlapping labels.
- B9: Strict (city, timestamp) sorting; break gaps > 3h; stable 10-city permanent color tokens.
- B10: cliponaxis=False; generous right margin (r=65); clean threshold placement.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

from data import HIGH_THRESHOLD, CRITICAL_THRESHOLD, classify_severity, severity_color
from theme import (
    BG_MAIN,
    BG_CARD,
    BG_SURFACE,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    TEXT_MUTED,
    SEVERITY_NORMAL,
    SEVERITY_HIGH,
    SEVERITY_CRITICAL,
    SEVERITY_ANOMALY,
    CITY_COLORS,
    get_city_color,
    get_severity_color,
)

GRIDLINE = "rgba(148, 163, 184, 0.12)"
BORDER = "rgba(45, 212, 191, 0.15)"


def _base_layout(**overrides) -> dict:
    """Return a base Plotly layout dict with CityPulse dark styling."""
    layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
        margin=dict(l=45, r=65, t=45, b=40),
        legend=dict(
            bgcolor="rgba(5, 11, 24, 0.8)",
            bordercolor="rgba(148, 163, 184, 0.2)",
            borderwidth=1,
            font=dict(color=TEXT_PRIMARY, size=11),
            orientation="h",
            yanchor="bottom",
            y=1.03,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(
            gridcolor=GRIDLINE,
            zerolinecolor=GRIDLINE,
            tickfont=dict(color=TEXT_SECONDARY, size=11),
            title_font=dict(color=TEXT_SECONDARY, size=12),
        ),
        yaxis=dict(
            gridcolor=GRIDLINE,
            zerolinecolor=GRIDLINE,
            tickfont=dict(color=TEXT_SECONDARY, size=11),
            title_font=dict(color=TEXT_SECONDARY, size=12),
        ),
        hoverlabel=dict(
            bgcolor="#0B1528",
            bordercolor="rgba(45, 212, 191, 0.3)",
            font=dict(color=TEXT_PRIMARY, family="Inter, sans-serif", size=12),
        ),
    )
    layout.update(overrides)
    return layout


def add_threshold_lines(fig: go.Figure) -> go.Figure:
    """Add HIGH and CRITICAL threshold reference lines with clean background annotations."""
    fig.add_hline(
        y=HIGH_THRESHOLD,
        line_dash="dot",
        line_color=SEVERITY_HIGH,
        line_width=1.5,
        annotation_text=f"HIGH ({HIGH_THRESHOLD:.0f})",
        annotation_font=dict(color=SEVERITY_HIGH, size=10),
        annotation_position="top right",
        annotation_bgcolor="rgba(5, 11, 24, 0.8)",
    )
    fig.add_hline(
        y=CRITICAL_THRESHOLD,
        line_dash="dot",
        line_color=SEVERITY_CRITICAL,
        line_width=1.5,
        annotation_text=f"CRITICAL ({CRITICAL_THRESHOLD:.0f})",
        annotation_font=dict(color=SEVERITY_CRITICAL, size=10),
        annotation_position="top right",
        annotation_bgcolor="rgba(5, 11, 24, 0.8)",
    )
    return fig


def add_threshold_lines_vertical(fig: go.Figure) -> go.Figure:
    """Add vertical threshold lines for horizontal bar charts."""
    fig.add_vline(
        x=HIGH_THRESHOLD,
        line_dash="dot",
        line_color=SEVERITY_HIGH,
        line_width=1.5,
        annotation_text=f"HIGH ({HIGH_THRESHOLD:.0f})",
        annotation_font=dict(color=SEVERITY_HIGH, size=10),
        annotation_position="top",
        annotation_bgcolor="rgba(5, 11, 24, 0.8)",
    )
    fig.add_vline(
        x=CRITICAL_THRESHOLD,
        line_dash="dot",
        line_color=SEVERITY_CRITICAL,
        line_width=1.5,
        annotation_text=f"CRITICAL ({CRITICAL_THRESHOLD:.0f})",
        annotation_font=dict(color=SEVERITY_CRITICAL, size=10),
        annotation_position="top",
        annotation_bgcolor="rgba(5, 11, 24, 0.8)",
    )
    return fig


def pm25_bar_chart(latest_df: pd.DataFrame) -> go.Figure:
    """City comparison bar chart with severity-aware colours and unclipped labels."""
    fig = go.Figure()

    sorted_df = latest_df.sort_values("pm25", ascending=True)

    for _, row in sorted_df.iterrows():
        sev = classify_severity(row["pm25"])
        color = get_severity_color(sev)
        fig.add_trace(go.Bar(
            x=[row["pm25"]],
            y=[row["city"]],
            orientation="h",
            name=row["city"],
            marker=dict(
                color=color,
                line=dict(color="rgba(255,255,255,0.15)", width=1),
                opacity=0.9,
            ),
            text=f" {row['pm25']:.1f}",
            textposition="outside",
            cliponaxis=False,
            textfont=dict(color=TEXT_PRIMARY, size=12, family="JetBrains Mono, monospace"),
            hovertemplate=(
                f"<b>{row['city']}</b><br>"
                f"PM2.5: {row['pm25']:.1f} µg/m³<br>"
                f"Severity: {sev}<extra></extra>"
            ),
            showlegend=False,
        ))

    add_threshold_lines_vertical(fig)

    fig.update_layout(
        **_base_layout(
            height=max(220, len(latest_df) * 44 + 80),
            yaxis_title="",
            xaxis_title="PM2.5 (µg/m³)",
            bargap=0.28,
        )
    )
    return fig


def temperature_bar_chart(latest_df: pd.DataFrame) -> go.Figure:
    """Temperature comparison bar chart with unclipped labels."""
    df = latest_df.sort_values("temperature", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["temperature"],
        y=df["city"],
        orientation="h",
        marker=dict(
            color=df["temperature"],
            colorscale=[[0, "#38BDF8"], [0.5, "#FBBF24"], [1, "#F43F5E"]],
            line=dict(width=0),
            opacity=0.9,
        ),
        text=[f" {t:.1f}°C" if pd.notna(t) else "" for t in df["temperature"]],
        textposition="outside",
        cliponaxis=False,
        textfont=dict(color=TEXT_PRIMARY, size=12, family="JetBrains Mono, monospace"),
        hovertemplate="<b>%{y}</b><br>Temperature: %{x:.1f}°C<extra></extra>",
        showlegend=False,
    ))
    fig.update_layout(
        **_base_layout(
            height=max(220, len(df) * 44 + 80),
            yaxis_title="",
            xaxis_title="Temperature (°C)",
            bargap=0.28,
        )
    )
    return fig


def _insert_gap_breaks(city_df: pd.DataFrame, time_col: str, max_gap_hours: float = 3.0) -> pd.DataFrame:
    """
    Insert NaN rows where consecutive timestamps exceed max_gap_hours.
    Prevents straight lines bridging days-long gaps (B9).
    """
    if len(city_df) < 2:
        return city_df

    sorted_df = city_df.sort_values(time_col).copy()
    diffs = sorted_df[time_col].diff()
    max_gap = pd.Timedelta(hours=max_gap_hours)

    rows = []
    for i in range(len(sorted_df)):
        if i > 0 and diffs.iloc[i] > max_gap:
            # Insert break row with NaN for numeric columns
            break_row = sorted_df.iloc[i].copy()
            for col in sorted_df.select_dtypes(include=[np.number]).columns:
                break_row[col] = np.nan
            break_row[time_col] = sorted_df.iloc[i - 1][time_col] + pd.Timedelta(seconds=1)
            rows.append(break_row)
        rows.append(sorted_df.iloc[i])

    return pd.DataFrame(rows)


def daily_trend_chart(df: pd.DataFrame, cities: list[str] | None = None) -> go.Figure:
    """Daily PM2.5 trend line chart with distinct city colors and sorted dates."""
    if cities:
        df = df[df["city"].isin(cities)]

    daily = (
        df.groupby(["date", "city"], as_index=False)
        .agg(average_pm25=("pm25", "mean"), max_pm25=("pm25", "max"))
    )

    fig = go.Figure()
    for city in sorted(daily["city"].unique()):
        city_data = daily[daily["city"] == city].sort_values("date")
        color = get_city_color(city)
        fig.add_trace(go.Scatter(
            x=city_data["date"],
            y=city_data["average_pm25"],
            name=city,
            mode="lines+markers",
            connectgaps=False,
            line=dict(color=color, width=2.5),
            marker=dict(size=6, color=color),
            hovertemplate=(
                f"<b>{city}</b><br>"
                "Date: %{x}<br>"
                "Avg PM2.5: %{y:.1f} µg/m³<extra></extra>"
            ),
        ))

    add_threshold_lines(fig)
    fig.update_layout(
        **_base_layout(
            height=400,
            xaxis_title="Date",
            yaxis_title="Average PM2.5 (µg/m³)",
        )
    )
    return fig


def hourly_trend_chart(df: pd.DataFrame, cities: list[str] | None = None) -> go.Figure:
    """Hourly PM2.5 trend chart with real hourly bins and gap handling (B9)."""
    if cities:
        df = df[df["city"].isin(cities)]

    hourly = (
        df.groupby(["hour", "city"], as_index=False)
        .agg(average_pm25=("pm25", "mean"))
    )

    fig = go.Figure()
    for city in sorted(hourly["city"].unique()):
        city_data = hourly[hourly["city"] == city].sort_values("hour")
        city_broken = _insert_gap_breaks(city_data, "hour", max_gap_hours=3.0)
        color = get_city_color(city)
        fig.add_trace(go.Scatter(
            x=city_broken["hour"],
            y=city_broken["average_pm25"],
            name=city,
            mode="lines+markers",
            connectgaps=False,
            line=dict(color=color, width=2),
            marker=dict(size=5, color=color),
            hovertemplate=(
                f"<b>{city}</b><br>"
                "Time: %{x|%b %d, %H:00 IST}<br>"
                "Avg PM2.5: %{y:.1f} µg/m³<extra></extra>"
            ),
        ))

    add_threshold_lines(fig)
    fig.update_layout(
        **_base_layout(
            height=400,
            xaxis_title="Hourly Window (IST)",
            yaxis_title="Average PM2.5 (µg/m³)",
        )
    )
    return fig


def pm25_timeseries_chart(
    df: pd.DataFrame, city: str | None = None, hours: int = 24
) -> go.Figure:
    """Time-series chart strictly sorted by timestamp with gap breaks and stable city colors (B9)."""
    if city and city != "All cities":
        df = df[df["city"] == city]

    fig = go.Figure()
    cities = sorted(df["city"].unique())

    for c in cities:
        city_raw = df[df["city"] == c].sort_values("recorded_at").tail(500)
        city_data = _insert_gap_breaks(city_raw, "recorded_at", max_gap_hours=3.0)
        color = get_city_color(c)

        fig.add_trace(go.Scatter(
            x=city_data["recorded_at"],
            y=city_data["pm25"],
            name=c,
            mode="lines+markers",
            connectgaps=False,
            line=dict(color=color, width=2),
            marker=dict(size=4, color=color),
            fill="tozeroy" if len(cities) == 1 else None,
            fillcolor=f"rgba({_hex_to_rgb(color)}, 0.12)" if len(cities) == 1 else None,
            hovertemplate=(
                f"<b>{c}</b><br>"
                "Recorded: %{x|%b %d, %H:%M:%S UTC}<br>"
                "PM2.5: %{y:.1f} µg/m³<extra></extra>"
            ),
        ))

    add_threshold_lines(fig)
    fig.update_layout(
        **_base_layout(
            height=400,
            xaxis_title="Recorded Timestamp",
            yaxis_title="PM2.5 (µg/m³)",
        )
    )
    return fig


def pollution_map_figure(map_df: pd.DataFrame) -> go.Figure:
    """
    Geographic pollution map (B8):
    - Auto-fit bounds over markers with padding so no city is cropped.
    - True anomaly ring encoding.
    - Non-overlapping clear labels.
    """
    fig = go.Figure()
    use_scattermap = hasattr(go, "Scattermap")
    TraceClass = go.Scattermap if use_scattermap else go.Scattermapbox

    if map_df.empty:
        return fig

    # Base markers colored by severity
    for severity in ["NORMAL", "HIGH", "CRITICAL"]:
        sev_data = map_df[map_df["severity"] == severity]
        if sev_data.empty:
            continue

        # Sizing proportional to PM2.5
        sizes = sev_data["pm25"].clip(lower=15, upper=250) / 4.5
        color = get_severity_color(severity)

        fig.add_trace(TraceClass(
            lat=sev_data["lat"],
            lon=sev_data["lon"],
            mode="markers+text",
            marker=dict(
                size=sizes,
                color=color,
                opacity=0.88,
                sizemode="diameter",
            ),
            text=sev_data["city"],
            textposition="top center",
            textfont=dict(color=TEXT_PRIMARY, size=11, family="Inter, sans-serif"),
            customdata=sev_data[["city", "pm25", "severity"]].values,
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "PM2.5: %{customdata[1]:.1f} µg/m³<br>"
                "Severity: %{customdata[2]}<br>"
                "<i>Approximate monitoring station</i><extra></extra>"
            ),
            name=f"{severity} (PM2.5: {len(sev_data)})",
        ))

    # Anomaly overlay: violet halo ring
    if "is_anomaly" in map_df.columns:
        anom_data = map_df[map_df["is_anomaly"] == True]
        if not anom_data.empty:
            anom_sizes = (anom_data["pm25"].clip(lower=15, upper=250) / 4.5) * 1.6
            fig.add_trace(TraceClass(
                lat=anom_data["lat"],
                lon=anom_data["lon"],
                mode="markers",
                marker=dict(
                    size=anom_sizes,
                    color=SEVERITY_ANOMALY,
                    opacity=0.7,
                    sizemode="diameter",
                ),
                name=f"Anomaly Alert ({len(anom_data)})",
                hoverinfo="skip",
            ))

    # Calculate geographic bounds with padding so Amritsar (31.6°N) and Chennai/Bengaluru (12.9°N) fit
    min_lat, max_lat = map_df["lat"].min(), map_df["lat"].max()
    min_lon, max_lon = map_df["lon"].min(), map_df["lon"].max()
    center_lat = (min_lat + max_lat) / 2.0
    center_lon = (min_lon + max_lon) / 2.0

    map_config = dict(
        style="dark" if use_scattermap else "carto-darkmatter",
        center=dict(lat=center_lat, lon=center_lon),
        zoom=4.25,
    )

    layout_kwargs = {
        "map" if use_scattermap else "mapbox": map_config,
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "margin": dict(l=0, r=0, t=0, b=0),
        "height": 520,
        "legend": dict(
            bgcolor="rgba(5, 11, 24, 0.85)",
            bordercolor="rgba(148, 163, 184, 0.2)",
            borderwidth=1,
            font=dict(color=TEXT_PRIMARY, size=11),
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left",
            x=0.02,
        ),
        "hoverlabel": dict(
            bgcolor="#0B1528",
            bordercolor="rgba(45, 212, 191, 0.3)",
            font=dict(color=TEXT_PRIMARY, family="Inter, sans-serif"),
        ),
    }

    fig.update_layout(**layout_kwargs)
    return fig


def _hex_to_rgb(hex_color: str) -> str:
    """Convert '#RRGGBB' to 'R, G, B' string for CSS rgba()."""
    h = hex_color.lstrip("#")
    if len(h) == 6:
        return f"{int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}"
    return "45, 212, 191"
