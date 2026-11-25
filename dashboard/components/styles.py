"""
CityPulse — Living Atmosphere Styles & Glassmorphic Design System

Injects modern custom CSS for:
- Glassmorphism cards with backdrop blur and cyan/amber/crimson glowing accents.
- Responsive container layouts.
- Micro-animations: hover lift, pulse glows on CRITICAL/HIGH, smooth transitions.
- Hiding Streamlit dev toolbar and deploy button chrome (B12).
"""

import streamlit as st


def inject_custom_styles():
    """Inject custom styles into the Streamlit app."""
    css = """
    <style>
    /* Google Fonts Import */
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

    /* Global Typography */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    h1, h2, h3, h4 {
        font-family: 'Space Grotesk', -apple-system, sans-serif !important;
        letter-spacing: -0.02em !important;
        font-weight: 600 !important;
    }

    /* Numbers & Metrics in JetBrains Mono */
    div[data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-weight: 600 !important;
        letter-spacing: -0.03em !important;
    }

    /* Glassmorphism Containers & Cards */
    div[data-testid="stMetric"],
    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        background: rgba(11, 21, 40, 0.72) !important;
        backdrop-filter: blur(14px) !important;
        -webkit-backdrop-filter: blur(14px) !important;
        border: 1px solid rgba(148, 163, 184, 0.14) !important;
        border-radius: 12px !important;
        box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.4) !important;
        transition: transform 0.22s ease, border-color 0.22s ease, box-shadow 0.22s ease !important;
    }

    div[data-testid="stMetric"]:hover,
    div[data-testid="stVerticalBlockBorderWrapper"] > div:hover {
        transform: translateY(-2px);
        border-color: rgba(45, 212, 191, 0.35) !important;
        box-shadow: 0 8px 28px -2px rgba(45, 212, 191, 0.12) !important;
    }

    /* Sidebar Glassmorphism */
    section[data-testid="stSidebar"] {
        background-color: rgba(5, 11, 24, 0.88) !important;
        backdrop-filter: blur(16px) !important;
        -webkit-backdrop-filter: blur(16px) !important;
        border-right: 1px solid rgba(148, 163, 184, 0.12) !important;
    }

    /* Hide Streamlit Header Deploy Button & Toolbar (B12) */
    header[data-testid="stHeader"] {
        background: transparent !important;
    }
    .stDeployButton,
    div[data-testid="stToolbar"],
    #MainMenu,
    footer {
        display: none !important;
        visibility: hidden !important;
    }

    /* Modern Pill Controls */
    div[data-testid="stPills"] button {
        border-radius: 20px !important;
        font-family: 'Space Grotesk', sans-serif !important;
        font-size: 13px !important;
        transition: all 0.2s ease !important;
    }

    /* Pulsing Pill for Critical Status */
    @keyframes pulseCritical {
        0% { box-shadow: 0 0 0 0 rgba(244, 63, 94, 0.5); }
        70% { box-shadow: 0 0 0 8px rgba(244, 63, 94, 0); }
        100% { box-shadow: 0 0 0 0 rgba(244, 63, 94, 0); }
    }

    span[data-testid="stBadge"]:has(span:contains("CRITICAL")),
    span[data-testid="stBadge"]:has(span:contains("Critical")) {
        animation: pulseCritical 2s infinite !important;
    }
    </style>
    """
    st.html(css)
