"""
CityPulse — Unified Design System & Severity Token Map ("Living Atmosphere")

Central design tokens for colors, typography, glassmorphism, and status badges.
Used across all dashboard pages to guarantee 100% aesthetic and semantic consistency.
"""

# ── Core Palette ──────────────────────────────────────────────────────────────
BG_MAIN = "#050B18"           # Deep Navy space
BG_SURFACE = "#0B1528"        # Elevated surface
BG_CARD = "rgba(15, 27, 48, 0.7)"  # Glass card fill
BORDER_SUBTLE = "rgba(45, 212, 191, 0.15)"
BORDER_CARD = "rgba(148, 163, 184, 0.12)"

TEXT_PRIMARY = "#F8FAFC"
TEXT_SECONDARY = "#94A3B8"
TEXT_MUTED = "#64748B"

# ── Semantic Severity Tokens (B6 & Part 2) ────────────────────────────────────
SEVERITY_NORMAL = "#2DD4BF"       # Vibrant Teal
SEVERITY_HIGH = "#F59E0B"         # Warm Amber
SEVERITY_CRITICAL = "#F43F5E"     # Vivid Crimson
SEVERITY_ANOMALY = "#8B5CF6"      # Electric Violet
SEVERITY_INSUFFICIENT = "#64748B" # Neutral Slate
SEVERITY_STALE = "#94A3B8"        # Muted Gray

SEVERITY_COLORS = {
    "NORMAL": SEVERITY_NORMAL,
    "HIGH": SEVERITY_HIGH,
    "CRITICAL": SEVERITY_CRITICAL,
    "ANOMALY": SEVERITY_ANOMALY,
    "INSUFFICIENT_DATA": SEVERITY_INSUFFICIENT,
    "STALE": SEVERITY_STALE,
}

SEVERITY_ICONS = {
    "NORMAL": ":material/check_circle:",
    "HIGH": ":material/warning:",
    "CRITICAL": ":material/emergency:",
    "ANOMALY": ":material/query_stats:",
    "INSUFFICIENT_DATA": ":material/hourglass_top:",
    "STALE": ":material/history:",
}

# Streamlit badge color tokens (green, orange, red, violet, gray)
SEVERITY_BADGE_COLORS = {
    "NORMAL": "green",
    "HIGH": "orange",
    "CRITICAL": "red",
    "ANOMALY": "violet",
    "INSUFFICIENT_DATA": "gray",
    "STALE": "gray",
}

# ── 10 Canonical City Distinct Colors (B9) ───────────────────────────────────
# Stable, high-contrast, permanent color mapping per city
CITY_COLORS = {
    "Delhi": "#F43F5E",       # Crimson (traditionally higher PM2.5)
    "Kolkata": "#F97316",     # Vibrant Orange
    "Mumbai": "#0284C7",      # Ocean Blue
    "Bengaluru": "#10B981",   # Emerald Teal
    "Chennai": "#FBBF24",     # Golden Amber
    "Hyderabad": "#8B5CF6",   # Royal Violet
    "Pune": "#06B6D4",        # Cyan
    "Amritsar": "#EC4899",    # Magenta / Rose
    "Jalandhar": "#3B82F6",   # Sapphire Blue
    "Ludhiana": "#D946EF",    # Electric Fuchsia
}


def get_city_color(city: str) -> str:
    """Return stable color token for a canonical city."""
    canonical = "Bengaluru" if city == "Bangalore" else city
    return CITY_COLORS.get(canonical, "#38BDF8")


def get_severity_color(severity: str) -> str:
    """Return hex color for a severity token."""
    return SEVERITY_COLORS.get(severity.upper(), "#94A3B8")


def get_severity_badge_color(severity: str) -> str:
    """Return Streamlit badge color string."""
    return SEVERITY_BADGE_COLORS.get(severity.upper(), "gray")


def get_severity_icon(severity: str) -> str:
    """Return Material Symbol icon for severity."""
    return SEVERITY_ICONS.get(severity.upper(), ":material/help:")
