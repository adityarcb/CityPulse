#!/usr/bin/env bash
# ==============================================================================
# CityPulse — Real-Time Indian Air Quality Ingestion Runner
#
# Usage:
#   ./scripts/fetch_live_data.sh           # Fetch 1 live batch & send to Kafka
#   ./scripts/fetch_live_data.sh --stream  # Continuously stream every 5 minutes
#   ./scripts/fetch_live_data.sh --dry-run # View live data without publishing
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$DIR/dashboard/.venv/bin/python"

if [ ! -f "$PYTHON" ]; then
    echo "Error: Virtualenv python not found at $PYTHON" >&2
    exit 1
fi

if [ "$1" == "--stream" ]; then
    echo "Starting continuous live stream across Indian cities (every 5 mins)..."
    exec "$PYTHON" "$DIR/scripts/live_cpcb_producer.py" --interval 300
elif [ "$1" == "--dry-run" ]; then
    echo "Testing live Indian air quality fetch (dry-run)..."
    exec "$PYTHON" "$DIR/scripts/live_cpcb_producer.py" --dry-run
else
    echo "Fetching real-time Indian air quality snapshot and publishing to Kafka..."
    exec "$PYTHON" "$DIR/scripts/live_cpcb_producer.py" --once
fi
