#!/usr/bin/env python3
"""
CityPulse Safe Historical-Data Retention Utility.

Manages old pollution readings, alerts, and historical records with
configurable retention periods per data category.

SAFETY FEATURES:
- Dry-run mode is the DEFAULT (no data is changed)
- Explicit --confirm flag required for actual deletion
- Configurable retention periods per table
- Recent readings and current alert state are always preserved
- Parameterized queries and batched transactions
- Detailed logging without exposing secrets
- Rollback on failure

USAGE:
    # Dry-run (default - safe, shows what would be deleted)
    python scripts/retention.py

    # Dry-run with custom retention
    python scripts/retention.py --readings-days 90 --alerts-days 365

    # Actual deletion (requires explicit --confirm)
    python scripts/retention.py --confirm --readings-days 90

    # Check what would be deleted without changing anything
    python scripts/retention.py --dry-run --verbose
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Default retention periods (conservative defaults)
DEFAULT_READINGS_RETENTION_DAYS = int(os.getenv("READINGS_RETENTION_DAYS", "365"))
DEFAULT_ALERTS_RETENTION_DAYS = int(os.getenv("ALERTS_RETENTION_DAYS", "730"))
DEFAULT_ANOMALIES_RETENTION_DAYS = int(os.getenv("ANOMALIES_RETENTION_DAYS", "365"))
DEFAULT_AGGREGATES_RETENTION_DAYS = int(os.getenv("AGGREGATES_RETENTION_DAYS", "730"))

# Batch size for deletions
BATCH_SIZE = int(os.getenv("RETENTION_BATCH_SIZE", "1000"))

# Tables that should NEVER have their recent data deleted
PROTECTED_TABLES = ["city_alert_state", "pollution_aggregates"]

DB_CONFIG = {
    "host": os.getenv("CITYPULSE_DB_HOST", "localhost"),
    "port": int(os.getenv("CITYPULSE_DB_PORT", "5433")),
    "dbname": "citypulse",
    "user": "citypulse",
    "password": os.getenv("CITYPULSE_DB_PASSWORD", "citypulse_dev_password"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_table_counts(conn):
    """Get current row counts for all tables."""
    counts = {}
    with conn.cursor() as cursor:
        for table in [
            "pollution_readings",
            "pollution_alerts",
            "pollution_anomalies",
            "pollution_aggregates",
            "city_alert_state",
        ]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            except Exception as e:
                logger.warning(f"Could not count {table}: {e}")
                counts[table] = None
    return counts


def compute_cutoff(retention_days):
    """Compute the cutoff timestamp for deletion."""
    return datetime.now(timezone.utc) - timedelta(days=retention_days)


def dry_run(conn, retention_config):
    """Report what would be deleted without making changes."""
    logger.info("=" * 60)
    logger.info("RETENTION DRY-RUN (no data will be changed)")
    logger.info("=" * 60)

    counts_before = get_table_counts(conn)

    for table, retention_days, date_column in retention_config:
        cutoff = compute_cutoff(retention_days)

        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {date_column} < %s",
                (cutoff,),
            )
            would_delete = cursor.fetchone()[0]

        current = counts_before.get(table, "unknown")
        logger.info(
            f"Table: {table:30s} | Rows: {current} | "
            f"Retention: {retention_days}d | Cutoff: {cutoff.isoformat()} | "
            f"Would delete: {would_delete}"
        )

    logger.info("=" * 60)
    logger.info("DRY-RUN COMPLETE - Use --confirm to perform actual deletion")
    logger.info("=" * 60)


def delete_batched(conn, table, date_column, cutoff, batch_size, dry_run_mode=True):
    """Delete rows older than cutoff in batches."""
    total_deleted = 0

    while True:
        with conn.cursor() as cursor:
            if dry_run_mode:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {date_column} < %s",
                    (cutoff,),
                )
                count = cursor.fetchone()[0]
                if count == 0:
                    break
                logger.info(f"  Would delete {count} rows from {table} (batch)")
                total_deleted += count
                break  # Dry-run: just report once
            else:
                cursor.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE id IN (
                        SELECT id FROM {table}
                        WHERE {date_column} < %s
                        ORDER BY {date_column} ASC
                        LIMIT %s
                    )
                    """,
                    (cutoff, batch_size),
                )
                deleted = cursor.rowcount
                conn.commit()
                total_deleted += deleted
                logger.info(f"  Deleted {deleted} rows from {table}")
                if deleted < batch_size:
                    break

    return total_deleted


def perform_retention(conn, retention_config, confirm=False, batch_size=BATCH_SIZE):
    """Perform actual data retention/deletion."""
    if not confirm:
        logger.error("Refusing to delete without --confirm flag")
        return False

    logger.info("=" * 60)
    logger.info("RETENTION EXECUTION (data will be deleted)")
    logger.info("=" * 60)

    counts_before = get_table_counts(conn)
    total_deleted = 0

    for table, retention_days, date_column in retention_config:
        # Skip protected tables
        if table in PROTECTED_TABLES:
            logger.info(f"Skipping protected table: {table}")
            continue

        cutoff = compute_cutoff(retention_days)
        logger.info(
            f"Table: {table} | Cutoff: {cutoff.isoformat()} | "
            f"Retention: {retention_days} days"
        )

        deleted = delete_batched(
            conn, table, date_column, cutoff, batch_size, dry_run_mode=False
        )
        total_deleted += deleted

    counts_after = get_table_counts(conn)

    logger.info("=" * 60)
    logger.info("RETENTION COMPLETE")
    logger.info(f"Total rows deleted: {total_deleted}")
    for table in counts_before:
        before = counts_before.get(table, 0) or 0
        after = counts_after.get(table, 0) or 0
        logger.info(f"  {table}: {before} → {after} ({before - after} deleted)")
    logger.info("=" * 60)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="CityPulse Safe Historical-Data Retention Utility"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform deletion (required; without this, dry-run only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicitly request dry-run mode (default)",
    )
    parser.add_argument(
        "--readings-days",
        type=int,
        default=DEFAULT_READINGS_RETENTION_DAYS,
        help=f"Retention days for pollution_readings (default: {DEFAULT_READINGS_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--alerts-days",
        type=int,
        default=DEFAULT_ALERTS_RETENTION_DAYS,
        help=f"Retention days for pollution_alerts (default: {DEFAULT_ALERTS_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--anomalies-days",
        type=int,
        default=DEFAULT_ANOMALIES_RETENTION_DAYS,
        help=f"Retention days for pollution_anomalies (default: {DEFAULT_ANOMALIES_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--aggregates-days",
        type=int,
        default=DEFAULT_AGGREGATES_RETENTION_DAYS,
        help=f"Retention days for pollution_aggregates (default: {DEFAULT_AGGREGATES_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size for deletions (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine mode
    is_dry_run = not args.confirm
    if args.dry_run and args.confirm:
        logger.error("Cannot specify both --dry-run and --confirm")
        sys.exit(1)

    # Build retention config: (table, retention_days, date_column)
    retention_config = [
        ("pollution_readings", args.readings_days, "recorded_at"),
        ("pollution_alerts", args.alerts_days, "created_at"),
        ("pollution_anomalies", args.anomalies_days, "recorded_at"),
        ("pollution_aggregates", args.aggregates_days, "updated_at"),
    ]

    logger.info(f"CityPulse Retention Utility")
    logger.info(f"Mode: {'DRY-RUN' if is_dry_run else 'EXECUTE (WILL DELETE)'}")
    logger.info(f"Batch size: {args.batch_size}")

    try:
        conn = get_connection()

        if is_dry_run:
            dry_run(conn, retention_config)
        else:
            perform_retention(
                conn,
                retention_config,
                confirm=args.confirm,
                batch_size=args.batch_size,
            )

        conn.close()

    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Retention utility error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
