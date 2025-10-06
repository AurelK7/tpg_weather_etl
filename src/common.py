# common.py
import argparse
import os
import textwrap
from typing import Optional

import duckdb
from dotenv import load_dotenv

from utils_logging import setup_logging

# -----------------------------------------------------------------------------
# Global logging
# -----------------------------------------------------------------------------
log = setup_logging(name="fetch_gtfs", level="DEBUG", to_file=True, to_console=True)
log.info("Starting common module…")

# -----------------------------------------------------------------------------
# Load .env / environment variables
# -----------------------------------------------------------------------------
load_dotenv()

DEFAULT_DB: str = os.environ.get("DUCKDB_PATH", "data/warehouse.duckdb")
DUCKDB_THREADS: int = int(os.environ.get("DUCKDB_THREADS", "12"))
DUCKDB_MEM: str = os.environ.get("DUCKDB_MEM", "16GB")  # e.g. "4GB", "0" for unlimited
DUCKDB_CKPT: str = os.environ.get("DUCKDB_CHECKPOINT_THRESHOLD", "1GB")
DUCKDB_ENABLE_PROGRESS: str = os.environ.get("DUCKDB_ENABLE_PROGRESS", "false")
DUCKDB_TEMP_DIR: Optional[str] = os.environ.get("DUCKDB_TEMP_DIR")  # e.g. "/fast/tmp"


# -----------------------------------------------------------------------------
# Database connection
# -----------------------------------------------------------------------------
def get_db(conn_str: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """
    Open (or create) a DuckDB database and apply performance PRAGMAs.

    :param conn_str: Path to the .duckdb file (defaults to DEFAULT_DB)
    :return: Active DuckDB connection
    """
    path = conn_str or DEFAULT_DB
    try:
        # Ensure the parent directory exists
        db_dir = os.path.dirname(path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        log.info(f"Connecting to DuckDB at: {path}")
        con = duckdb.connect(path)

        # Apply performance PRAGMAs
        log.debug(f"Applying PRAGMA threads={DUCKDB_THREADS}, memory_limit={DUCKDB_MEM}")
        con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
        con.execute(f"PRAGMA memory_limit='{DUCKDB_MEM}';")
        # log.debug(f"PRAGMA memory_limit='{DUCKDB_MEM}'")

        # Faster bulk appends: don't preserve insertion order
        try:
            con.execute("PRAGMA preserve_insertion_order=false;")
            log.debug("PRAGMA preserve_insertion_order=false")
        except Exception as e:
            log.debug(f"preserve_insertion_order not supported on this version: {e}")

        # Make checkpoints less frequent during large transactions
        try:
            con.execute(f"PRAGMA checkpoint_threshold='{DUCKDB_CKPT}';")  # adjust via env if you like
            log.debug(f"PRAGMA checkpoint_threshold='{DUCKDB_CKPT}';")
        except Exception as e:
            log.debug(f"checkpoint_threshold not supported on this version: {e}")

        try:
            con.execute(f"PRAGMA enable_progress_bar={DUCKDB_ENABLE_PROGRESS};")
            log.debug(f"Setting enable_progress_bar to: {str(DUCKDB_ENABLE_PROGRESS).lower()}")
        except Exception as e:
            log.debug(f"enable_progress_bar not supported on this version: {e}")

        if DUCKDB_TEMP_DIR:
            log.debug(f"Setting temp_directory to: {DUCKDB_TEMP_DIR}")
            os.makedirs(DUCKDB_TEMP_DIR, exist_ok=True)
            con.execute(f"PRAGMA temp_directory='{DUCKDB_TEMP_DIR}';")

        return con
    except Exception as e:
        log.exception(f"Failed to connect to database at {path}: {e}")
        raise


# -----------------------------------------------------------------------------
# DDL schema
# -----------------------------------------------------------------------------
DDL = textwrap.dedent("""
-- GTFS
CREATE TABLE IF NOT EXISTS gtfs_stops (
  stop_id TEXT PRIMARY KEY,
  stop_name TEXT,
  lat DOUBLE,
  lon DOUBLE,
  zone_id TEXT,
  location_type TEXT,
  parent_station TEXT,
  platform_code TEXT,
  feed_version TEXT
);
CREATE TABLE IF NOT EXISTS gtfs_routes (
  route_id TEXT PRIMARY KEY,
  route_short_name TEXT,
  route_long_name TEXT,
  route_type INT,
  operator_name TEXT,
  feed_version TEXT
);
CREATE TABLE IF NOT EXISTS gtfs_trips (
  trip_id TEXT PRIMARY KEY,
  route_id TEXT REFERENCES gtfs_routes(route_id),
  service_id TEXT,
  direction_id INT,
  trip_headsign TEXT,
  feed_version TEXT
);
CREATE TABLE IF NOT EXISTS gtfs_stop_times (
  trip_id TEXT REFERENCES gtfs_trips(trip_id),
  stop_sequence INT,
  stop_id TEXT REFERENCES gtfs_stops(stop_id),
  arrival_time_planned TEXT,
  departure_time_planned TEXT,
  feed_version TEXT,
  PRIMARY KEY (trip_id, stop_sequence)
);

-- ISTDATEN
CREATE TABLE IF NOT EXISTS ist_events (
  ist_event_id BIGINT,
  service_date DATE,
  fahrt_bezeichner TEXT,
  operator_abbr TEXT,
  product_id TEXT,
  line_text TEXT,
  stop_name TEXT,
  stop_code TEXT,
  arrival_sched_ts TIMESTAMP,
  arrival_est_ts   TIMESTAMP,
  arrival_status   TEXT,
  depart_sched_ts  TIMESTAMP,
  depart_est_ts    TIMESTAMP,
  depart_status    TEXT,
  pass_through     BOOLEAN,
  is_extra_trip    BOOLEAN,
  is_cancelled     BOOLEAN
);
CREATE SEQUENCE IF NOT EXISTS ist_events_seq START 1;
CREATE INDEX IF NOT EXISTS idx_ist_1 ON ist_events(service_date);
CREATE INDEX IF NOT EXISTS idx_ist_2 ON ist_events(operator_abbr, product_id, line_text);

-- WEATHER
CREATE TABLE IF NOT EXISTS weather_obs (
  station_id TEXT,
  ts_utc TIMESTAMP,
  temp_c DOUBLE,
  rain_mm DOUBLE,
  wind_ms DOUBLE,
  gust_ms DOUBLE,
  wind_dir_deg DOUBLE,
  humidity DOUBLE,
  pressure_hpa DOUBLE,
  global_rad_wm2 DOUBLE,
  sunshine_min DOUBLE,
  dewpoint_c DOUBLE,
  PRIMARY KEY (station_id, ts_utc)
);

-- FEATURES (gold)
CREATE TABLE IF NOT EXISTS feature_training_row (
  row_id BIGINT,
  service_date DATE,
  route_id TEXT,
  line_text TEXT,
  stop_id TEXT,
  stop_name TEXT,
  ts_event TIMESTAMP,
  target_late2m_15 BOOLEAN,
  target_late2m_30 BOOLEAN,
  delay_depart_sec INT,
  med_delay_7d_sec INT,
  med_delay_14d_sec INT,
  med_delay_28d_sec INT,
  dow INT, hour INT, minute_bin INT, is_holiday BOOLEAN,
  sin_hour DOUBLE, cos_hour DOUBLE,
  temp_c DOUBLE, rain_mm DOUBLE, wind_ms DOUBLE, gust_ms DOUBLE,
  rain_mm_lag10 DOUBLE, rain_mm_lag20 DOUBLE, wind_ms_lag10 DOUBLE, wind_ms_lag20 DOUBLE
);
CREATE SEQUENCE IF NOT EXISTS feature_training_row_seq START 1;
""").strip()


# -----------------------------------------------------------------------------
# Analytical macros
# -----------------------------------------------------------------------------
def ensure_feature_macros(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create/replace analytical macros used in feature engineering.
    Idempotent: safe to re-run.
    """
    log.debug("Creating/refreshing analytical macros…")

    # Delay in minutes (+ = late, - = early)
    con.execute("""
    CREATE OR REPLACE MACRO delay_minutes(est_ts, sched_ts) AS (
        CASE
            WHEN est_ts IS NULL OR sched_ts IS NULL THEN NULL
            ELSE CAST(
                date_diff('minute', CAST(sched_ts AS TIMESTAMP), CAST(est_ts AS TIMESTAMP))
                AS DOUBLE
            )
        END
    );
    """)

    # Simple binning for rain, wind, and temperature
    con.execute("""
    CREATE OR REPLACE MACRO rain_bin(mm) AS (
        CASE
          WHEN mm IS NULL THEN NULL
          WHEN mm = 0 THEN 'no_rain'
          WHEN mm < 0.5 THEN 'drizzle'
          WHEN mm < 2   THEN 'light'
          WHEN mm < 5   THEN 'moderate'
          ELSE 'heavy'
        END
    );
    """)
    con.execute("""
    CREATE OR REPLACE MACRO wind_bin(ms) AS (
        CASE
          WHEN ms IS NULL THEN NULL
          WHEN ms < 4   THEN 'calm'
          WHEN ms < 8   THEN 'breeze'
          WHEN ms < 14  THEN 'moderate'
          WHEN ms < 21  THEN 'fresh'
          ELSE 'strong'
        END
    );
    """)
    con.execute("""
    CREATE OR REPLACE MACRO temp_bin(c) AS (
        CASE
          WHEN c IS NULL THEN NULL
          WHEN c < 0    THEN '<0'
          WHEN c < 10   THEN '0-10'
          WHEN c < 20   THEN '10-20'
          WHEN c < 30   THEN '20-30'
          ELSE '>=30'
        END
    );
    """)
    log.info("Macros available: delay_minutes, rain_bin, wind_bin, temp_bin.")


# -----------------------------------------------------------------------------
# Schema initialization
# -----------------------------------------------------------------------------
def init_db(path: Optional[str] = None) -> None:
    """
    Initialize schema (tables, indexes, sequences) and analytical macros.

    :param path: Path to the .duckdb file (defaults to DEFAULT_DB)
    """
    con = get_db(path)
    try:
        log.info("Initializing database schema…")
        stmts = [s.strip() for s in DDL.split(";\n") if s.strip()]
        for i, stmt in enumerate(stmts, start=1):
            sql = stmt + ";"
            log.debug(f"[DDL {i}/{len(stmts)}] {sql.splitlines()[0][:120]}...")
            con.execute(sql)
        ensure_feature_macros(con)
        log.info(f"Schema initialized at {path or DEFAULT_DB}")
        print("Initialized schema at", path or DEFAULT_DB)
    except Exception as e:
        log.exception(f"Schema initialization failed: {e}")
        raise


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Common utilities for DuckDB/GTFS project.")
    ap.add_argument("--init", action="store_true", help="Initialize DB schema (tables, indexes, macros).")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"Path to DuckDB file (default: {DEFAULT_DB})")
    return ap


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    if args.init:
        init_db(args.db)
    else:
        # If called without --init, just test connection
        con = get_db(args.db)
        log.info("Connected successfully (no action). Run with --init to create schema.")
