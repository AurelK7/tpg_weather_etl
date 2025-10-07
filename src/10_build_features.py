#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# src/10_build_features.pyt
"""
10_build_features.py — Event-level features (IstDaten × Weather), memory-safe & station-aware

Overview
--------
Builds the canonical event-level table `features_events` and exports it to Parquet.
This table is the "gold" input for EDA and ML training.

Key properties
--------------
- Station-aware weather join to avoid duplicates:
    1) If table `stop_to_station(stop_code, station_id)` exists → use it per stop.
    2) Else if `--station-id` is provided → use that single station.
    3) Else → fall back to the dominant station in `weather_obs` (warn).
- Strict 10-minute join by default: `weather_obs.ts_utc == sched_bin`.
  Optional ASOF mode (`--asof`) uses the nearest past observation, kept only if
  0 ≤ (sched_bin - weather_ts) ≤ 5 minutes.
- Preserves raw timestamps: arrival/depart × (sched, est).
- Explicit COALESCE flags to trace potential bias when depart_* was missing:
    * coalesce_sched_from_arrival
    * coalesce_est_from_arrival
    * any_coalesce_from_arrival
- Two delay targets:
    * delay_sec             = est_ts - sched_ts   (coalesced, wide coverage)
    * depart_only_delay_sec = depart_est - depart_sched (strict, unbiased, sparser)
- Memory-safe: CTAS inside DuckDB + COPY TO Parquet (no giant pandas DataFrame).

CLI
---
--db           : optional DuckDB path (else resolved by project helpers in common.py)
--log-level    : DEBUG | INFO | WARNING | ERROR  (default: INFO)
--asof         : enable nearest-past join (≤ 5 min) instead of strict equality
--station-id   : fallback single weather station if no mapping table is present

Outputs
-------
- DuckDB table:  features_events
- Parquet file:  data/gold/features_events.parquet

Usage
-----
python src/10_build_features.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

# Project helpers (expected to exist in the repo)
from common import init_db, get_db

# ----------------------------- Configuration ---------------------------------

BIN_MINUTES = 10  # time grid for aligning events to weather
ASOF_WINDOW_MIN = 5  # used only with --asof
PARQUET_OUT = Path("data/gold/features_events.parquet")


# ----------------------------- Logging helper --------------------------------

def setup_logging(name: str, level: str = "INFO") -> logging.Logger:
    """Initialize a simple console logger (fallback if project logger is absent)."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)


# ----------------------------- Small utilities -------------------------------

def ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    """Check if a table exists in the given schema."""
    return con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, name],
    ).fetchone() is not None


def pick_dominant_station(con: duckdb.DuckDBPyConnection) -> str | None:
    """Pick the station_id with the most rows in weather_obs."""
    row = con.execute(
        """
        SELECT station_id
        FROM weather_obs
        GROUP BY station_id
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else None


# ----------------------------- Core builder ----------------------------------

def build_features_events(
        con: duckdb.DuckDBPyConnection,
        log: logging.Logger,
        *,
        asof: bool = False,
        station_id: str | None = None,
) -> None:
    """
    Build the `features_events` table entirely inside DuckDB (no pandas materialization).

    Join priority (to avoid duplicates if multiple stations in weather_obs):
      1) Use per-stop mapping `stop_to_station` if present.
      2) Else use explicit `--station-id` if provided.
      3) Else use the dominant station (warn).
    """
    log.info("Building features_events (bin=%s, join=%s)", BIN_MINUTES, "ASOF" if asof else "STRICT")

    # Helpful indexes (no-op if they already exist)
    con.execute("CREATE INDEX IF NOT EXISTS idx_weather_ts_station ON weather_obs(ts_utc, station_id)")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_ist_stop_ts ON ist_events(stop_code, depart_sched_ts, arrival_sched_ts)")

    # Resolve station strategy
    use_mapping = table_exists(con, "main", "stop_to_station")
    chosen_station = None
    if not use_mapping:
        if station_id:
            chosen_station = station_id
            log.info("Using single weather station: %s", chosen_station)
        else:
            chosen_station = pick_dominant_station(con)
            log.warning(
                "stop_to_station not found and --station-id not provided. "
                "Falling back to dominant station: %s",
                chosen_station,
            )

    station_filter_sql = (
        "AND w.station_id = sm.station_id" if use_mapping
        else f"AND w.station_id = '{chosen_station}'" if chosen_station
        else ""
    )

    mapping_join_sql = "LEFT JOIN stop_to_station sm ON sm.stop_code = base.stop_code" if use_mapping else ""

    if not asof:
        weather_join_sql = f"""
          SELECT e.*,
                 w.temp_c, w.rain_mm, w.wind_ms, w.gust_ms, w.wind_dir_deg,
                 w.humidity, w.pressure_hpa, w.global_rad_wm2, w.sunshine_min, w.dewpoint_c
          FROM enriched e
          LEFT JOIN weather_obs w
            ON w.ts_utc = e.sched_bin
           {station_filter_sql}
        """
    else:
        weather_join_sql = f"""
          SELECT e.*,
                 w.temp_c, w.rain_mm, w.wind_ms, w.gust_ms, w.wind_dir_deg,
                 w.humidity, w.pressure_hpa, w.global_rad_wm2, w.sunshine_min, w.dewpoint_c,
                 w.ts_utc AS weather_ts
          FROM enriched e
          LEFT JOIN weather_obs w
            ON w.ts_utc <= e.sched_bin
           {station_filter_sql}
          QUALIFY w.ts_utc = MAX(w.ts_utc) OVER (PARTITION BY e._row_id)
        """

    # 1) Create/replace table inside DuckDB (CTAS) — memory-safe
    con.execute(f"""
      CREATE OR REPLACE TABLE features_events AS
      WITH base AS (
        SELECT
          service_date,
          operator_abbr, product_id, line_text,
          stop_name, stop_code,
          arrival_sched_ts, arrival_est_ts,
          depart_sched_ts,  depart_est_ts
        FROM ist_events
        WHERE operator_abbr = 'TPG'
          AND (product_id IN ('Bus','Tram') OR product_id IS NULL)
          AND (arrival_sched_ts IS NOT NULL OR depart_sched_ts IS NOT NULL)
      ),
      enriched AS (
        SELECT
          ROW_NUMBER() OVER () AS _row_id,   -- stable row id for windowed ASOF
          base.*,

          -- coalesced timestamps (prefer departure over arrival)
          COALESCE(depart_sched_ts, arrival_sched_ts) AS sched_ts,
          COALESCE(depart_est_ts,   arrival_est_ts)   AS est_ts,

          -- explicit COALESCE flags to trace potential bias
          (depart_sched_ts IS NULL AND arrival_sched_ts IS NOT NULL) AS coalesce_sched_from_arrival,
          (depart_est_ts   IS NULL AND arrival_est_ts   IS NOT NULL) AS coalesce_est_from_arrival,
          ((depart_sched_ts IS NULL AND arrival_sched_ts IS NOT NULL)
            OR (depart_est_ts   IS NULL AND arrival_est_ts   IS NOT NULL)) AS any_coalesce_from_arrival,

          -- delays: coalesced (broad coverage) and strict depart-only (unbiased)
          CASE
            WHEN COALESCE(depart_sched_ts, arrival_sched_ts) IS NOT NULL
             AND COALESCE(depart_est_ts,   arrival_est_ts)   IS NOT NULL
            THEN DATE_DIFF('second',
                           CAST(COALESCE(depart_sched_ts, arrival_sched_ts) AS TIMESTAMP),
                           CAST(COALESCE(depart_est_ts,   arrival_est_ts)   AS TIMESTAMP))
            ELSE NULL
          END AS delay_sec,

          CASE
            WHEN depart_sched_ts IS NOT NULL AND depart_est_ts IS NOT NULL
            THEN DATE_DIFF('second', CAST(depart_sched_ts AS TIMESTAMP), CAST(depart_est_ts AS TIMESTAMP))
            ELSE NULL
          END AS depart_only_delay_sec,

          -- 10-minute floor bin based on sched_ts
          (TIMESTAMP '1970-01-01'
            + INTERVAL (FLOOR(
                DATE_DIFF('minute', TIMESTAMP '1970-01-01',
                          COALESCE(depart_sched_ts, arrival_sched_ts)) / {BIN_MINUTES}
              ) * {BIN_MINUTES}) MINUTE
          ) AS sched_bin
        FROM base
        {mapping_join_sql}
      ),
      weather_join AS (
        {weather_join_sql}
      )
      SELECT
        service_date, line_text, stop_name, stop_code,
        arrival_sched_ts, arrival_est_ts, depart_sched_ts, depart_est_ts,
        sched_ts, est_ts,
        coalesce_sched_from_arrival, coalesce_est_from_arrival, any_coalesce_from_arrival,
        delay_sec, CAST(delay_sec AS DOUBLE)/60.0 AS delay_min,
        depart_only_delay_sec,
        sched_bin,
        {"sm.station_id AS station_id," if use_mapping else ""}
        temp_c, rain_mm, wind_ms, gust_ms, wind_dir_deg,
        humidity, pressure_hpa, global_rad_wm2, sunshine_min, dewpoint_c
      FROM weather_join
    """)

    # 2) ASOF post-filter (kept fully inside DuckDB)
    if asof:
        con.execute(f"""
          DELETE FROM features_events
          WHERE weather_ts IS NULL
             OR DATE_DIFF('minute', weather_ts, sched_bin) < 0
             OR DATE_DIFF('minute', weather_ts, sched_bin) > {ASOF_WINDOW_MIN}
        """)
        # Drop helper column if present
        cols = [c[0] for c in con.execute("PRAGMA table_info('features_events')").fetchall()]
        if "weather_ts" in cols:
            con.execute("ALTER TABLE features_events DROP COLUMN weather_ts")

    # 3) Compact QC logging (small aggregations only)
    row = con.execute(
        """
        SELECT
          COUNT(*)                                             AS rows_total,
          SUM(CAST(coalesce_sched_from_arrival AS INTEGER))    AS coalesce_sched,
          SUM(CAST(coalesce_est_from_arrival   AS INTEGER))    AS coalesce_est,
          SUM(CAST(any_coalesce_from_arrival   AS INTEGER))    AS any_coalesce,
          SUM(CASE WHEN depart_sched_ts IS NOT NULL AND depart_est_ts IS NOT NULL THEN 1 ELSE 0 END) AS both_depart,
          SUM(CASE WHEN sched_ts IS NULL OR est_ts IS NULL THEN 1 ELSE 0 END)                        AS unusable
        FROM features_events
        """
    ).fetchone()
    log.info(
        "QC | rows=%s coalesce_sched=%s coalesce_est=%s any_coalesce=%s both_depart=%s unusable=%s",
        *row,
    )


# ----------------------------- Export helper ---------------------------------

def export_parquet(con: duckdb.DuckDBPyConnection, out_path: Path, log: logging.Logger) -> None:
    """Export the full table via DuckDB COPY (no pandas)."""
    ensure_parent_dir(out_path)
    con.execute(
        f"""
        COPY (SELECT * FROM features_events)
        TO '{out_path.as_posix()}'
        (FORMAT 'parquet', COMPRESSION 'ZSTD');
        """
    )
    n = con.execute("SELECT COUNT(*) FROM features_events").fetchone()[0]
    log.info("Parquet → %s (rows=%s)", out_path, f"{n:,}")


# ----------------------------- Main ------------------------------------------

def main(
        db: str | None,
        log_level: str = "INFO",
        *,
        asof: bool = False,
        station_id: str | None = None,
) -> None:
    log = setup_logging("build_features", log_level)
    init_db(db)
    con = get_db(db)

    build_features_events(con, log, asof=asof, station_id=station_id)
    export_parquet(con, PARQUET_OUT, log)
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build features_events (memory-safe, station-aware).")
    parser.add_argument("--db", default=None, help="DuckDB path (None → default from common.py)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--asof", action="store_true",
                        help="Use nearest-past weather join (≤ 5 min) instead of strict equality")
    parser.add_argument("--station-id", default=None, help="Fallback station if no stop_to_station mapping")
    args = parser.parse_args()

    main(args.db, args.log_level, asof=args.asof, station_id=args.station_id)
