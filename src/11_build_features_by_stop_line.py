#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  src/11_build_features_by_stop_line.py
"""
11_build_features_by_stop_line.py — 10-minute aggregation by (line_text × stop_code)

Overview
--------
Aggregates the canonical event-level table `features_events` over 10-minute bins
(`sched_bin`) and writes the result to `features_by_stop_line` + Parquet.

Why this table?
---------------
- Provides a compact, dashboard-friendly view per (line × stop × time bin).
- Adds a stable `stop_key` = "{line_text}·{stop_code}" for filtering/joins.
- Surfaces potential COALESCE bias at aggregate level via `share_coalesce`.

Key metrics
-----------
- n_trips            : number of events in the bin
- delay_avg_min      : average delay (minutes)
- delay_p50_min      : median delay (minutes)        → MEDIAN(delay_min)
- delay_p90_min      : 90th percentile delay (min)   → QUANTILE(delay_min, 0.9)
- share_late_ge2     : share of events with delay ≥ 2 min
- share_coalesce     : share of events where depart_* was substituted by arrival_*

Weather aggregates (means/max) are included for quick EDA.

CLI
---
--db         : optional DuckDB path (else from common.py)
--log-level  : DEBUG | INFO | WARNING | ERROR  (default: INFO)

Outputs
-------
- DuckDB table:  features_by_stop_line
- Parquet file:  data/gold/features_by_stop_line.parquet

Usage
-----
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from common import init_db, get_db
from utils_logging import setup_logging

PARQUET_OUT = Path("data/gold/features_by_stop_line.parquet")


def ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    return con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, name],
    ).fetchone() is not None


def build_features_by_stop_line(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> None:
    """
    Build the aggregated table from `features_events`:
      - grouping keys: line_text, stop_code, stop_key, sched_bin
      - metrics: n_trips, delays (avg/median/p90/share ≥2min), share_coalesce
      - weather: mean/max summaries
    """
    if not table_exists(con, "main", "features_events"):
        raise RuntimeError("features_events not found. Run 10_build first.")

    # Build inside DuckDB (CTAS). We keep 10-minute bins as-is (no downsampling).
    con.execute(
        """
        DROP TABLE IF EXISTS features_by_stop_line;

        CREATE TABLE features_by_stop_line AS
        WITH base AS (
          SELECT
            line_text,
            stop_code,
            COALESCE(stop_name, CAST(stop_code AS VARCHAR)) AS stop_name,
            sched_bin,
            delay_min,
            any_coalesce_from_arrival,
            -- weather at sched_bin
            temp_c, rain_mm, wind_ms, gust_ms, wind_dir_deg,
            humidity, pressure_hpa, global_rad_wm2, sunshine_min, dewpoint_c
          FROM features_events
          WHERE sched_bin IS NOT NULL
        ),
        aggr AS (
          SELECT
            line_text,
            stop_code,
            -- stop_key for dashboard filters and joins
            line_text || '·' || CAST(stop_code AS VARCHAR) AS stop_key,
            MAX(stop_name) AS stop_name,
            sched_bin,

            CAST(COUNT(*) AS BIGINT)                 AS n_trips,
            CAST(AVG(delay_min) AS DOUBLE)           AS delay_avg_min,
            MEDIAN(delay_min)                        AS delay_p50_min,
            QUANTILE(delay_min, 0.9)                 AS delay_p90_min,

            AVG(CAST(delay_min >= 2 AS DOUBLE))      AS share_late_ge2,
            AVG(CAST(any_coalesce_from_arrival AS DOUBLE)) AS share_coalesce,

            -- Weather summaries
            AVG(temp_c)           AS temp_c_mean,
            AVG(rain_mm)          AS rain_mm_mean,
            MAX(rain_mm)          AS rain_mm_max,
            AVG(wind_ms)          AS wind_ms_mean,
            AVG(gust_ms)          AS gust_ms_mean,
            AVG(wind_dir_deg)     AS wind_dir_deg_mean,
            AVG(humidity)         AS humidity_mean,
            AVG(pressure_hpa)     AS pressure_hpa_mean,
            AVG(global_rad_wm2)   AS global_rad_wm2_mean,
            AVG(sunshine_min)     AS sunshine_min_mean,
            AVG(dewpoint_c)       AS dewpoint_c_mean

          FROM base
          GROUP BY 1,2,3,5
        )
        SELECT * FROM aggr
        ORDER BY sched_bin DESC, line_text, stop_name;
        """
    )

    # Small QC logging
    qc = con.execute(
        """
        SELECT
          COUNT(*)                                        AS rows_bins,
          COUNT(DISTINCT sched_bin)                       AS unique_bins,
          COUNT(DISTINCT line_text || '·' || CAST(stop_code AS VARCHAR)) AS unique_stop_keys
        FROM features_by_stop_line
        """
    ).fetchone()
    log.info("QC | bins=%s unique_bins=%s unique_stop_keys=%s", *qc)


def export_parquet(con: duckdb.DuckDBPyConnection, out_path: Path, log: logging.Logger) -> None:
    ensure_parent_dir(out_path)
    con.execute(
        f"""
        COPY (SELECT * FROM features_by_stop_line)
        TO '{out_path.as_posix()}'
        (FORMAT 'parquet', COMPRESSION 'ZSTD');
        """
    )
    n = con.execute("SELECT COUNT(*) FROM features_by_stop_line").fetchone()[0]
    log.info("Parquet → %s (rows=%s)", out_path, f"{n:,}")


def main(db: str | None, log_level: str = "INFO") -> None:
    log = setup_logging("build_features_by_stop_line", log_level)
    init_db(db)
    con = get_db(db)

    build_features_by_stop_line(con, log)
    export_parquet(con, PARQUET_OUT, log)
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build features_by_stop_line (10-min aggregation).")
    parser.add_argument("--db", default=None, help="DuckDB path (None → default from common.py)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    main(args.db, args.log_level)
