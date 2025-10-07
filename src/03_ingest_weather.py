#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# src/03_ingest_weather_v.py

"""
Ingest MeteoSwiss "one-file-all-stations" CSV into Parquet + DuckDB.

Highlights
----------
- Single CSV input (no glob expansion).
- Read only needed raw columns; robust encoding fallback.
- Normalize columns (UTC timestamp, units, names).
- One Parquet per run, stored under a hard-coded directory.
- Upsert in DuckDB by (station_id, ts_utc) window [tmin, tmax] with a single fast transaction.

Usage
-----
python src/03_ingest_weather.py --csv data/raw/weather/ogd-smn_gve_t_recent.csv --db data/warehouse.duckdb
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from common import get_db, init_db
from utils_logging import setup_logging

# ───────────────────────── column mapping & selection ───────────────────────── #

# Raw MeteoSwiss → normalized names used in the warehouse
COLMAP = {
    "station_abbr": "station_id",
    "reference_timestamp": "ts_utc",
    "tre200s0": "temp_c",  # Temperature (°C)
    "rre150z0": "rain_mm",  # Precipitation over 10 min (mm)
    "fu3010z0": "wind_kmh",  # Mean wind (km/h) → will convert to m/s
    "fu3010z1": "gust_kmh",  # Gust peak (km/h) → will convert to m/s
    "dkl010z0": "wind_dir_deg",  # Wind direction (deg)
    "ure200s0": "humidity",  # Relative humidity (%)
    "prestas0": "pressure_hpa",  # Pressure (hPa, QFE)
    "gre000z0": "global_rad_wm2",  # Global radiation (W/m²)
    "sre000z0": "sunshine_min",  # Sunshine duration (min / 10 min)
    "tde200s0": "dewpoint_c",  # Dew point (°C)
}

RAW_USED = list(COLMAP.keys())

# Final column order expected by the DDL (weather_obs with converted raw from kmh -> ms)
FINAL_COLS = [
    "station_id", "ts_utc",
    "temp_c", "rain_mm",
    "wind_ms", "gust_ms",  # note: converted from *_kmh
    "wind_dir_deg", "humidity",
    "pressure_hpa", "global_rad_wm2",
    "sunshine_min", "dewpoint_c",
]


# ───────────────────────── IO helpers ───────────────────────── #

def read_csv_any(path: str, log) -> pd.DataFrame:
    """
    Read MeteoSwiss CSV with robust encoding and column pruning.
    - Separator ';'
    - Missing values often as '-'
    - Try utf-8 then latin-1
    """
    log.info(f"[read] CSV: {path}")
    try:
        return pd.read_csv(
            filepath_or_buffer=path, sep=";", na_values=["-"], encoding="utf-8",
            low_memory=False, usecols=RAW_USED
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            filepath_or_buffer=path, sep=";", na_values=["-"], encoding="latin-1",
            low_memory=False, usecols=RAW_USED
        )


def save_parquet(df: pd.DataFrame, out_path: Optional[str], log) -> str:
    """
    Save a DataFrame to Parquet.
    If out_path is None, build a filename under the hard-coded output dir.
    """
    out_dir = Path("data/silver")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not out_path:
        # Build a descriptive filename based on station(s) and time range
        station_set = sorted(df["station_id"].dropna().astype(str).unique().tolist())
        station_tag = (station_set[0] if len(station_set) == 1 else "multi")
        tmin = df["ts_utc"].min()
        tmax = df["ts_utc"].max()
        stamp = (
            f"{tmin.strftime('%Y%m%d%H%M')}_{tmax.strftime('%Y%m%d%H%M')}"
            if pd.notna(tmin) and pd.notna(tmax) else "range_unknown"
        )
        out_path = str(out_dir / f"weather_{station_tag}_{stamp}.parquet")

    log.info(f"[write] Parquet: {out_path}")
    df.to_parquet(out_path, index=False)
    log.info(f"[ok] Parquet rows={len(df):,}")
    return out_path


# ───────────────────────── normalization ───────────────────────── #
def normalize_df(df: pd.DataFrame, log) -> pd.DataFrame:
    """
    normalize one MeteoSwiss CSV.

    Steps
    -----
    - Rename keys and parse timestamp.
    - Cast numeric payload.
    - Convert wind speed/gust from km/h → m/s.
    - Drop rows with null timestamp; sort by (station_id, ts_utc).
    - Return DataFrame with FINAL_COLS order.

    """
    if df.empty:
        return df

    # Rename keys
    df = df.rename(columns={
        "station_abbr": "station_id",
        "reference_timestamp": "ts_utc"
    })

    # Parse timestamp (MeteoSwiss format like 'dd.mm.YYYY HH:MM')
    # NOTE: Provided timestamps are UTC per provider docs.
    log.info("[normalize] parse timestamps")
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], format="%d.%m.%Y %H:%M", errors="coerce", utc=True)

    # Build output with typed numeric columns
    log.info("[normalize] map numeric fields")
    out = df[["station_id", "ts_utc"]].copy()
    for raw, dst in COLMAP.items():
        if raw in ("station_abbr", "reference_timestamp"):
            continue
        if raw in df.columns:
            out[COLMAP[raw]] = pd.to_numeric(df[raw], errors="coerce")

    # Convert km/h → m/s and drop km/h columns
    log.info("[normalize] convert wind km/h → m/s")
    if "wind_kmh" in out.columns:
        out["wind_ms"] = out["wind_kmh"] * (1000.0 / 3600.0)
        out.drop(columns=["wind_kmh"], inplace=True, errors="ignore")
    if "gust_kmh" in out.columns:
        out["gust_ms"] = out["gust_kmh"] * (1000.0 / 3600.0)
        out.drop(columns=["gust_kmh"], inplace=True, errors="ignore")

    # Clean null timestamps and sort
    out = out.dropna(subset=["ts_utc"]).sort_values(["station_id", "ts_utc"])

    # Align final columns (missing ones will be added as NA)
    for col in FINAL_COLS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[FINAL_COLS]

    log.info(f"[normalize] loaded rows={len(out):,}")

    return out


def dedupe_df(df: pd.DataFrame, log) -> pd.DataFrame:
    """
    Ensure uniqueness on (station_id, ts_utc).

    Strategy
    --------
    1) Drop exact duplicates (all columns equal).
    2) If multiple rows share the same (station_id, ts_utc),
       aggregate numeric columns with a robust median (skip NA).

    Notes
    -----
    - This matches the PK constraint in weather_obs and prevents upsert conflicts.
    - Median is deterministic and robust to occasional discordant readings.
    """
    if df.empty:
        log.info("Empty input df")
        return df

    # 1) remove exact duplicates
    work = df.drop_duplicates().copy()

    # 2) enforce uniqueness on business key
    key = ["station_id", "ts_utc"]
    dup_mask = work.duplicated(subset=key, keep=False)

    if not dup_mask.any():
        # already unique on key
        return work

    log.info("[dedupe] resolving %d duplicate rows on key %s", int(dup_mask.sum()), tuple(key))

    # All measurement columns are numeric in FINAL_COLS except the key
    value_cols = [c for c in FINAL_COLS if c not in key]

    # Group by key and aggregate:
    # - for numeric columns: median(skipna)
    agg_df = (
        work.groupby(key, as_index=False)[value_cols]
        .median(numeric_only=True)
    )

    # Reassemble in FINAL_COLS order
    out = agg_df[key + value_cols].sort_values(key).reset_index(drop=True)
    log.info("[dedupe] %d → %d rows after key-level aggregation", len(df), len(out))
    return out


def load_file(csv_path: str, log) -> pd.DataFrame:
    """
    Read + normalize +dedupe one MeteoSwiss CSV.

    Steps
    -----
    - Read only the required raw columns.
    - normalize.
    - dedupe.
    - Return DataFrame with FINAL_COLS order.
    """
    df = read_csv_any(csv_path, log)

    # Mandatory columns
    for req in ("reference_timestamp", "station_abbr"):
        if req not in df.columns:
            raise ValueError(f"Missing column in {csv_path}: {req}")
    df = normalize_df(df, log)
    df = dedupe_df(df, log)
    log.info(f"[load_file] loaded rows={len(df):,}")
    return df


# ───────────────────────── DuckDB upsert ───────────────────────── #

def upsert_duckdb(con, parquet_path: str, tmin: datetime, tmax: datetime, log) -> None:
    """
    Upsert into weather_obs using a single transaction:

    1) Read the Parquet once into a temp table _incoming.
    2) DELETE target rows overlapping [tmin, tmax] per station present in _incoming.
    3) INSERT all rows from _incoming.
    """
    if not parquet_path:
        log.warning("[upsert] no parquet → skip")
        return

    log.info(f"[upsert] start | parquet={parquet_path}")
    con.execute("BEGIN")
    try:
        log.info("[upsert] create temp _incoming")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _incoming AS
            SELECT * FROM read_parquet(?)
        """, [parquet_path])

        log.info("[upsert] delete overlapped windows per station")
        con.execute("""
            DELETE FROM weather_obs t
            USING (
                SELECT station_id, MIN(ts_utc) AS tmin, MAX(ts_utc) AS tmax
                FROM _incoming
                GROUP BY station_id
            ) d
            WHERE t.station_id = d.station_id
              AND t.ts_utc BETWEEN d.tmin AND d.tmax
        """)

        log.info("[upsert] insert new rows")
        con.execute("INSERT INTO weather_obs SELECT * FROM _incoming")

        con.execute("COMMIT")
        log.info(f"[upsert] done | window=[{tmin} .. {tmax}]")
    except Exception as e:
        con.execute("ROLLBACK")
        log.error(f"[upsert] ROLLBACK: {e}")
        raise


# ───────────────────────── orchestrator ───────────────────────── #

def main(
        csv_path: str,
        db: Optional[str],
        log_level: str = "INFO",
        no_file_log: bool = False,
        no_console_log: bool = False,
) -> None:
    """
    Parameters
    ----------
    csv_path : str
        Path to the single MeteoSwiss CSV file to ingest.
    db : Optional[str]
        DuckDB file path (None → default from common.py).
    log_level : str
        Logging verbosity.
        :param no_file_log:
    """
    log = setup_logging("ingest_weather", log_level, not no_file_log, not no_console_log)

    # Ensure DB and schema are ready
    init_db(db)
    con = get_db(db)

    # Read + normalize the single CSV
    df = load_file(csv_path, log)

    # Hard-coded output directory (like IstDaten)
    parquet_path = save_parquet(df, out_path=None, log=log)

    # Time bounds for targeted DELETE per station
    tmin, tmax = df["ts_utc"].min(), df["ts_utc"].max()

    # Upsert into DuckDB
    upsert_duckdb(con, parquet_path, tmin, tmax, log)

    log.info("Ingestion OK.")


# ───────────────────────── CLI ───────────────────────── #

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest one MeteoSwiss CSV → Parquet + DuckDB.")
    ap.add_argument("--csv", required=True, help='Path to the CSV, e.g. data/raw/weather/ogd-smn_gve_t_recent.csv')
    ap.add_argument("--db", default=None, help="DuckDB path (default from common.py).")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    ap.add_argument("--no-file-log", action="store_true", help="Disable file logging.")
    ap.add_argument("--no-console-log", action="store_true", help="Disable console logging.")
    args = ap.parse_args()

    main(args.csv, args.db, args.log_level, args.no_file_log, args.no_console_log)
