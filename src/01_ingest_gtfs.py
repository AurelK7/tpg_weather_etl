# src/01_ingest_gtfs.py
"""
Ingest a GTFS snapshot (ZIP) into DuckDB + write silver Parquet extracts.

- Detects feed_version from feed_info.txt (fallback: filename suffix)
- Filters to TPG routes only (by default, operator name containing "Genevois")
- Writes silver Parquet directly from each process_* function
- Upserts into DuckDB in a single transaction
- Logging is configurable via CLI flags
"""

import argparse
import zipfile
from pathlib import Path
import pandas as pd

from common import get_db, init_db
from utils_logging import setup_logging


# ───────────────────────────── helpers ───────────────────────────── #

def read_csv_from_zip(zip_path: str | Path, member: str, logger=None) -> pd.DataFrame:
    """
    Read a CSV file inside the GTFS ZIP.

    Parameters
    ----------
    zip_path : str | Path
        Path to the GTFS ZIP file.
    member : str
        The name of the file to read inside the ZIP (e.g., "routes.txt").
    logger : logging.Logger, optional
        Logger for warnings.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the file contents. Empty if file missing.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(member) as f:
                return pd.read_csv(f, low_memory=False)
    except KeyError:
        if logger:
            logger.warning(f"Missing file in GTFS ZIP: {member} → returning empty DataFrame")
        return pd.DataFrame()


def ensure_columns(df: pd.DataFrame, required: list[str], optional_defaults: dict[str, object] | None = None, ctx: str = "") -> pd.DataFrame:
    """
    Ensure required columns exist, add optional columns with defaults.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to check.
    required : list[str]
        Columns that must be present.
    optional_defaults : dict, optional
        Dict of {column: default_value} for optional columns.
    ctx : str
        Context name for error messages.

    Returns
    -------
    pd.DataFrame
        DataFrame with required + optional columns ensured.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{ctx}] required columns missing: {missing}")
    if optional_defaults:
        for c, val in optional_defaults.items():
            if c not in df.columns:
                df[c] = val
    return df


def detect_feed_version(gtfs_zip: str | Path, logger=None) -> str:
    """
    Detect feed_version from feed_info.txt, fallback to filename.

    Parameters
    ----------
    gtfs_zip : str | Path
        Path to GTFS ZIP.
    logger : logging.Logger, optional

    Returns
    -------
    str
        Feed version string.
    """
    info = read_csv_from_zip(gtfs_zip, "feed_info.txt", logger)
    if not info.empty and "feed_version" in info.columns and pd.notna(info.loc[0, "feed_version"]):
        fv = str(info.loc[0, "feed_version"])
        if logger: logger.info(f"feed_version from feed_info.txt = {fv}")
        return fv
    fv = Path(gtfs_zip).stem.split("_")[-1]
    if logger: logger.info(f"feed_version fallback from filename = {fv}")
    return fv


# ───────────────────────────── processors ───────────────────────────── #

def process_routes(gtfs_zip: str | Path, feed_version: str, operator_pattern: str | None, silver_dir: Path, logger) -> Path:
    """
    Process routes.txt + agency.txt, filter TPG routes, save parquet.

    Parameters
    ----------
    gtfs_zip : str | Path
        Path to GTFS ZIP file.
    feed_version : str
        Feed version string.
    operator_pattern : str | None
        Pattern to filter operator_name (e.g., 'Genevois'). If None, no filter.
    silver_dir : Path
        Directory to save Parquet file.
    logger : logging.Logger

    Returns
    -------
    Path
        Path to saved Parquet file.
    """
    agency = read_csv_from_zip(gtfs_zip, "agency.txt", logger)
    if not agency.empty:
        agency = agency[[c for c in agency.columns if c in ("agency_id", "agency_name")]].copy()

    routes = read_csv_from_zip(gtfs_zip, "routes.txt", logger)
    routes = ensure_columns(routes, ["route_id", "route_type"],
                            {"route_short_name": pd.NA, "route_long_name": pd.NA, "agency_id": pd.NA},
                            ctx="routes")

    if not agency.empty and "agency_id" in routes.columns and "agency_id" in agency.columns:
        routes = routes.merge(agency, on="agency_id", how="left").rename(columns={"agency_name": "operator_name"})
    else:
        routes["operator_name"] = pd.NA

    if operator_pattern:
        before = len(routes)
        routes = routes[routes["operator_name"].astype(str).str.contains(operator_pattern, case=False, na=False)].reset_index(drop=True)
        logger.info(f"TPG filter kept {len(routes):,}/{before:,} routes")

    routes["feed_version"] = feed_version
    routes = routes[["route_id", "route_short_name", "route_long_name", "route_type", "operator_name", "feed_version"]]

    p = silver_dir / "gtfs_routes.parquet"
    routes.to_parquet(p, index=False)
    logger.info(f"routes → {p} ({len(routes):,} rows)")
    return p


def process_trips(gtfs_zip: str | Path, feed_version: str, routes_path: Path, silver_dir: Path, logger) -> Path:
    """
    Process trips.txt, filter by route_ids in routes parquet, save parquet.
    """
    trips = read_csv_from_zip(gtfs_zip, "trips.txt", logger)
    trips = ensure_columns(trips, ["trip_id", "route_id", "service_id"],
                           {"direction_id": pd.NA, "trip_headsign": pd.NA},
                           ctx="trips")

    routes = pd.read_parquet(routes_path)
    trips = trips[trips["route_id"].isin(routes["route_id"])].reset_index(drop=True)

    trips["feed_version"] = feed_version
    trips = trips[["trip_id", "route_id", "service_id", "direction_id", "trip_headsign", "feed_version"]]

    p = silver_dir / "gtfs_trips.parquet"
    trips.to_parquet(p, index=False)
    logger.info(f"trips → {p} ({len(trips):,} rows)")
    return p


def process_stop_times(gtfs_zip: str | Path, feed_version: str, trips_path: Path, silver_dir: Path, logger) -> Path:
    """
    Process stop_times.txt, filter by trip_ids in trips parquet, save parquet.
    """
    st = read_csv_from_zip(gtfs_zip, "stop_times.txt", logger)
    st = ensure_columns(st, ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"], ctx="stop_times")

    trips = pd.read_parquet(trips_path)
    st = st[st["trip_id"].isin(trips["trip_id"])].reset_index(drop=True)

    st = st.rename(columns={"arrival_time": "arrival_time_planned", "departure_time": "departure_time_planned"})
    st["feed_version"] = feed_version
    st = st[["trip_id", "stop_sequence", "stop_id", "arrival_time_planned", "departure_time_planned", "feed_version"]]

    p = silver_dir / "gtfs_stop_times.parquet"
    st.to_parquet(p, index=False)
    logger.info(f"stop_times → {p} ({len(st):,} rows)")
    return p


def process_stops(gtfs_zip: str | Path, feed_version: str, stop_times_path: Path, silver_dir: Path, logger) -> Path:
    """
    Process stops.txt, keep only stops in stop_times parquet, save parquet.
    """
    stops = read_csv_from_zip(gtfs_zip, "stops.txt", logger)
    stops = ensure_columns(stops, ["stop_id", "stop_name", "stop_lat", "stop_lon"],
                           {"location_type": pd.NA, "parent_station": pd.NA, "platform_code": pd.NA, "zone_id": pd.NA},
                           ctx="stops")

    st = pd.read_parquet(stop_times_path)
    stops = stops[stops["stop_id"].isin(st["stop_id"])].reset_index(drop=True)

    stops = stops.rename(columns={"stop_lat": "lat", "stop_lon": "lon"})
    stops["feed_version"] = feed_version
    stops = stops[["stop_id", "stop_name", "lat", "lon", "zone_id", "location_type", "parent_station", "platform_code", "feed_version"]]

    p = silver_dir / "gtfs_stops.parquet"
    stops.to_parquet(p, index=False)
    logger.info(f"stops → {p} ({len(stops):,} rows)")
    return p


def upsert_duckdb(con, feed_version: str, p_stops: Path, p_routes: Path, p_trips: Path, p_stop_times: Path, logger):
    """
    Load Parquet files into DuckDB in a single transaction.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Database connection.
    feed_version : str
        Current feed version.
    p_stops, p_routes, p_trips, p_stop_times : Path
        Paths to Parquet files.
    logger : logging.Logger
    """
    con.execute("BEGIN")
    try:
        con.execute("DELETE FROM gtfs_stops WHERE feed_version = ?", [feed_version])
        con.execute("INSERT INTO gtfs_stops SELECT * FROM read_parquet(?)", [str(p_stops)])

        con.execute("DELETE FROM gtfs_routes WHERE feed_version = ?", [feed_version])
        con.execute("INSERT INTO gtfs_routes SELECT * FROM read_parquet(?)", [str(p_routes)])

        con.execute("DELETE FROM gtfs_trips WHERE feed_version = ?", [feed_version])
        con.execute("INSERT INTO gtfs_trips SELECT * FROM read_parquet(?)", [str(p_trips)])

        con.execute("DELETE FROM gtfs_stop_times WHERE feed_version = ?", [feed_version])
        con.execute("INSERT INTO gtfs_stop_times SELECT * FROM read_parquet(?)", [str(p_stop_times)])

        con.execute("COMMIT")
        logger.info(f"GTFS upserted successfully | feed_version={feed_version}")
    except Exception as e:
        con.execute("ROLLBACK")
        logger.error(f"Ingest failed, rolled back. Error: {e}")
        raise


# ───────────────────────────── main ───────────────────────────── #

def main(gtfs_zip: str, feed_version: str | None, db: str | None,
         log_level: str = "INFO", no_file_log: bool = False, no_console_log: bool = False,
         operator_pattern: str | None = "Transports Publics Genevois"):
    log = setup_logging("ingest_gtfs", log_level, not no_file_log, not no_console_log)
    init_db(db)
    con = get_db(db)

    if not feed_version:
        feed_version = detect_feed_version(gtfs_zip, log)

    silver_dir = Path("data/silver")

    p_routes = process_routes(gtfs_zip, feed_version, operator_pattern, silver_dir, log)
    p_trips = process_trips(gtfs_zip, feed_version, p_routes, silver_dir, log)
    p_stop_times = process_stop_times(gtfs_zip, feed_version, p_trips, silver_dir, log)
    p_stops = process_stops(gtfs_zip, feed_version, p_stop_times, silver_dir, log)

    upsert_duckdb(con, feed_version, p_stops, p_routes, p_trips, p_stop_times, log)
    print(f"Loaded GTFS feed_version = {feed_version}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest GTFS ZIP into DuckDB (TPG only by default).")
    ap.add_argument("--gtfs", required=True)
    ap.add_argument("--feed_version", default=None)
    ap.add_argument("--db", default=None)
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    ap.add_argument("--no-file-log", action="store_true")
    ap.add_argument("--no-console-log", action="store_true")
    ap.add_argument("--operator-pattern", default="Transports Publics Genevois")
    args = ap.parse_args()

    main(args.gtfs, args.feed_version, args.db, args.log_level, args.no_file_log, args.no_console_log,
         args.operator_pattern if args.operator_pattern.strip() else None)
