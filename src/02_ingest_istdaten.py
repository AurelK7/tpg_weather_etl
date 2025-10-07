#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# src/02_ingest_istdaten_v1.py
"""
Ingest IstDaten (CSV + ZIP of CSV) WITHOUT extracting ZIPs to disk.

Highlights
----------
- Streams ZIP members: try Python `zipfile`; if compression unsupported (e.g., Deflate64),
  fall back to piping bytes via `unzip -p` or `7z x -so` (toujours sans extraction).
- Mixe CSV plats et CSV issus de ZIP en une seule passe.
- Lit uniquement les colonnes utiles; encodage robuste (utf-8-sig -> latin-1).
- Normalise + déduplique selon règles métier (par jour/course/arrêt/heure planifiée).
- Écrit 1 Parquet par CSV (au jour) sous: data/silver/ist/month=YYYY-MM/<same-name>.parquet
- Upsert DuckDB mois-par-mois avec petits COMMIT et compat pour (dés)activer les checkpoints.

Usage
-----
python src/02_ingest_istdaten.py \
  --glob "data/raw/istdaten/*.zip" "data/raw/istdaten/*_istdaten.csv" "data/raw/istdaten/*_IstDaten.csv" \
  --workers 8 --log-level INFO
"""

from __future__ import annotations

import argparse
import io
import os
import re
import subprocess
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Iterable, Optional, Union

import pandas as pd

from common import init_db, get_db
from utils_logging import setup_logging

# ───────────────────────── schema & mappings ───────────────────────── #

# Raw (DE) → normalized (EN) mapping
COLMAP = {
    "BETRIEBSTAG": "service_date",
    "FAHRT_BEZEICHNER": "fahrt_bezeichner",
    "BETREIBER_ABK": "operator_abbr",
    "PRODUKT_ID": "product_id",
    "LINIEN_TEXT": "line_text",
    "HALTESTELLEN_NAME": "stop_name",
    "BPUIC": "stop_code",
    "ANKUNFTSZEIT": "arrival_sched_ts",
    "AN_PROGNOSE": "arrival_est_ts",
    "AN_PROGNOSE_STATUS": "arrival_status",
    "ABFAHRTSZEIT": "depart_sched_ts",
    "AB_PROGNOSE": "depart_est_ts",
    "AB_PROGNOSE_STATUS": "depart_status",
    "DURCHFAHRT_TF": "pass_through",
    "ZUSATZFAHRT_TF": "is_extra_trip",
    "FAELLT_AUS_TF": "is_cancelled",
}

# Final column order (used in Parquet + INSERT)
NEEDED = [
    "service_date", "fahrt_bezeichner", "operator_abbr", "product_id", "line_text",
    "stop_name", "stop_code",
    "arrival_sched_ts", "arrival_est_ts", "arrival_status",
    "depart_sched_ts", "depart_est_ts", "depart_status",
    "pass_through", "is_extra_trip", "is_cancelled",
]

# Read only these raw columns (less I/O)
RAW_USECOLS = list(COLMAP.keys())

# Regex helpers
CSV_RE = re.compile(r".*\.csv$", re.IGNORECASE)
ISTD_RE = re.compile(r"istdaten", re.IGNORECASE)  # matches IstDaten / istdaten
DATE_IN_NAME = re.compile(r"(\d{4})[-_](\d{2})[-_](\d{2})")


# ───────────────────────── misc helpers ───────────────────────── #

def expand_globs(patterns: Iterable[str]) -> list[Path]:
    """
    Expand several patterns into a unique, sorted list of paths.
    Works even if patterns are quoted (fallback to glob module).
    """
    out: list[Path] = []
    for pat in patterns:
        out.extend([Path(p) for p in sorted(set(Path().glob(pat)))])
    if not out:
        import glob as _glob
        s: set[str] = set()
        for pat in patterns:
            s.update(_glob.glob(pat))
        out = [Path(p) for p in sorted(s)]
    return out


def date_key_from_name(name: str) -> Optional[str]:
    """
    Extract YYYY-MM from a filename (e.g., 2025-01-04_istdaten.csv → 2025-01).
    """
    m = DATE_IN_NAME.search(name)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


# ───────────────────────── ZIP streaming (no extraction) ───────────────────────── #

def zip_list_members(zip_path: Path) -> list[str]:
    """
    List CSV members inside a ZIP. Try python's zipfile; if compression unsupported,
    fallback to `unzip -Z1` then `7z l`.
    """
    # 1) python zipfile
    try:
        with zipfile.ZipFile(zip_path) as zf:
            return [n for n in zf.namelist() if CSV_RE.match(n) and ISTD_RE.search(Path(n).name)]
    except NotImplementedError:
        pass
    except Exception:
        pass

    # 2) unzip -Z1
    try:
        res = subprocess.run(["unzip", "-Z1", str(zip_path)],
                             check=True, capture_output=True, text=True)
        names = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
        return [n for n in names if CSV_RE.match(n) and ISTD_RE.search(Path(n).name)]
    except Exception:
        pass

    # 3) 7z l
    try:
        res = subprocess.run(["7z", "l", "-ba", str(zip_path)],
                             check=True, capture_output=True, text=True)
        names = []
        for ln in res.stdout.splitlines():
            parts = ln.split()
            if len(parts) >= 6:
                name = " ".join(parts[5:])
                names.append(name)
        return [n for n in names if CSV_RE.match(n) and ISTD_RE.search(Path(n).name)]
    except Exception:
        return []


def zip_read_member_bytes(zip_path: Path, member: str, log) -> bytes:
    """
    Stream a member's raw bytes without extraction:
      1) python zipfile
      2) unzip -p
      3) 7z x -so
    """
    # 1) python zipfile
    try:
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(member) as f:
                return f.read()
    except NotImplementedError as e:
        log.debug(f"[stream] zipfile unsupported compression for {zip_path.name}:{member} → {e}")
    except Exception as e:
        log.debug(f"[stream] zipfile failed for {zip_path.name}:{member} → {e}")

    # 2) unzip -p
    try:
        res = subprocess.run(["unzip", "-p", str(zip_path), member], check=True, capture_output=True)
        return res.stdout
    except FileNotFoundError:
        log.debug("[stream] 'unzip' not found; trying 7z")
    except subprocess.CalledProcessError as e:
        log.debug(f"[stream] unzip -p failed: {e}; trying 7z")

    # 3) 7z x -so
    res = subprocess.run(["7z", "x", "-so", str(zip_path), member], check=True, capture_output=True)
    return res.stdout


# ───────────────────────── CSV reading / normalize / dedupe ───────────────────────── #

def read_csv_smart_bytes(data: bytes, log) -> pd.DataFrame:
    """
    Read a semicolon-separated CSV from raw bytes.
    Try utf-8-sig first, then latin-1. Read only RAW_USECOLS.
    """
    try:
        return pd.read_csv(io.BytesIO(data), sep=";", dtype=str, encoding="utf-8-sig",
                           low_memory=False, usecols=lambda c: c in RAW_USECOLS)
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(data), sep=";", dtype=str, encoding="latin-1",
                           low_memory=False, usecols=lambda c: c in RAW_USECOLS)


def read_csv_smart_path(path: Path, log) -> pd.DataFrame:
    """
    Read a semicolon-separated CSV from a file path.
    Try utf-8-sig first, then latin-1. Read only RAW_USECOLS.
    """
    try:
        return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig",
                           low_memory=False, usecols=lambda c: c in RAW_USECOLS)
    except UnicodeDecodeError:
        return pd.read_csv(path, sep=";", dtype=str, encoding="latin-1",
                           low_memory=False, usecols=lambda c: c in RAW_USECOLS)


def _to_boolean(series: pd.Series) -> pd.Series:
    """
    Robust boolean casting:
    - Accepts "1/0", "true/false", None/NaN/empty → False.
    - Returns pandas nullable BooleanDtype ("boolean"), avoiding FutureWarning.
    """
    s = series.astype("string").str.strip().str.lower()
    mapped = s.map({"1": True, "true": True, "0": False, "false": False}, na_action=None)
    # Any non-mapped (incl. "", "nan", None) → False
    mapped = mapped.fillna(False)
    return mapped.astype("boolean")


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw IstDaten to our schema:
    - rename columns (DE→EN) and ensure all NEEDED exist,
    - fill NA statuses with "PROGNOSE" (per cookbook "Leer (= PROGNOSE)"),
    - parse datetimes,
    - cast booleans on pass/cancel/extra,
    - filter operator TPG and product in {Bus, Tram, NA},
    - return columns in NEEDED order.
    """
    if df.empty:
        return df

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={src: dst for src, dst in COLMAP.items() if src in df.columns})
    for tgt in NEEDED:
        if tgt not in df.columns:
            df[tgt] = pd.NA

    # Status: empty/NA → PROGNOSE
    df["arrival_status"] = df["arrival_status"].fillna("PROGNOSE")
    df["depart_status"] = df["depart_status"].fillna("PROGNOSE")

    # Datetimes
    df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce", dayfirst=True)
    for col in ("arrival_sched_ts", "arrival_est_ts", "depart_sched_ts", "depart_est_ts"):
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Booleans
    df["is_cancelled"] = _to_boolean(df["is_cancelled"])
    df["pass_through"] = _to_boolean(df["pass_through"])
    df["is_extra_trip"] = _to_boolean(df["is_extra_trip"])

    # Filters
    df = df[df["operator_abbr"] == "TPG"]
    df = df[df["product_id"].isin(["Bus", "Tram"]) | df["product_id"].isna()]

    return df[NEEDED]


def status_rank(series: pd.Series) -> pd.Series:
    """
    Rank status quality for dedupe: REAL/IST(3) > GESCHAETZT(2) > PROGNOSE(1) > UNBEKANNT/NA(0).
    """
    mapping = {"REAL": 3, "IST": 3, "GESCHAETZT": 2, "PROGNOSE": 1, "UNBEKANNT": 0}
    return series.astype("string").str.upper().map(mapping).fillna(0).astype(int)


def dedupe_df(df: pd.DataFrame, log=None) -> pd.DataFrame:
    """
    Drop exact duplicates, then dedupe on business key:
      (service_date, fahrt_bezeichner, stop_code, sched_key)
    where sched_key = COALESCE(arrival_sched_ts, depart_sched_ts).

    Keep the 'best' record using priority:
      1) higher status (max of arrival/depart),
      2) has estimate timestamp,
      3) not cancelled,
      4) not pass_through,
      5) latest estimate timestamp.
    """
    if df.empty:
        return df

    work = df.drop_duplicates().copy()

    sched_key = work["arrival_sched_ts"].combine_first(work["depart_sched_ts"])
    est_ts = work["arrival_est_ts"].combine_first(work["depart_est_ts"])

    arr_rank = status_rank(work["arrival_status"])
    dep_rank = status_rank(work["depart_status"])
    stat_rank = arr_rank.combine(dep_rank, max)

    # ensure clean booleans without FutureWarning
    with pd.option_context("future.no_silent_downcasting", True):
        work["is_cancelled"] = work["is_cancelled"].fillna(False).astype("boolean")
        work["pass_through"] = work["pass_through"].fillna(False).astype("boolean")

    work = work.assign(
        _sched_key=sched_key,
        _est_ts=est_ts,
        _stat_rk=stat_rank,
        _has_est=est_ts.notna(),
        _ok_cancel=~work["is_cancelled"],
        _ok_pass=~work["pass_through"],
    )

    work = work.sort_values(
        ["_stat_rk", "_has_est", "_ok_cancel", "_ok_pass", "_est_ts"],
        ascending=[False, False, False, False, False],
        kind="mergesort",
    )

    out = (
        work.drop_duplicates(
            subset=["service_date", "fahrt_bezeichner", "stop_code", "_sched_key"], keep="first"
        )
        .drop(columns=["_sched_key", "_est_ts", "_stat_rk", "_has_est", "_ok_cancel", "_ok_pass"])
    )

    if log:
        log.debug(f"[dedupe] {len(df)} → {len(out)} rows (removed {len(df) - len(out)})")
    return out


# ───────────────────────── Parquet writer ───────────────────────── #

def write_day_parquet(df: pd.DataFrame, source_name: str, out_root: Path) -> Optional[Path]:
    """
    Write one Parquet per source CSV:
      - partition dir = month=YYYY-MM (prefer from filename, else min service_date),
      - filename mirrors the CSV name (".csv" → ".parquet").
    """
    if df.empty:
        return None

    month = date_key_from_name(source_name)
    if not month:
        smin = df["service_date"].min()
        month = smin.strftime("%Y-%m") if pd.notna(smin) else "unknown"

    part_dir = out_root / f"month={month}"
    part_dir.mkdir(parents=True, exist_ok=True)

    fname = Path(source_name).name
    if fname.lower().endswith(".csv"):
        fname = fname[:-4] + ".parquet"

    out = part_dir / fname
    df.to_parquet(out, index=False)
    return out


# ───────────────────────── Unified CSV processor (ZIP-stream or flat) ───────────────────────── #

def process_csv_task(
        item: tuple[str, str, Union[Callable[[], bytes], Path]],
        out_root: Path,
        log,
) -> tuple[Optional[Path], int, str]:
    """
    Unified processor for one CSV "task".

    Parameters
    ----------
    item : (kind, label, payload)
        kind   = "zip" or "flat"
        label  = pretty name for logs (e.g., "archive.zip:2025-01-01_istdaten.csv" or "2025-01-01_istdaten.csv")
        payload= for kind="zip": callable returning raw bytes; for kind="flat": Path to file

    Returns
    -------
    (parquet_path|None, rows_written, label)
    """
    kind, label, payload = item

    # 1) Read
    if kind == "zip":
        name = Path(label.split(":", 1)[-1]).name  # member basename
        log.info(f"[CSV-START] {name} (stream)")
        raw_bytes = payload()  # type: ignore[operator]
        raw = read_csv_smart_bytes(raw_bytes, log)
    else:
        path: Path = payload  # type: ignore[assignment]
        name = path.name
        log.info(f"[CSV-START] {name} (file)")
        raw = read_csv_smart_path(path, log)

    # 2) Normalize + dedupe
    norm = normalize_df(raw)
    dedup = dedupe_df(norm, log)

    # 3) Write Parquet
    p_out = write_day_parquet(dedup, name, out_root)
    rows = len(dedup)

    # 4) Logs
    mode = "stream" if kind == "zip" else "file"
    log.info(f"[CSV] {name}: rows {len(norm):,}→{rows:,}, dedupe={len(norm) - rows:,}, file={p_out}, mode={mode}")

    return p_out, rows, label


# ───────────────────────── Upsert month-wise (short commits) ───────────────────────── #

def upsert_monthwise(con, parquet_files: list[str], log):
    """
    Month-wise upsert with small COMMIT and portable checkpoint toggles:
      - load all new shards to _incoming_all (once),
      - drop indexes ONCE,
      - optionally skip checkpoint-on-commit (version dependent),
      - per month: DELETE that month + INSERT month rows + COMMIT,
      - recreate indexes ONCE,
      - checkpoint if it was skipped.
    """
    if not parquet_files:
        log.warning("[UPSERT] No parquet files; skipping.")
        return

    t0 = time.time()
    log.info(f"[UPSERT] Monthwise upsert starting; shards={len(parquet_files)}")

    # Load once
    t = time.time()
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _incoming_all AS
        SELECT
            CAST(NULL AS BIGINT)        AS ist_event_id,
            CAST(service_date AS DATE)  AS service_date,
            fahrt_bezeichner, operator_abbr, product_id, line_text,
            stop_name, stop_code,
            CAST(arrival_sched_ts AS TIMESTAMP) AS arrival_sched_ts,
            CAST(arrival_est_ts   AS TIMESTAMP) AS arrival_est_ts,
            arrival_status,
            CAST(depart_sched_ts  AS TIMESTAMP) AS depart_sched_ts,
            CAST(depart_est_ts    AS TIMESTAMP) AS depart_est_ts,
            depart_status,
            CAST(pass_through  AS BOOLEAN) AS pass_through,
            CAST(is_extra_trip AS BOOLEAN) AS is_extra_trip,
            CAST(is_cancelled  AS BOOLEAN) AS is_cancelled,
            strftime(service_date, '%Y-%m') AS ym
        FROM read_parquet(?)
    """, [parquet_files])
    rows_all = con.execute("SELECT COUNT(*) FROM _incoming_all").fetchone()[0]
    log.info(f"[UPSERT] _incoming_all rows={rows_all:,} in {time.time() - t:.2f}s")

    # Distinct months
    months = [r[0] for r in con.execute(
        "SELECT DISTINCT ym FROM _incoming_all WHERE service_date IS NOT NULL ORDER BY ym"
    ).fetchall()]
    if not months:
        log.info("[UPSERT] No valid months to upsert — nothing to do.")
        return
    log.info(f"[UPSERT] months={months}")

    # Drop indexes ONCE
    t = time.time()
    con.execute("DROP INDEX IF EXISTS idx_ist_1")
    con.execute("DROP INDEX IF EXISTS idx_ist_2")
    log.info(f"[UPSERT] Dropped indexes in {time.time() - t:.2f}s")

    # Try to skip checkpoint on commit (DuckDB version dependent)
    skipped_checkpoints = False
    for pragma_on in ("PRAGMA disable_checkpoint_on_commit=1;", "PRAGMA debug_skip_checkpoint_on_commit=1;"):
        try:
            con.execute(pragma_on)
            skipped_checkpoints = True
            log.debug(f"Enabled: {pragma_on.strip()}")
            break
        except Exception:
            continue

    # Per-month loop
    for i, ym in enumerate(months, start=1):
        log.info(f"[UPSERT {i}/{len(months)}] Month={ym} BEGIN")
        t_m = time.time()
        con.execute("BEGIN")
        try:
            # DELETE just that month
            t_del = time.time()
            con.execute("DELETE FROM ist_events WHERE strftime(service_date, '%Y-%m') = ?", [ym])
            log.info(f"[UPSERT {i}/{len(months)}] delete {ym} in {time.time() - t_del:.2f}s")

            # INSERT that month from incoming
            t_ins = time.time()
            con.execute("""
                INSERT INTO ist_events
                SELECT
                    ist_event_id, service_date, fahrt_bezeichner, operator_abbr, product_id, line_text,
                    stop_name, stop_code, arrival_sched_ts, arrival_est_ts, arrival_status,
                    depart_sched_ts, depart_est_ts, depart_status, pass_through, is_extra_trip, is_cancelled
                FROM _incoming_all
                WHERE ym = ?
            """, [ym])
            log.info(f"[UPSERT {i}/{len(months)}] insert {ym} in {time.time() - t_ins:.2f}s")

            con.execute("COMMIT")
            log.info(f"[UPSERT {i}/{len(months)}] COMMIT OK {ym} in {time.time() - t_m:.2f}s")
        except Exception as e:
            con.execute("ROLLBACK")
            log.error(f"[UPSERT {i}/{len(months)}] ROLLBACK {ym}: {e}")
            raise

    # Recreate indexes ONCE
    t = time.time()
    con.execute("CREATE INDEX IF NOT EXISTS idx_ist_1 ON ist_events(service_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_ist_2 ON ist_events(operator_abbr, product_id, line_text)")
    log.info(f"[UPSERT] Recreated indexes in {time.time() - t:.2f}s")

    # Final checkpoint if we skipped earlier, then restore toggle
    if skipped_checkpoints:
        try:
            con.execute("PRAGMA checkpoint;")
            log.debug("PRAGMA checkpoint executed")
        except Exception:
            pass
        for pragma_off in ("PRAGMA disable_checkpoint_on_commit=0;", "PRAGMA debug_skip_checkpoint_on_commit=0;"):
            try:
                con.execute(pragma_off)
                log.debug(f"Disabled: {pragma_off.strip()}")
                break
            except Exception:
                continue

    log.info(f"[UPSERT] Done in {time.time() - t0:.2f}s")


# ───────────────────────── Orchestrator ───────────────────────── #

def main(
        glob_patterns: list[str],
        db: Optional[str],
        workers: Optional[int],
        log_level: str = "INFO",
        no_file_log: bool = False,
        no_console_log: bool = False,
):
    """
    Parameters
    ----------
    glob_patterns : list[str]
        Mix of ZIP and CSV patterns/paths.
    db : Optional[str]
        DuckDB database path (None → default from common.py).
    workers : Optional[int]
        Thread pool size (default: CPU cores).
    log_level : str
        Logger level.
    """
    t_start = time.time()
    log = setup_logging("ingest_istdaten", log_level, not no_file_log, not no_console_log)

    # DB bootstrap
    init_db(db)
    con = get_db(db)

    # Discover sources
    sources = expand_globs(glob_patterns)
    if not sources:
        log.error(f"No files matched patterns: {glob_patterns}")
        print("No sources found.")
        return
    log.info(f"Discovered {len(sources)} source(s)")

    # Build processing tasks
    work_items: list[tuple[str, str, Union[Callable[[], bytes], Path]]] = []
    for p in sources:
        if p.suffix.lower() == ".zip":
            members = zip_list_members(p)
            members.sort()
            if not members:
                log.warning(f"{p.name}: no CSV members found")
                continue
            for m in members:
                # payload is a zero-arg callable that returns the member bytes
                def _make_loader(zp: Path, mem: str) -> Callable[[], bytes]:
                    return lambda: zip_read_member_bytes(zp, mem, log)

                label = f"{p.name}:{m}"
                work_items.append(("zip", label, _make_loader(p, m)))
        else:
            # payload is the Path itself (flat CSV)
            work_items.append(("flat", p.name, p))

    log.info(f"Queued {len(work_items)} CSV task(s) (flat + zip-stream)")

    out_root = Path("data/silver/ist")
    out_root.mkdir(parents=True, exist_ok=True)

    # Process in parallel
    max_workers = workers or os.cpu_count() or 4
    parquet_paths: list[str] = []
    total_rows = 0
    t0 = time.time()
    log.info(f"Processing with workers={max_workers}…")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(process_csv_task, it, out_root, log) for it in work_items]
        done = 0
        for fut in as_completed(futs):
            try:
                p, rows, label = fut.result()
                done += 1
                total_rows += rows
                if p:
                    parquet_paths.append(str(p))
                if done % 10 == 0 or done == len(work_items):
                    log.info(f"[{done}/{len(work_items)}] rows_acc={total_rows:,} elapsed={time.time() - t0:.1f}s")
            except Exception as e:
                done += 1
                log.error(f"[{done}/{len(work_items)}] worker failed: {e}")

    if not parquet_paths:
        log.warning("No Parquet written.")
        print("No Parquet written.")
        return

    log.info(f"Wrote {len(parquet_paths)} Parquet file(s) in {time.time() - t0:.1f}s | total_rows={total_rows:,}")

    # Upsert only the shards produced in this run
    upsert_monthwise(con, parquet_paths, log)

    log.info(f"Ingest OK. Duration={time.time() - t_start:.1f}s")


# ───────────────────────── CLI ───────────────────────── #

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest IstDaten by streaming ZIP members (no extraction) + flat CSVs.")
    ap.add_argument("--glob", nargs="+", required=True,
                    help='e.g.: data/raw/istdaten/*.zip data/raw/istdaten/*_istdaten.csv data/raw/istdaten/*_IstDaten.csv')
    ap.add_argument("--db", default=None, help="DuckDB path (default from common.py).")
    ap.add_argument("--workers", type=int, default=None, help="Parallel workers (default: CPU cores).")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    ap.add_argument("--no-file-log", action="store_true", help="Disable file logging.")
    ap.add_argument("--no-console-log", action="store_true", help="Disable console logging.")
    args = ap.parse_args()

    main(args.glob, args.db, args.workers, args.log_level, args.no_file_log, args.no_console_log)
