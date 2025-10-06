"""
Streamlit Dashboard — Event‑level features (TPG × Weather)

Overview
--------
This Streamlit app provides a compact, production‑ready view over the ETL outputs:

1) **Latest TPG events** read live from DuckDB (`ist_events`) with a quick
   delay calculation (estimated vs scheduled).
2) **Feature sample** loaded from the gold Parquet produced by
   `10_buid_features.py` (`features_events.parquet`). All columns are shown
   on a fixed‑size random sample for performance.
3) **Data Quality**: top‑level KPIs, a concise **Missing values** table
   (top‑N columns by %NA), and a **Coalescing analysis** table that quantifies
   how often arrival timestamps were used in place of departure timestamps
   (counts and % of total).

Inputs
------
- DuckDB warehouse path (sidebar) — defaults to `data/warehouse.duckdb`.
- Features Parquet path (sidebar) — defaults to `data/gold/features_events.parquet`.

Notes
-----
- The app does **not** modify data. It only reads from the warehouse and the
  Parquet file.
- There are **no interactive filters** in order to keep the layout clean and
  deterministic for QA. If you need exploration filters, consider adding a
  separate exploratory page.
- The sample sizes and table lengths are fixed via constants below and can be
  adjusted to your environment.
"""

from __future__ import annotations

import os
from typing import Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd
import streamlit as st

# =============================================================================
# Configuration
# =============================================================================
DB_DEFAULT: str = os.environ.get("DUCKDB_PATH", "data/warehouse.duckdb")
PARQUET_DEFAULT: str = "data/gold/features_events.parquet"

LATEST_LIMIT: int = 50  # rows shown in the "Latest events" table
SAMPLE_SIZE: int = 100  # number of rows sampled from the features Parquet
MISS_TOP_N: int = 10  # show only the TOP‑N columns with highest %NA

# Weather columns expected in features_events (used for coverage KPI)
WEATHER_COLS: List[str] = [
    "temp_c", "rain_mm", "wind_ms", "gust_ms", "wind_dir_deg",
    "humidity", "pressure_hpa", "global_rad_wm2", "sunshine_min", "dewpoint_c",
]

st.set_page_config(page_title="TPG Delay — Event Features", layout="wide")
st.title("🚋 TPG Delay Prediction — Event Features")
st.caption("Compact view: latest events, feature sample, data quality (missing values + coalescing).")


# =============================================================================
# Helpers
# =============================================================================
@st.cache_resource
def get_con(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection in read‑only mode (cached across reruns)."""
    return duckdb.connect(db_path, read_only=True)


def safe_pct(num: int, den: int) -> float:
    """Return percentage (0..100) with zero‑division safety."""
    return (100.0 * num / den) if den else float("nan")


def load_latest_events(con: duckdb.DuckDBPyConnection, limit: int) -> pd.DataFrame:
    """Query the most recent TPG events with a simple delay calculation."""
    df = con.execute(f"""
        SELECT service_date, line_text, stop_name,
               COALESCE(depart_sched_ts, arrival_sched_ts) AS sched_ts,
               COALESCE(depart_est_ts,   arrival_est_ts)   AS est_ts,
               EXTRACT(EPOCH FROM (
                   COALESCE(depart_est_ts, arrival_est_ts)
                 - COALESCE(depart_sched_ts, arrival_sched_ts)
               ))::INT AS delay_sec
        FROM ist_events
        WHERE operator_abbr = 'TPG'
          AND (product_id IN ('Bus','Tram') OR product_id IS NULL)
        ORDER BY service_date DESC, sched_ts DESC
        LIMIT {limit}
    """).df()
    if not df.empty:
        df["delay_min"] = df["delay_sec"] / 60.0
    return df


def load_features_parquet(path: str, sample_size: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load the features Parquet and return (full_df, sample_df).

    The sample is taken over all columns with a fixed seed for reproducibility.
    """
    full = pd.read_parquet(path)
    if full.empty:
        return full, full
    sample = full.sample(n=min(sample_size, len(full)), random_state=42)
    return full, sample


def compute_kpis(df: pd.DataFrame) -> Dict[str, int | float]:
    """Compute top‑level QA KPIs from the features dataframe."""
    rows_total = int(len(df))

    if {"depart_sched_ts", "depart_est_ts"}.issubset(df.columns):
        both_depart_present = int((df["depart_sched_ts"].notna() & df["depart_est_ts"].notna()).sum())
    else:
        both_depart_present = 0

    pct_any_coalesce = float("nan")
    if "any_coalesce_from_arrival" in df.columns:
        pct_any_coalesce = 100.0 * float(df["any_coalesce_from_arrival"].mean())

    unusable = 0
    if {"sched_ts", "est_ts"}.issubset(df.columns):
        unusable = int((df["sched_ts"].isna() | df["est_ts"].isna()).sum())

    full_weather_rows = None
    if set(WEATHER_COLS).issubset(df.columns):
        full_weather_rows = int((~df[WEATHER_COLS].isna()).all(axis=1).sum())

    return {
        "rows_total": rows_total,
        "both_depart_present": both_depart_present,
        "pct_any_coalesce": pct_any_coalesce,
        "unusable": unusable,
        "full_weather_rows": (full_weather_rows if full_weather_rows is not None else "—"),
    }


def missing_values_table(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Return a TOP‑N table of columns by percentage of missing values."""
    na_pct = (df.isna().mean() * 100.0).sort_values(ascending=False)
    out = na_pct.reset_index()
    out.columns = ["column", "na_percent"]
    out["na_percent"] = out["na_percent"].round(1)
    return out.head(top_n)


def coalescing_table(df: pd.DataFrame, rows_total: int) -> pd.DataFrame:
    """Return counts and percentages for coalescing‑related metrics."""
    coalesce_sched = int(df["coalesce_sched_from_arrival"].sum()) if "coalesce_sched_from_arrival" in df.columns else 0
    coalesce_est = int(df["coalesce_est_from_arrival"].sum()) if "coalesce_est_from_arrival" in df.columns else 0
    any_coalesce = int(df["any_coalesce_from_arrival"].sum()) if "any_coalesce_from_arrival" in df.columns else 0
    if {"depart_sched_ts", "depart_est_ts"}.issubset(df.columns):
        both_depart_present = int((df["depart_sched_ts"].notna() & df["depart_est_ts"].notna()).sum())
    else:
        both_depart_present = 0

    rows = [
        ("coalesce_sched_from_arrival", coalesce_sched),
        ("coalesce_est_from_arrival", coalesce_est),
        ("any_coalesce_from_arrival", any_coalesce),
        ("both_depart_present", both_depart_present),
    ]
    out = pd.DataFrame(
        {
            "metric": [k for k, _ in rows],
            "count": [v for _, v in rows],
            "percent": [safe_pct(v, rows_total) for _, v in rows],
        }
    )
    out["percent"] = out["percent"].map(lambda x: f"{x:.2f}%" if not np.isnan(x) else "—")
    return out


# =============================================================================
# Sidebar — data source paths (no filters)
# =============================================================================
with st.sidebar:
    st.header("Data sources")
    db_path = st.text_input("DuckDB path (events)", value=DB_DEFAULT)
    parquet_path = st.text_input("Features Parquet path", value=PARQUET_DEFAULT)

# =============================================================================
# Section 1 — Latest events (warehouse)
# =============================================================================
st.subheader("Latest TPG events (warehouse)")
con = get_con(db_path)

df_events = load_latest_events(con, LATEST_LIMIT)
# Show even if empty, to make the state explicit
st.dataframe(df_events, use_container_width=True)

st.divider()

# =============================================================================
# Section 2 — Feature sample (Parquet)
# =============================================================================
st.subheader("Feature sample (from Parquet)")

if not os.path.exists(parquet_path):
    st.info(
        "Features Parquet not found. Generate it with: `python src/10_build_features_v3.py` (optionally add `--asof`).")
    st.stop()

try:
    f_full, f_sample = load_features_parquet(parquet_path, SAMPLE_SIZE)
except Exception as e:
    st.error(f"Failed to read Parquet: {e}")
    st.stop()

if f_full.empty:
    st.info("Features Parquet is empty.")
else:
    st.dataframe(f_sample, use_container_width=True)
    # Optional: lightweight export of the sample for quick offline inspection
    csv_bytes = f_sample.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download sample (CSV)",
        data=csv_bytes,
        file_name="features_events_sample.csv",
        mime="text/csv",
    )

st.divider()

# =============================================================================
# Section 3 — Data Quality
# =============================================================================
st.subheader("Data Quality")

kpis = compute_kpis(f_full)

colA, colB, colC, colD, colE = st.columns(5)
with colA:
    st.metric("Rows", f"{kpis['rows_total']:,}")
with colB:
    st.metric("Both depart present", f"{kpis['both_depart_present']:,}")
with colC:
    st.metric("Any coalesce from arrival",
              f"{kpis['pct_any_coalesce']:.1f}%" if not np.isnan(kpis['pct_any_coalesce']) else "—")
with colD:
    st.metric("Unusable (sched or est missing)", f"{kpis['unusable']:,}")
with colE:
    st.metric("Rows with full weather", f"{kpis['full_weather_rows']}")

left, right = st.columns(2)

with left:
    st.markdown("**Missing values by column** *(Top‑N)*")
    na_top = missing_values_table(f_full, MISS_TOP_N)
    # Smaller table height keeps the two panels visually balanced
    st.dataframe(na_top, height=420, use_container_width=True)

with right:
    st.markdown("**Coalescing analysis — counts and % of total**")
    coa = coalescing_table(f_full, kpis["rows_total"])
    st.dataframe(coa, height=420, width='content')
