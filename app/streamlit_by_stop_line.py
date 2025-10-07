#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# app/streamlit_by_stop_line.py
"""
Streamlit Dashboard — Aggregated features by Stop × Line (10-min bins)

Overview
--------
This app visualizes the aggregated table produced by the ETL step
`11_build_features_by_stop_line.py`. Each row represents metrics for a specific
(line_text, stop_code) and a 10-minute time bin (`sched_bin`).

What you can do here
--------------------
- Filter by line(s), stop(s), and date range
- Pick a metric to visualize as a time series (e.g., avg delay, share of late trips)
- Inspect an hour × day heatmap of average delays
- Download the filtered slice as CSV

Inputs
------
- Either a Parquet file at: data/gold/features_by_stop_line.parquet
- Or a DuckDB warehouse containing table: features_by_stop_line

Expected columns (from the ETL)
-------------------------------
- Keys: line_text, stop_code, stop_name, stop_key, sched_bin (UTC timestamp)
- Traffic: n_trips, delay_avg_min, delay_p50_min, delay_p90_min, share_late_ge2
- Weather (aggregated): temp_c_mean, rain_mm_mean, rain_mm_max, wind_ms_mean, gust_ms_mean,
                        humidity_mean, pressure_hpa_mean, global_rad_wm2_mean, sunshine_min_mean, dewpoint_c_mean
- Coalesce flag (aggregated): share_coalesce (optional in UI, useful for QA)

Notes
-----
- All timestamps are treated as UTC and displayed as-is.
- `share_late_ge2` and `share_coalesce` are fractions in [0,1]; KPIs display them as percentages.
"""

from __future__ import annotations

import os
from typing import Optional, List

import duckdb
import pandas as pd
import streamlit as st
import altair as alt

# =============================================================================
# Page configuration
# =============================================================================
st.set_page_config(page_title="TPG · Features by Stop × Line", layout="wide")
st.title("📈 TPG · Features by Stop × Line (10-min bin)")
st.caption("Aggregated metrics per (line × stop × 10-minute slot).")

# =============================================================================
# Helpers
# =============================================================================
@st.cache_resource
def get_con(db_path: Optional[str]) -> duckdb.DuckDBPyConnection:
    """
    Create a read-only DuckDB connection.
    Cached across reruns to avoid opening multiple connections.
    """
    return duckdb.connect(db_path or "data/warehouse.duckdb", read_only=True)

def load_table_or_parquet(con: duckdb.DuckDBPyConnection, parquet_path: Optional[str] = None) -> pd.DataFrame:
    """
    Load the aggregated dataset from a Parquet file if provided and exists,
    otherwise from the DuckDB table `features_by_stop_line`.

    The function fails fast with a Streamlit error if neither source is available.
    """
    if parquet_path and os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
        return df

    exists = con.execute("""
      SELECT COUNT(*) FROM information_schema.tables
      WHERE table_name = 'features_by_stop_line'
    """).fetchone()[0]
    if not exists:
        st.error("Table 'features_by_stop_line' not found and no Parquet provided.")
        st.stop()

    return con.execute("SELECT * FROM features_by_stop_line").df()

def enhance_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add helper time columns derived from `sched_bin`:
      - date  (YYYY-MM-DD)
      - hour  (0..23)
      - dow   (0=Mon .. 6=Sun)

    Assumes `sched_bin` is a UTC timestamp string/ts; coerces to pandas datetime[UTC].
    """
    out = df.copy()
    out["sched_bin"] = pd.to_datetime(out["sched_bin"], utc=True, errors="coerce")
    out["date"] = out["sched_bin"].dt.date
    out["hour"] = out["sched_bin"].dt.hour
    out["dow"]  = out["sched_bin"].dt.dayofweek
    return out

def metric_human_name(key: str) -> str:
    """
    Human-friendly labels for y-axis / selectors.
    Extend this mapping as you add new aggregated columns.
    """
    labels = {
        "delay_avg_min":      "Average delay (min)",
        "delay_p50_min":      "Median delay (min)",
        "delay_p90_min":      "P90 delay (min)",
        "share_late_ge2":     "Share delays ≥2min",
        "rain_mm_mean":       "Rain mean (mm/10min)",
        "rain_mm_max":        "Rain max (mm/10min)",
        "wind_ms_mean":       "Wind mean (m/s)",
        "gust_ms_mean":       "Gust mean (m/s)",
        "temp_c_mean":        "Temperature mean (°C)",
        "humidity_mean":      "Humidity mean (%)",
        "pressure_hpa_mean":  "Pressure mean (hPa)",
        "global_rad_wm2_mean":"Global radiation (W/m²)",
        "sunshine_min_mean":  "Sunshine mean (min/10min)",
        "dewpoint_c_mean":    "Dew point mean (°C)",
        # Optional QA metric:
        "share_coalesce":     "Share coalesced (arrival→depart)",
    }
    return labels.get(key, key)

def safe_pct(v: float) -> str:
    """Format a fraction in [0,1] as a percentage string with one decimal place."""
    try:
        return f"{100.0 * float(v):.1f}%"
    except Exception:
        return "—"

# =============================================================================
# Sidebar — data sources
# =============================================================================
with st.sidebar:
    st.header("Data")
    db_path = st.text_input("DuckDB path", value="data/warehouse.duckdb")
    parquet_path = st.text_input("Or Parquet (optional)", value="data/gold/features_by_stop_line.parquet")
    parquet_path = parquet_path or None

con = get_con(db_path)
df = load_table_or_parquet(con, parquet_path)
if df.empty:
    st.info("No data to display.")
    st.stop()

df = enhance_time(df)

# =============================================================================
# Sidebar — filters & metric selection
# =============================================================================
with st.sidebar:
    st.header("Filters")

    # 1) Line filter (multiselect)
    lines: List[str] = sorted(df["line_text"].dropna().astype(str).unique().tolist())
    line_sel = st.multiselect("Line(s)", lines, default=lines[:1] if lines else [])

    # 2) Stop filter (dependent on selected lines)
    #    We use stop_key as the actual selection key and render stop_name as label.
    stop_map = (
        df.loc[df["line_text"].isin(line_sel), ["stop_key", "stop_name"]]
          .dropna().drop_duplicates().sort_values("stop_name")
    )
    options = stop_map["stop_key"].tolist()
    labels  = stop_map["stop_name"].tolist()
    name_for = dict(zip(options, labels))

    stop_sel = st.multiselect(
        "Stop(s)",
        options=options,
        default=options[:1] if options else [],
        format_func=lambda x: name_for.get(x, str(x)),
        help="Use the search box to find a stop by name."
    )

    # 3) Date range (inclusive) derived from available data
    date_min, date_max = df["date"].min(), df["date"].max()
    dr = st.date_input(
        "Date range",
        value=(date_min, date_max),
        min_value=date_min, max_value=date_max
    )

    # 4) Metric to plot (time series)
    metric_choices = [
        "delay_avg_min", "delay_p50_min", "delay_p90_min",
        "share_late_ge2",
        "rain_mm_mean", "rain_mm_max",
        "wind_ms_mean", "gust_ms_mean",
        "temp_c_mean", "humidity_mean",
        "global_rad_wm2_mean", "sunshine_min_mean", "dewpoint_c_mean",
        # Uncomment if you want to expose the QA metric in the UI:
        "share_coalesce",
    ]
    y_metric = st.selectbox("Metric", metric_choices, index=0, format_func=metric_human_name)

# =============================================================================
# Apply filters
# =============================================================================
mask = pd.Series(True, index=df.index)
if line_sel:
    mask &= df["line_text"].isin(line_sel)
if stop_sel:
    mask &= df["stop_key"].isin(stop_sel)
if isinstance(dr, (list, tuple)) and len(dr) == 2:
    d0, d1 = pd.to_datetime(dr[0]), pd.to_datetime(dr[1])
    mask &= (pd.to_datetime(df["date"]) >= d0) & (pd.to_datetime(df["date"]) <= d1)

view = df.loc[mask].sort_values("sched_bin").copy()
st.caption(f"{len(view):,} rows after filters")

# =============================================================================
# KPIs
# =============================================================================
colA, colB, colC, colD = st.columns(4)
with colA:
    trips = int(view["n_trips"].sum()) if "n_trips" in view else 0
    st.metric("Trips", f"{trips:,}")
with colB:
    st.metric("Avg delay (min)",
              f"{view['delay_avg_min'].mean():.2f}" if "delay_avg_min" in view and not view.empty else "—")
with colC:
    st.metric("P90 delay (min)",
              f"{view['delay_p90_min'].mean():.2f}" if "delay_p90_min" in view and not view.empty else "—")
with colD:
    st.metric("Share ≥2min",
              safe_pct(view["share_late_ge2"].mean()) if "share_late_ge2" in view and not view.empty else "—")

st.divider()

# =============================================================================
# Time series — metric over time (grouped by stop)
# =============================================================================
st.subheader(f"Time series — {metric_human_name(y_metric)}")
if not view.empty and y_metric in view.columns:
    # Build a line chart over time, one line per stop_key (legend uses stop_name if unique).
    # Altair handles interactive hover tooltips nicely.
    label_col = "stop_name" if "stop_name" in view.columns else "stop_key"
    timeseries = (
        alt.Chart(view)
        .mark_line(point=False)
        .encode(
            x=alt.X("sched_bin:T", title="Time (UTC)"),
            y=alt.Y(f"{y_metric}:Q", title=metric_human_name(y_metric)),
            color=alt.Color(f"{label_col}:N", title="Stop"),
            tooltip=[
                alt.Tooltip("sched_bin:T", title="Time (UTC)"),
                alt.Tooltip("line_text:N", title="Line"),
                alt.Tooltip(f"{label_col}:N", title="Stop"),
                alt.Tooltip(f"{y_metric}:Q", title=metric_human_name(y_metric), format=".3f"),
                alt.Tooltip("n_trips:Q", title="#Trips"),
            ],
        )
        .properties(height=280)
        .interactive()
    )
    st.altair_chart(timeseries, use_container_width=True)
else:
    st.info("Not enough data for time series.")

# =============================================================================
# Heatmap — hour × day (average delay)
# =============================================================================
st.subheader("Hour × Day heatmap (avg delay)")
if not view.empty and "delay_avg_min" in view.columns:
    dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    heat = (
        view.assign(dow_name=view["dow"].map(dow_map))
            .groupby(["dow", "dow_name", "hour"], as_index=False)["delay_avg_min"].mean()
    )
    heat_chart = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour"),
            y=alt.Y("dow_name:O", sort=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], title="Day"),
            color=alt.Color("delay_avg_min:Q", title="Avg delay (min)"),
            tooltip=[
                alt.Tooltip("dow_name:O", title="Day"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("delay_avg_min:Q", title="Avg delay (min)", format=".2f"),
            ],
        )
        .properties(height=240)
    )
    st.altair_chart(heat_chart, use_container_width=True)
else:
    st.info("Not enough data for heatmap.")

# =============================================================================
# Details table + download
# =============================================================================
st.subheader("Details")
if not view.empty:
    st.dataframe(view.sort_values(["sched_bin", "line_text", "stop_key"]).reset_index(drop=True))
    csv = view.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered (CSV)", data=csv,
                       file_name="by_stop_line_filtered.csv", mime="text/csv")
else:
    st.info("No rows for the current filters.")
