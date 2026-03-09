#!/usr/bin/env python3
"""Phase 3: consolidate batch outputs into the final study artifacts."""

import glob
import os
import re
import shutil
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from eplus_study.config import load_config


config = load_config()
paths = config["resolved_paths"]
runtime = config["runtime"]

study_folder = paths["study_dir"]
batch_dir = paths["batch_results_dir"]

mapping_path = paths["zone_mapping_path"]
lhs_path = paths["lhs_parameters_path"]
summary_out = paths["study_results_path"]
hourly_out = paths["hourly_results_path"]
timing_out = paths["worker_timings_path"]
batch_timing_out = paths["worker_batch_timings_path"]
timing_summary_out = paths["worker_timing_summary_path"]
dashboard_out = paths["dashboard_path"]

EXPORT_DASHBOARD = os.environ.get(
    "EXPORT_DASHBOARD",
    "1" if runtime.get("export_dashboard", True) else "0",
) != "0"
EXPORT_DASHBOARD_ONLY = os.environ.get("EXPORT_DASHBOARD_ONLY", "0") != "0"
HOURLY_BATCH_ROWS = int(
    os.environ.get("HOURLY_BATCH_ROWS", runtime.get("hourly_batch_rows", 50000))
)
SCHEMA_VERSION = 1


def _normalize_column_name(name):
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", str(name).strip()).strip("_").lower()
    if not normalized:
        normalized = "col"
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def _normalize_columns(frame):
    rename_map = {}
    used = set()
    for col in frame.columns:
        candidate = _normalize_column_name(col)
        base = candidate
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        rename_map[col] = candidate
        used.add(candidate)
    return frame.rename(columns=rename_map)


def _energyplus_datetime_to_timestamp(raw_value, base_year=2001):
    text = " ".join(str(raw_value).split())
    if not text:
        return pd.NaT
    date_part, time_part = text.split()
    month, day = (int(part) for part in date_part.split("/"))
    hour, minute, second = (int(part) for part in time_part.split(":"))
    if hour == 24:
        return datetime(base_year, month, day, minute=minute, second=second) + timedelta(days=1)
    return datetime(base_year, month, day, hour=hour, minute=minute, second=second)


def _replace_duckdb_table(conn, table_name, frame):
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.register(f"{table_name}_frame", frame)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}_frame")
    conn.unregister(f"{table_name}_frame")


def _default_run_stats(sim_id):
    return {
        "sim_id": int(sim_id),
        "annual_total_heating_j": 0.0,
        "nonzero_hour_count": 0,
        "nonzero_zone_observation_count": 0,
        "peak_hour_total_heating_j": 0.0,
        "peak_hour_index": 0,
    }


def _build_zone_catalog():
    mapping_df = pd.read_csv(mapping_path)
    heated_df = mapping_df.loc[mapping_df["Is_Heated"]].copy()
    heated_df["Zone_Name"] = heated_df["Zone_Name"].astype(str)
    heated_df.insert(0, "zone_id", np.arange(len(heated_df), dtype=np.int16))
    heated_df["zone_label"] = "zone_" + heated_df["Zone_Name"]
    zone_catalog = heated_df[
        ["zone_id", "Zone_Name", "zone_label", "Cluster", "Z_Level", "Area_sqm", "Is_Heated"]
    ].copy()
    return _normalize_columns(zone_catalog)


def _build_runs_table():
    lhs_df = pd.read_csv(lhs_path)
    lhs_df["SIM_ID"] = np.arange(1, len(lhs_df) + 1, dtype=np.int32)
    runs_df = lhs_df.copy()

    summary_ids = pd.Index([], dtype="int64")
    if os.path.exists(summary_out):
        summary_df = pd.read_csv(summary_out)
        summary_ids = pd.Index(summary_df["SIM_ID"].astype("int64"))
        summary_extra_cols = ["SIM_ID"] + [c for c in summary_df.columns if c not in lhs_df.columns]
        runs_df = runs_df.merge(summary_df[summary_extra_cols], on="SIM_ID", how="left")

    timing_ids = pd.Index([], dtype="int64")
    if os.path.exists(timing_out):
        timing_df = pd.read_csv(timing_out)
        timing_ids = pd.Index(timing_df["SIM_ID"].astype("int64"))
        runs_df = runs_df.merge(timing_df, on="SIM_ID", how="left")

    runs_df["has_summary"] = runs_df["SIM_ID"].isin(summary_ids)
    runs_df["has_timing"] = runs_df["SIM_ID"].isin(timing_ids)
    if "energyplus_returncode" in runs_df.columns:
        runs_df["run_status"] = np.where(
            runs_df["energyplus_returncode"].isna(),
            np.where(runs_df["has_summary"], "summary_only", "missing"),
            np.where(runs_df["energyplus_returncode"] == 0, "ok", "failed"),
        )
    else:
        runs_df["run_status"] = np.where(runs_df["has_summary"], "ok", "missing")

    runs_df.sort_values("SIM_ID", inplace=True)
    runs_df.reset_index(drop=True, inplace=True)
    runs_df = _normalize_columns(runs_df)
    runs_df["sim_id"] = runs_df["sim_id"].astype("int32")
    return runs_df


def _build_parameter_catalog(runs_df):
    descriptions = {
        "sim_id": "Simulation identifier",
        "f_wall": "Envelope aging factor for exterior walls",
        "f_roof": "Envelope aging factor for roofs",
        "f_win": "Envelope aging factor for windows",
        "setpoint": "Heating setpoint in Celsius",
        "hours": "Heating schedule hours selector",
        "inf": "Infiltration rate in air changes per hour",
        "vent": "Ventilation rate in air changes per hour",
        "misc": "Miscellaneous internal gains in W/m2",
        "shgc": "Solar heat gain coefficient",
        "s_hours": "Shading schedule hours selector",
        "s_close": "Shading close hour selector",
        "cop": "Boiler coefficient of performance",
        "kwh_sqm_ground": "Heating energy intensity for ground floor zones",
        "kwh_sqm_middle": "Heating energy intensity for middle floor zones",
        "kwh_sqm_top": "Heating energy intensity for top floor zones",
        "run_status": "Run outcome derived from EnergyPlus return code and outputs",
        "has_summary": "Whether aggregated simulation outputs were produced",
        "has_timing": "Whether timing instrumentation data was produced",
    }
    parameter_columns = {
        "f_wall", "f_roof", "f_win", "setpoint", "hours", "inf",
        "vent", "misc", "shgc", "s_hours", "s_close", "cop",
    }
    rows = []
    for col in runs_df.columns:
        if col == "sim_id":
            group_name = "identifier"
        elif col in parameter_columns:
            group_name = "parameter"
        elif col.startswith("kwh_sqm_"):
            group_name = "summary_kpi"
        elif col.endswith("_sec") or col in {"batch_id", "input_format", "energyplus_returncode"}:
            group_name = "runtime"
        elif col in {"run_status", "has_summary", "has_timing"}:
            group_name = "status"
        else:
            group_name = "metric"
        rows.append({
            "column_name": col,
            "group_name": group_name,
            "description": descriptions.get(col, ""),
        })
    return pd.DataFrame(rows)


def _load_optional_table(path, default_columns):
    if os.path.exists(path):
        return _normalize_columns(pd.read_csv(path))
    return pd.DataFrame(columns=default_columns)


def _build_hourly_dashboard_tables(conn, zone_catalog_df):
    conn.execute("DROP TABLE IF EXISTS hourly_sparse")
    conn.execute(
        "CREATE TABLE hourly_sparse (sim_id INTEGER, hour_index SMALLINT, zone_id SMALLINT, value REAL)"
    )

    empty_calendar = pd.DataFrame(columns=["hour_index", "datetime", "timestamp"])
    empty_run_stats = pd.DataFrame(columns=[
        "sim_id",
        "annual_total_heating_j",
        "nonzero_hour_count",
        "nonzero_zone_observation_count",
        "peak_hour_total_heating_j",
        "peak_hour_index",
        "peak_datetime",
        "peak_timestamp",
    ])
    if not os.path.exists(hourly_out):
        return empty_calendar, empty_run_stats, 0, 0.0

    hourly_file = pq.ParquetFile(hourly_out)
    hourly_columns = [name for name in hourly_file.schema.names if name.startswith("zone_")]
    zone_id_map = dict(zip(zone_catalog_df["zone_label"], zone_catalog_df["zone_id"]))
    missing_zone_columns = [name for name in hourly_columns if name not in zone_id_map]
    if missing_zone_columns:
        raise KeyError(
            f"Zone columns missing from zone catalog: {', '.join(sorted(missing_zone_columns))}"
        )

    sim_offsets = {}
    reference_sim_id = None
    calendar_records = {}
    run_stats = {}
    total_zone_cells = 0
    nonzero_zone_cells = 0
    sparse_rows = 0

    for batch in hourly_file.iter_batches(
        batch_size=HOURLY_BATCH_ROWS,
        columns=["sim_id", "datetime", *hourly_columns],
    ):
        batch_df = batch.to_pandas()
        if batch_df.empty:
            continue

        sim_ids = batch_df["sim_id"].to_numpy(dtype=np.int32, copy=False)
        hour_indices = np.empty(len(batch_df), dtype=np.int16)
        current_sim = None
        current_offset = 0
        for idx, sim_id in enumerate(sim_ids):
            sim_id = int(sim_id)
            if sim_id != current_sim:
                current_sim = sim_id
                current_offset = sim_offsets.get(sim_id, 0)
            hour_indices[idx] = current_offset
            current_offset += 1
            sim_offsets[sim_id] = current_offset

        if reference_sim_id is None and len(sim_ids):
            reference_sim_id = int(sim_ids[0])
        if reference_sim_id is not None:
            reference_mask = sim_ids == reference_sim_id
            if reference_mask.any():
                ref_hours = hour_indices[reference_mask]
                ref_datetimes = batch_df.loc[reference_mask, "datetime"].astype(str).tolist()
                for hour_index, raw_datetime in zip(ref_hours.tolist(), ref_datetimes):
                    calendar_records.setdefault(int(hour_index), raw_datetime)

        zone_matrix = batch_df[hourly_columns].to_numpy(dtype=np.float32, copy=False)
        nonzero_matrix = zone_matrix != 0
        nonzero_counts = nonzero_matrix.sum(axis=1).astype(np.int32)
        row_totals = zone_matrix.sum(axis=1, dtype=np.float64)

        total_zone_cells += zone_matrix.size
        nonzero_zone_cells += int(nonzero_matrix.sum())

        stats_frame = pd.DataFrame({
            "sim_id": sim_ids,
            "hour_index": hour_indices,
            "_total_heating_j": row_totals,
            "_any_nonzero": (nonzero_counts > 0).astype(np.int8),
            "_nonzero_zone_observation_count": nonzero_counts,
        })
        aggregated = stats_frame.groupby("sim_id", sort=False).agg(
            annual_total_heating_j=("_total_heating_j", "sum"),
            nonzero_hour_count=("_any_nonzero", "sum"),
            nonzero_zone_observation_count=("_nonzero_zone_observation_count", "sum"),
        )
        peak_rows = stats_frame.loc[
            stats_frame.groupby("sim_id", sort=False)["_total_heating_j"].idxmax(),
            ["sim_id", "hour_index", "_total_heating_j"],
        ]

        for sim_id, row in aggregated.iterrows():
            stats = run_stats.setdefault(int(sim_id), _default_run_stats(sim_id))
            stats["annual_total_heating_j"] += float(row["annual_total_heating_j"])
            stats["nonzero_hour_count"] += int(row["nonzero_hour_count"])
            stats["nonzero_zone_observation_count"] += int(row["nonzero_zone_observation_count"])

        for sim_id, hour_index, peak_value in peak_rows.itertuples(index=False, name=None):
            stats = run_stats.setdefault(int(sim_id), _default_run_stats(sim_id))
            peak_value = float(peak_value)
            if peak_value > stats["peak_hour_total_heating_j"]:
                stats["peak_hour_total_heating_j"] = peak_value
                stats["peak_hour_index"] = int(hour_index)

        sparse_parts = []
        for zone_col in hourly_columns:
            values = batch_df[zone_col].to_numpy(dtype=np.float32, copy=False)
            mask = values != 0
            if not mask.any():
                continue
            sparse_parts.append(pd.DataFrame({
                "sim_id": sim_ids[mask].astype(np.int32, copy=False),
                "hour_index": hour_indices[mask].astype(np.int16, copy=False),
                "zone_id": np.full(int(mask.sum()), zone_id_map[zone_col], dtype=np.int16),
                "value": values[mask].astype(np.float32, copy=False),
            }))

        if sparse_parts:
            sparse_df = pd.concat(sparse_parts, ignore_index=True)
            sparse_df.sort_values(["sim_id", "hour_index", "zone_id"], inplace=True)
            sparse_rows += len(sparse_df)
            conn.register("hourly_sparse_chunk", sparse_df)
            conn.execute("INSERT INTO hourly_sparse SELECT * FROM hourly_sparse_chunk")
            conn.unregister("hourly_sparse_chunk")

    calendar_df = pd.DataFrame(
        sorted(calendar_records.items()),
        columns=["hour_index", "datetime"],
    )
    if not calendar_df.empty:
        calendar_df["hour_index"] = calendar_df["hour_index"].astype(np.int16)
        calendar_df["timestamp"] = pd.to_datetime(
            [_energyplus_datetime_to_timestamp(value) for value in calendar_df["datetime"]]
        )

    run_stats_df = pd.DataFrame(sorted(run_stats.values(), key=lambda row: row["sim_id"]))
    if run_stats_df.empty:
        run_stats_df = empty_run_stats.copy()
    else:
        calendar_lookup = dict(zip(calendar_df["hour_index"], calendar_df["datetime"])) if not calendar_df.empty else {}
        timestamp_lookup = (
            dict(zip(calendar_df["hour_index"], calendar_df["timestamp"]))
            if not calendar_df.empty else {}
        )
        run_stats_df["peak_datetime"] = run_stats_df["peak_hour_index"].map(calendar_lookup)
        run_stats_df["peak_timestamp"] = run_stats_df["peak_hour_index"].map(timestamp_lookup)
        run_stats_df["sim_id"] = run_stats_df["sim_id"].astype("int32")
        run_stats_df["nonzero_hour_count"] = run_stats_df["nonzero_hour_count"].astype("int32")
        run_stats_df["nonzero_zone_observation_count"] = run_stats_df["nonzero_zone_observation_count"].astype("int32")
        run_stats_df["peak_hour_index"] = run_stats_df["peak_hour_index"].astype("int16")

    zero_fraction = 0.0
    if total_zone_cells:
        zero_fraction = 1.0 - (nonzero_zone_cells / total_zone_cells)
    return calendar_df, run_stats_df, sparse_rows, zero_fraction


def _ensure_duckdb_available():
    try:
        import duckdb  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Dashboard export requires duckdb. Install dependencies from requirements.txt "
            "or set EXPORT_DASHBOARD=0 to skip the additive bundle."
        ) from exc


def _export_dashboard_bundle(export_mode):
    import duckdb

    print(" Exporting dashboard bundle...", flush=True)
    if os.path.exists(dashboard_out):
        os.remove(dashboard_out)

    zone_catalog_df = _build_zone_catalog()
    runs_df = _build_runs_table()
    parameter_catalog_df = _build_parameter_catalog(runs_df)
    batch_stats_df = _load_optional_table(batch_timing_out, ["batch_id"])
    study_stats_df = _load_optional_table(timing_summary_out, ["n_batches", "n_sims"])

    conn = duckdb.connect(dashboard_out)
    try:
        conn.execute(f"PRAGMA threads={max(os.cpu_count() or 1, 1)}")
        _replace_duckdb_table(conn, "runs", runs_df)
        _replace_duckdb_table(conn, "zone_catalog", zone_catalog_df)
        _replace_duckdb_table(conn, "parameter_catalog", parameter_catalog_df)
        _replace_duckdb_table(conn, "batch_stats", batch_stats_df)
        _replace_duckdb_table(conn, "study_stats", study_stats_df)

        calendar_df, run_stats_df, sparse_rows, zero_fraction = _build_hourly_dashboard_tables(
            conn, zone_catalog_df
        )
        _replace_duckdb_table(conn, "calendar", calendar_df)
        _replace_duckdb_table(conn, "run_stats", run_stats_df)

        manifest_df = pd.DataFrame([{
            "schema_version": SCHEMA_VERSION,
            "created_at_utc": datetime.now(timezone.utc),
            "export_mode": export_mode,
            "study_folder": study_folder,
            "n_runs": int(len(runs_df)),
            "n_heated_zones": int(len(zone_catalog_df)),
            "n_calendar_hours": int(len(calendar_df)),
            "n_sparse_rows": int(sparse_rows),
            "hourly_zero_fraction": round(float(zero_fraction), 6),
            "has_timing_data": bool(os.path.exists(timing_out)),
            "source_hourly_path": hourly_out if os.path.exists(hourly_out) else "",
        }])
        _replace_duckdb_table(conn, "manifest", manifest_df)

        conn.execute("DROP VIEW IF EXISTS run_dashboard")
        conn.execute(
            "CREATE VIEW run_dashboard AS "
            "SELECT r.*, s.annual_total_heating_j, s.nonzero_hour_count, "
            "s.nonzero_zone_observation_count, s.peak_hour_total_heating_j, "
            "s.peak_hour_index, s.peak_datetime, s.peak_timestamp "
            "FROM runs r LEFT JOIN run_stats s USING (sim_id)"
        )
        conn.execute("DROP VIEW IF EXISTS hourly_dashboard")
        conn.execute(
            "CREATE VIEW hourly_dashboard AS "
            "SELECT h.sim_id, h.hour_index, c.datetime, c.timestamp, "
            "z.zone_id, z.zone_name, z.cluster, z.area_sqm, h.value "
            "FROM hourly_sparse h "
            "JOIN calendar c USING (hour_index) "
            "JOIN zone_catalog z USING (zone_id)"
        )
    finally:
        conn.close()

    file_mb = os.path.getsize(dashboard_out) / (1024 * 1024)
    print(f"  Dashboard bundle -> {dashboard_out} ({file_mb:.1f} MB)", flush=True)


def main():
    if EXPORT_DASHBOARD:
        _ensure_duckdb_available()

    for output_path in [summary_out, hourly_out, timing_out, batch_timing_out, timing_summary_out, dashboard_out]:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if not EXPORT_DASHBOARD_ONLY:
        if os.path.exists(summary_out):
            print(f" {summary_out} already exists. Delete to rebuild.")
            return 1
        if os.path.exists(hourly_out):
            print(f" {hourly_out} already exists. Delete to rebuild.")
            return 1

        print(" Merging summary CSVs...", flush=True)
        summary_files = sorted(glob.glob(os.path.join(batch_dir, "summary_*.csv")))
        if not summary_files:
            print(" ERROR: No summary files found in batch_results/")
            return 1

        dfs = [pd.read_csv(f) for f in summary_files]
        df_summary = pd.concat(dfs, ignore_index=True)
        df_summary.sort_values("SIM_ID", inplace=True)
        df_summary.to_csv(summary_out, index=False)
        print(f"  {len(df_summary)} rows -> {summary_out}", flush=True)

        print(" Merging hourly Parquets...", flush=True)
        hourly_files = sorted(glob.glob(os.path.join(batch_dir, "hourly_*.parquet")))
        if not hourly_files:
            print(" WARNING: No hourly files found. Skipping hourly merge.")
        else:
            writer = None
            n_merged = 0
            for hf in hourly_files:
                table = pq.read_table(hf)
                if writer is None:
                    writer = pq.ParquetWriter(hourly_out, table.schema, compression="zstd")
                writer.write_table(table)
                n_merged += 1
                if n_merged % 100 == 0:
                    print(f"   {n_merged}/{len(hourly_files)} batches merged...", flush=True)
            if writer:
                writer.close()
            file_mb = os.path.getsize(hourly_out) / (1024 * 1024)
            print(f"  {n_merged} batches -> {hourly_out} ({file_mb:.1f} MB)", flush=True)

        print(" Merging worker timing CSVs...", flush=True)
        timing_files = sorted(glob.glob(os.path.join(batch_dir, "timing_detail_*.csv")))
        batch_timing_files = sorted(glob.glob(os.path.join(batch_dir, "timing_summary_*.csv")))

        if not timing_files:
            print(" WARNING: No timing files found. Skipping timing merge.", flush=True)
        else:
            timing_dfs = [pd.read_csv(f) for f in timing_files]
            df_timing = pd.concat(timing_dfs, ignore_index=True)
            df_timing.sort_values("SIM_ID", inplace=True)
            df_timing.to_csv(timing_out, index=False)
            print(f"  {len(df_timing)} timing rows -> {timing_out}", flush=True)

            summary_stats = {
                "n_batches": int(df_timing["batch_id"].nunique()),
                "n_sims": int(len(df_timing)),
            }
            for col in [c for c in df_timing.columns if c.endswith("_sec")]:
                summary_stats[f"median_{col}"] = round(df_timing[col].median(), 6)
                summary_stats[f"mean_{col}"] = round(df_timing[col].mean(), 6)

            if batch_timing_files:
                batch_timing_dfs = [pd.read_csv(f) for f in batch_timing_files]
                df_batch_timing = pd.concat(batch_timing_dfs, ignore_index=True)
                df_batch_timing.sort_values("batch_id", inplace=True)
                df_batch_timing.to_csv(batch_timing_out, index=False)
                print(
                    f"  {len(df_batch_timing)} batch timing rows -> {batch_timing_out}",
                    flush=True,
                )
                for col in [
                    "input_load_sec",
                    "baseline_idf_load_sec",
                    "baseline_model_load_sec",
                    "summary_write_sec",
                    "hourly_concat_sec",
                    "hourly_write_sec",
                    "batch_total_sec",
                ]:
                    if col in df_batch_timing.columns:
                        summary_stats[f"total_{col}"] = round(df_batch_timing[col].sum(), 6)

            pd.DataFrame([summary_stats]).to_csv(timing_summary_out, index=False)
            print(f"  Timing summary -> {timing_summary_out}", flush=True)
    else:
        print(" Dashboard-only export from existing consolidated outputs...", flush=True)

    if EXPORT_DASHBOARD:
        _export_dashboard_bundle(
            export_mode="dashboard_only" if EXPORT_DASHBOARD_ONLY else "full_consolidation"
        )

    if not EXPORT_DASHBOARD_ONLY:
        print(" Cleaning up batch_results/...", flush=True)
        shutil.rmtree(batch_dir)

    print(" Phase 3 complete.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
