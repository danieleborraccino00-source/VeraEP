#!/usr/bin/env python3
"""
Phase 2: run one worker batch in node-local scratch and write batch outputs.

Author: Sanjay Somanath <sanjay.somanath@chalmers.se>
Division of Sustainable Built Environments, Chalmers University of Technology

License: MIT
"""

import atexit
import json
import os
import shutil
import subprocess
import tempfile
import time

import pandas as pd

from eplus_study.config import load_config
from eplus_study.epjson_parametrics import (apply_parametric_inputs_to_epjson,
                                            configure_heating_output_variables_epjson,
                                            ensure_baseline_epjson, load_epjson_text,
                                            write_epjson)


def _elapsed_seconds(start_time):
    return round(time.perf_counter() - start_time, 6)


config = load_config()
paths = config["resolved_paths"]
runtime = config["runtime"]

EP_EXE = paths["energyplus_exe"]
CONVERTER = paths["converter_path"]

epw_file = paths["epw_path"]
idf_baseline_path = paths["idf_path"]
epjson_baseline_path = paths["epjson_cache_path"]
study_folder = paths["study_dir"]
batch_dir = paths["batch_results_dir"]

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))
batch_id = int(os.environ.get("SLURM_ARRAY_TASK_ID", 1))
tmpdir = os.environ.get("TMPDIR")
if not tmpdir:
    tmpdir = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, tmpdir, True)
ENABLE_TIMING = os.environ.get(
    "ENABLE_TIMING",
    "1" if runtime.get("enable_timing", True) else "0",
) != "0"
input_format = "epjson"


def main():
    batch_started = time.perf_counter()

    input_load_started = time.perf_counter()
    lhs_df = pd.read_csv(paths["lhs_parameters_path"])
    mapping_df = pd.read_csv(paths["zone_mapping_path"])
    input_load_sec = _elapsed_seconds(input_load_started)
    heated_zones = (
        mapping_df.loc[mapping_df["Is_Heated"], ["Zone_Name", "Cluster", "Area_sqm"]]
        .assign(Zone_Name=lambda frame: frame["Zone_Name"].astype(str))
        .to_dict("records")
    )

    n_sim = len(lhs_df)
    start_sim = (batch_id - 1) * BATCH_SIZE + 1
    end_sim = min(batch_id * BATCH_SIZE, n_sim)
    batch_size = end_sim - start_sim + 1

    print(
        f" Batch {batch_id}: sims {start_sim}-{end_sim} (of {n_sim}) using {input_format}",
        flush=True,
    )

    baseline_load_started = time.perf_counter()
    ensure_baseline_epjson(idf_baseline_path, epjson_baseline_path, CONVERTER)
    baseline_epjson_text = load_epjson_text(epjson_baseline_path)
    baseline_model_load_sec = _elapsed_seconds(baseline_load_started)

    summary_rows = []
    hourly_chunks = []
    timing_rows = []
    completed = 0

    for sim_id in range(start_sim, end_sim + 1):
        sim_started = time.perf_counter()
        timing_row = {
            "batch_id": batch_id,
            "SIM_ID": sim_id,
            "input_format": input_format,
        }
        row = lhs_df.iloc[sim_id - 1]

        model_load_started = time.perf_counter()
        model = json.loads(baseline_epjson_text)
        timing_row["model_load_sec"] = _elapsed_seconds(model_load_started)

        model_edit_started = time.perf_counter()
        apply_parametric_inputs_to_epjson(
            model, mapping_df,
            row["f_wall"], row["f_roof"], row["f_win"],
            row["setpoint"], row["hours"], row["inf"],
            row["vent"], row["misc"], row["cop"],
            row["shgc"], row["s_hours"], row["s_close"])
        timing_row["apply_model_edits_sec"] = _elapsed_seconds(model_edit_started)

        configure_started = time.perf_counter()
        configure_heating_output_variables_epjson(model)
        timing_row["configure_output_sec"] = _elapsed_seconds(configure_started)

        tmp_run = os.path.join(tmpdir, f"Run_{sim_id}")
        os.makedirs(tmp_run, exist_ok=True)
        input_path = os.path.join(tmp_run, "in.epJSON")
        write_started = time.perf_counter()
        write_epjson(model, input_path)
        timing_row["write_input_sec"] = _elapsed_seconds(write_started)

        energyplus_started = time.perf_counter()
        proc = subprocess.run(
            [EP_EXE, "-w", epw_file, "-d", tmp_run, "-p", "eplus", "-r", input_path],
            capture_output=True,
        )
        timing_row["energyplus_sec"] = _elapsed_seconds(energyplus_started)
        timing_row["energyplus_returncode"] = proc.returncode

        parse_started = time.perf_counter()
        csv_path = os.path.join(tmp_run, "eplusout.csv")
        if os.path.exists(csv_path):
            read_output_started = time.perf_counter()
            df_res = pd.read_csv(csv_path)
            timing_row["read_output_csv_sec"] = _elapsed_seconds(read_output_started)
            heat_cols = [c for c in df_res.columns if "Heating Energy" in c]
            heat_cols_by_zone = {}
            for col in heat_cols:
                zone_id = col.split(" IDEAL LOADS AIR")[0].strip()
                heat_cols_by_zone.setdefault(zone_id, []).append(col)
            timing_row["heating_column_count"] = len(heat_cols)
            timing_row["output_rows"] = len(df_res)

            summary_started = time.perf_counter()
            res_row = row.to_dict()
            res_row["SIM_ID"] = sim_id
            final_list = []
            for zone in heated_zones:
                z_id = zone["Zone_Name"]
                cols = heat_cols_by_zone.get(z_id, [])
                if cols:
                    tot_kwh = (df_res[cols].sum().sum() / 3_600_000.0) / row["cop"]
                    final_list.append({
                        "Floor": zone["Cluster"],
                        "kWh_sqm": round(tot_kwh / zone["Area_sqm"], 2),
                    })
            if final_list:
                df_temp = pd.DataFrame(final_list)
                for level in ["GROUND", "MIDDLE", "TOP"]:
                    subset = df_temp[df_temp["Floor"] == level]["kWh_sqm"]
                    res_row[f"kWh_sqm_{level}"] = round(subset.mean(), 2) if len(subset) else 0.0
            summary_rows.append(res_row)
            timing_row["summary_aggregation_sec"] = _elapsed_seconds(summary_started)

            hourly_started = time.perf_counter()
            if heat_cols:
                df_heat = df_res[["Date/Time"] + heat_cols].copy()
                df_heat.insert(0, "sim_id", sim_id)
                rename = {"Date/Time": "datetime"}
                for zone_id, cols in heat_cols_by_zone.items():
                    for col in cols:
                        rename[col] = f"zone_{zone_id}"
                df_heat.rename(columns=rename, inplace=True)
                float_cols = df_heat.select_dtypes("float64").columns
                df_heat[float_cols] = df_heat[float_cols].astype("float32")
                df_heat["sim_id"] = df_heat["sim_id"].astype("int32")
                hourly_chunks.append(df_heat)
            timing_row["hourly_prep_sec"] = _elapsed_seconds(hourly_started)
        else:
            print(f"  WARNING: No output for sim {sim_id}", flush=True)
            timing_row["read_output_csv_sec"] = 0.0
            timing_row["heating_column_count"] = 0
            timing_row["output_rows"] = 0
            timing_row["summary_aggregation_sec"] = 0.0
            timing_row["hourly_prep_sec"] = 0.0
        timing_row["parse_results_sec"] = _elapsed_seconds(parse_started)

        cleanup_started = time.perf_counter()
        shutil.rmtree(tmp_run, ignore_errors=True)
        timing_row["cleanup_sec"] = _elapsed_seconds(cleanup_started)
        timing_row["sim_total_sec"] = _elapsed_seconds(sim_started)
        if ENABLE_TIMING:
            timing_rows.append(timing_row)
        completed += 1
        if completed % 10 == 0 or sim_id == end_sim:
            print(f"  Batch {batch_id}: completed {completed}/{batch_size} sims", flush=True)

    os.makedirs(batch_dir, exist_ok=True)

    summary_write_sec = 0.0
    if summary_rows:
        summary_write_started = time.perf_counter()
        pd.DataFrame(summary_rows).to_csv(
            os.path.join(batch_dir, f"summary_{batch_id}.csv"), index=False)
        summary_write_sec = _elapsed_seconds(summary_write_started)

    hourly_concat_sec = 0.0
    hourly_write_sec = 0.0
    if hourly_chunks:
        hourly_concat_started = time.perf_counter()
        df_hourly = pd.concat(hourly_chunks, ignore_index=True)
        hourly_concat_sec = _elapsed_seconds(hourly_concat_started)
        hourly_write_started = time.perf_counter()
        df_hourly.to_parquet(
            os.path.join(batch_dir, f"hourly_{batch_id}.parquet"),
            engine="pyarrow", compression="zstd", index=False)
        hourly_write_sec = _elapsed_seconds(hourly_write_started)

    if ENABLE_TIMING and timing_rows:
        df_timing = pd.DataFrame(timing_rows)
        timing_out = os.path.join(batch_dir, f"timing_detail_{batch_id}.csv")
        df_timing.to_csv(timing_out, index=False)
        timing_sec_columns = sorted(
            col for col in df_timing.columns if col.endswith("_sec"))

        batch_timing = {
            "batch_id": batch_id,
            "input_format": input_format,
            "start_sim": start_sim,
            "end_sim": end_sim,
            "n_sims": completed,
            "tmpdir": tmpdir,
            "input_load_sec": input_load_sec,
            "baseline_model_load_sec": baseline_model_load_sec,
            "summary_write_sec": summary_write_sec,
            "hourly_concat_sec": hourly_concat_sec,
            "hourly_write_sec": hourly_write_sec,
            "batch_total_sec": _elapsed_seconds(batch_started),
        }
        for col in timing_sec_columns:
            batch_timing[f"median_{col}"] = round(df_timing[col].median(), 6)
            batch_timing[f"mean_{col}"] = round(df_timing[col].mean(), 6)

        pd.DataFrame([batch_timing]).to_csv(
            os.path.join(batch_dir, f"timing_summary_{batch_id}.csv"), index=False)
        print(
            f" Batch {batch_id} timing medians ({input_format}): "
            f"model_load={batch_timing['median_model_load_sec']:.3f}s, "
            f"model_edits={batch_timing['median_apply_model_edits_sec']:.3f}s, "
            f"energyplus={batch_timing['median_energyplus_sec']:.3f}s, "
            f"total={batch_timing['median_sim_total_sec']:.3f}s",
            flush=True,
        )

    print(f" Batch {batch_id} complete: {completed}/{end_sim - start_sim + 1} sims.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
