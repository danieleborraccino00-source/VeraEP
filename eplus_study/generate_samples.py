#!/usr/bin/env python3
"""
Phase 1: generate the LHS matrix, zone mapping, and epJSON cache.

Author: Sanjay Somanath <sanjay.somanath@chalmers.se>
Division of Sustainable Built Environments, Chalmers University of Technology

License: MIT
"""

import os

from eppy.modeleditor import IDF

from eplus_study.config import load_config
from eplus_study.epjson_parametrics import ensure_baseline_epjson
from eplus_study.idf_parametrics import build_zone_mapping, generate_lhs_samples


config = load_config()
paths = config["resolved_paths"]
runtime = config["runtime"]

idd_file = paths["idd_file"]
converter_path = paths["converter_path"]
IDF.setiddname(idd_file)

idf_baseline_path = paths["idf_path"]
epjson_baseline_path = paths["epjson_cache_path"]
n_sim = int(os.environ.get("N_SIM", 1000))
lhs_seed = int(os.environ.get("LHS_SEED", runtime.get("lhs_seed", 42)))


def main():
    study_folder = paths["study_dir"]
    os.makedirs(study_folder, exist_ok=True)

    print(" Generating zone mapping from baseline IDF...", flush=True)
    mapping_path = paths["zone_mapping_path"]
    os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
    mapping_df = build_zone_mapping(idf_baseline_path, mapping_path)

    n_ground = len(mapping_df[mapping_df["Cluster"] == "GROUND"])
    n_middle = len(mapping_df[mapping_df["Cluster"] == "MIDDLE"])
    n_top = len(mapping_df[mapping_df["Cluster"] == "TOP"])
    print(
        f" Mapped Building: {n_ground} ground, {n_middle} middle, {n_top} top zones.",
        flush=True,
    )

    print(f" Generating LHS matrix with {n_sim} samples...", flush=True)
    lhs_df = generate_lhs_samples(n_sim, seed=lhs_seed)
    lhs_csv_path = paths["lhs_parameters_path"]
    os.makedirs(os.path.dirname(lhs_csv_path), exist_ok=True)
    lhs_df.to_csv(lhs_csv_path, index=False)

    batch_dir = paths["batch_results_dir"]
    os.makedirs(batch_dir, exist_ok=True)

    print(" Ensuring baseline epJSON cache...", flush=True)
    ensure_baseline_epjson(idf_baseline_path, epjson_baseline_path, converter_path)
    print(f" Baseline epJSON ready: {epjson_baseline_path}", flush=True)

    print(f" LHS matrix saved: {lhs_csv_path} ({len(lhs_df)} rows)", flush=True)
    print(" Phase 1 complete.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
