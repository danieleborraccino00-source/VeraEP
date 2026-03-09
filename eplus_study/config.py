#!/usr/bin/env python3
"""
Shared configuration loader and CLI helpers for the EnergyPlus study.

Author: Sanjay Somanath <sanjay.somanath@chalmers.se>
Division of Sustainable Built Environments, Chalmers University of Technology

License: MIT
"""

import argparse
import json
import os
import shlex
import shutil
import sys
from copy import deepcopy
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "project_config.json"
DEFAULT_OVERRIDE_PATH = REPO_ROOT / "project_config.local.json"
PHASE_NAMES = ("generate", "worker", "consolidate")
PHASE_LOG_PATTERNS = {
    "generate": ("generate-%j.out", "generate-%j.err"),
    "worker": ("worker-%A_%a.out", "worker-%A_%a.err"),
    "consolidate": ("consolidate-%j.out", "consolidate-%j.err"),
}


def _deep_merge(base, override):
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _resolve_path(root: Path, value: str) -> str:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = root / candidate
    return str(candidate.resolve())


def _resolve_within_path(base_path: str, value: str) -> str:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = Path(base_path) / candidate
    return str(candidate.resolve())


def _bool_to_env(value) -> str:
    return "1" if bool(value) else "0"


def load_config():
    config_path = Path(os.environ.get("EPLUS_CONFIG", DEFAULT_CONFIG_PATH))
    if not config_path.is_absolute():
        config_path = (REPO_ROOT / config_path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = _load_json(config_path)

    override_path = Path(os.environ.get("EPLUS_CONFIG_LOCAL", DEFAULT_OVERRIDE_PATH))
    if not override_path.is_absolute():
        override_path = (REPO_ROOT / override_path).resolve()
    if override_path.exists():
        config = _deep_merge(config, _load_json(override_path))

    project = config.setdefault("project", {})
    inputs = config.setdefault("inputs", {})
    energyplus = config.setdefault("energyplus", {})
    slurm = config.setdefault("slurm", {})
    runtime = config.setdefault("runtime", {})

    project.setdefault("study_dir", "results")
    project.setdefault("logs_dir", "logs")
    project.setdefault("archives_dir", "archives")
    project.setdefault("venv_dir", "venv")
    project.setdefault("batch_results_dir", "batch_results")
    project.setdefault("lhs_parameters_file", "lhs_parameters.csv")
    project.setdefault("study_results_file", "study_results.csv")
    project.setdefault("hourly_results_file", "hourly_heating.parquet")
    project.setdefault("dashboard_file", "dashboard.duckdb")
    project.setdefault("worker_timings_file", "worker_timings.csv")
    project.setdefault("worker_batch_timings_file", "worker_batch_timings.csv")
    project.setdefault("worker_timing_summary_file", "worker_timing_summary.csv")

    zone_mapping_path = project.get("zone_mapping_path")
    if zone_mapping_path is None:
        zone_mapping_path = project.get("auto_mapping_path")
    if zone_mapping_path is None:
        zone_mapping_path = "zone_mapping.csv"
    project["zone_mapping_path"] = zone_mapping_path

    if "idf_path" not in inputs:
        raise KeyError("inputs.idf_path is required in project_config.json")
    if "epw_path" not in inputs:
        raise KeyError("inputs.epw_path is required in project_config.json")
    inputs.setdefault("epjson_cache_path", str(Path(inputs["idf_path"]).with_suffix(".epJSON")))

    if "download_url" not in energyplus:
        raise KeyError("energyplus.download_url is required in project_config.json")
    if "install_dir" not in energyplus:
        raise KeyError("energyplus.install_dir is required in project_config.json")

    for phase_name in PHASE_NAMES:
        phase_cfg = slurm.setdefault(phase_name, {})
        phase_cfg.setdefault("job_name", f"eplus_{phase_name}")
        phase_cfg.setdefault("ntasks", 1)
        phase_cfg.setdefault("cpus", 1)
        phase_cfg.setdefault("time", "0-01:00:00")

    runtime.setdefault("enable_timing", True)
    runtime.setdefault("export_dashboard", True)
    runtime.setdefault("hourly_batch_rows", 50000)
    runtime.setdefault("est_sim_sec", 21)
    runtime.setdefault("worker_time_limit_sec", 7200)
    runtime.setdefault("worker_time_margin_sec", 600)
    runtime.setdefault("home_file_reserve", 5000)
    runtime.setdefault("lhs_seed", 42)

    download_url = str(energyplus["download_url"])
    archive_name = energyplus.get("archive_name") or Path(download_url).name

    study_dir = _resolve_path(REPO_ROOT, project["study_dir"])

    resolved_paths = {
        "repo_root": str(REPO_ROOT),
        "study_dir": study_dir,
        "logs_dir": _resolve_path(REPO_ROOT, project["logs_dir"]),
        "archives_dir": _resolve_path(REPO_ROOT, project["archives_dir"]),
        "venv_dir": _resolve_path(REPO_ROOT, project["venv_dir"]),
        "idf_path": _resolve_path(REPO_ROOT, inputs["idf_path"]),
        "epw_path": _resolve_path(REPO_ROOT, inputs["epw_path"]),
        "epjson_cache_path": _resolve_path(REPO_ROOT, inputs["epjson_cache_path"]),
        "energyplus_install_dir": _resolve_path(REPO_ROOT, energyplus["install_dir"]),
        "energyplus_archive_path": _resolve_path(REPO_ROOT, archive_name),
    }
    resolved_paths["batch_results_dir"] = _resolve_within_path(study_dir, project["batch_results_dir"])
    resolved_paths["zone_mapping_path"] = _resolve_within_path(study_dir, project["zone_mapping_path"])
    resolved_paths["lhs_parameters_path"] = _resolve_within_path(study_dir, project["lhs_parameters_file"])
    resolved_paths["study_results_path"] = _resolve_within_path(study_dir, project["study_results_file"])
    resolved_paths["hourly_results_path"] = _resolve_within_path(study_dir, project["hourly_results_file"])
    resolved_paths["dashboard_path"] = _resolve_within_path(study_dir, project["dashboard_file"])
    resolved_paths["dashboard_wal_path"] = f"{resolved_paths['dashboard_path']}.wal"
    resolved_paths["worker_timings_path"] = _resolve_within_path(study_dir, project["worker_timings_file"])
    resolved_paths["worker_batch_timings_path"] = _resolve_within_path(study_dir, project["worker_batch_timings_file"])
    resolved_paths["worker_timing_summary_path"] = _resolve_within_path(study_dir, project["worker_timing_summary_file"])
    resolved_paths["idd_file"] = str(Path(resolved_paths["energyplus_install_dir"]) / "Energy+.idd")
    resolved_paths["converter_path"] = str(Path(resolved_paths["energyplus_install_dir"]) / "ConvertInputFormat")
    resolved_paths["energyplus_exe"] = str(Path(resolved_paths["energyplus_install_dir"]) / "energyplus")
    resolved_paths["state_file"] = str(Path(study_dir) / ".run_state.env")

    config["_meta"] = {
        "config_path": str(config_path),
        "override_path": str(override_path) if override_path.exists() else "",
    }
    config["resolved_paths"] = resolved_paths
    return config


def get_shell_env(config=None):
    cfg = config or load_config()
    paths = cfg["resolved_paths"]
    env = {
        "EPLUS_REPO_ROOT": paths["repo_root"],
        "EPLUS_CONFIG_PATH": cfg["_meta"]["config_path"],
        "EPLUS_CONFIG_OVERRIDE_PATH": cfg["_meta"]["override_path"],
        "EPLUS_STUDY_DIR": paths["study_dir"],
        "EPLUS_LOGS_DIR": paths["logs_dir"],
        "EPLUS_ARCHIVES_DIR": paths["archives_dir"],
        "EPLUS_BATCH_RESULTS_DIR": paths["batch_results_dir"],
        "EPLUS_ZONE_MAPPING_PATH": paths["zone_mapping_path"],
        "EPLUS_AUTO_MAPPING_PATH": paths["zone_mapping_path"],
        "EPLUS_LHS_PARAMETERS_PATH": paths["lhs_parameters_path"],
        "EPLUS_STUDY_RESULTS_PATH": paths["study_results_path"],
        "EPLUS_HOURLY_RESULTS_PATH": paths["hourly_results_path"],
        "EPLUS_DASHBOARD_PATH": paths["dashboard_path"],
        "EPLUS_DASHBOARD_WAL_PATH": paths["dashboard_wal_path"],
        "EPLUS_WORKER_TIMINGS_PATH": paths["worker_timings_path"],
        "EPLUS_WORKER_BATCH_TIMINGS_PATH": paths["worker_batch_timings_path"],
        "EPLUS_WORKER_TIMING_SUMMARY_PATH": paths["worker_timing_summary_path"],
        "EPLUS_VENV_DIR": paths["venv_dir"],
        "EPLUS_INPUT_IDF": paths["idf_path"],
        "EPLUS_INPUT_EPW": paths["epw_path"],
        "EPLUS_INPUT_EPJSON": paths["epjson_cache_path"],
        "EPLUS_EP_INSTALL_DIR": paths["energyplus_install_dir"],
        "EPLUS_EP_ARCHIVE_PATH": paths["energyplus_archive_path"],
        "EPLUS_EP_DOWNLOAD_URL": cfg["energyplus"]["download_url"],
        "EPLUS_EP_VERSION": str(cfg["energyplus"].get("version", "")),
        "EPLUS_EP_IDD": paths["idd_file"],
        "EPLUS_EP_CONVERTER": paths["converter_path"],
        "EPLUS_EP_EXE": paths["energyplus_exe"],
        "EPLUS_STATE_FILE": paths["state_file"],
        "EPLUS_ENABLE_TIMING": _bool_to_env(cfg["runtime"].get("enable_timing", True)),
        "EPLUS_EXPORT_DASHBOARD": _bool_to_env(cfg["runtime"].get("export_dashboard", True)),
        "EPLUS_HOURLY_BATCH_ROWS": str(cfg["runtime"].get("hourly_batch_rows", 50000)),
        "EPLUS_EST_SIM_SEC": str(cfg["runtime"].get("est_sim_sec", 21)),
        "EPLUS_WORKER_TIME_LIMIT_SEC": str(cfg["runtime"].get("worker_time_limit_sec", 7200)),
        "EPLUS_WORKER_TIME_MARGIN_SEC": str(cfg["runtime"].get("worker_time_margin_sec", 600)),
        "EPLUS_HOME_FILE_RESERVE": str(cfg["runtime"].get("home_file_reserve", 5000)),
        "EPLUS_LHS_SEED": str(cfg["runtime"].get("lhs_seed", 42)),
        "EPLUS_SLURM_ACCOUNT": str(cfg["slurm"].get("account", "")),
        "EPLUS_SLURM_PARTITION": str(cfg["slurm"].get("partition", "")),
    }

    for phase_name in PHASE_NAMES:
        phase_cfg = cfg["slurm"][phase_name]
        prefix = f"EPLUS_{phase_name.upper()}"
        env[f"{prefix}_JOB_NAME"] = str(phase_cfg.get("job_name", ""))
        env[f"{prefix}_NTASKS"] = str(phase_cfg.get("ntasks", 1))
        env[f"{prefix}_CPUS"] = str(phase_cfg.get("cpus", 1))
        env[f"{prefix}_TIME"] = str(phase_cfg.get("time", "0-01:00:00"))

    return env


def get_sbatch_args(config, phase_name):
    if phase_name not in PHASE_NAMES:
        raise KeyError(f"Unknown phase '{phase_name}'")

    phase_cfg = config["slurm"][phase_name]
    paths = config["resolved_paths"]
    output_pattern, error_pattern = PHASE_LOG_PATTERNS[phase_name]

    args = []
    account = str(config["slurm"].get("account", "")).strip()
    partition = str(config["slurm"].get("partition", "")).strip()
    if account:
        args.append(f"--account={account}")
    if partition:
        args.append(f"--partition={partition}")
    args.append(f"--ntasks={phase_cfg.get('ntasks', 1)}")
    args.append(f"--cpus-per-task={phase_cfg.get('cpus', 1)}")
    args.append(f"--time={phase_cfg.get('time', '0-01:00:00')}")
    args.append(f"--job-name={phase_cfg.get('job_name', f'eplus_{phase_name}')}")
    args.append(f"--output={Path(paths['logs_dir']) / output_pattern}")
    args.append(f"--error={Path(paths['logs_dir']) / error_pattern}")
    return args


def validate_config(config=None, require_inputs=False, require_energyplus=False,
                    require_venv=False, require_slurm=False):
    cfg = config or load_config()
    paths = cfg["resolved_paths"]
    errors = []
    warnings = []

    for key in ("study_dir", "logs_dir", "archives_dir"):
        parent = Path(paths[key]).parent
        if not parent.exists():
            warnings.append(f"Parent directory does not exist yet for {key}: {parent}")

    if require_inputs:
        if not Path(paths["idf_path"]).is_file():
            errors.append(f"Missing baseline IDF: {paths['idf_path']}")
        if not Path(paths["epw_path"]).is_file():
            errors.append(f"Missing weather file: {paths['epw_path']}")

    if require_energyplus:
        for label, key in (
            ("EnergyPlus install", "energyplus_install_dir"),
            ("EnergyPlus executable", "energyplus_exe"),
            ("ConvertInputFormat", "converter_path"),
            ("Energy+.idd", "idd_file"),
        ):
            path = Path(paths[key])
            if key == "energyplus_install_dir":
                exists = path.is_dir()
            else:
                exists = path.exists()
            if not exists:
                errors.append(f"Missing {label}: {path}")

    if require_venv:
        venv_python = Path(paths["venv_dir"]) / "bin" / "python"
        if not venv_python.exists():
            errors.append(f"Virtual environment is missing or incomplete: {venv_python}")

    if require_slurm:
        for cmd in ("sbatch", "squeue", "sacct"):
            if shutil.which(cmd) is None:
                errors.append(f"Required SLURM command not found on PATH: {cmd}")
        if not str(cfg["slurm"].get("account", "")).strip():
            errors.append("slurm.account is empty in the config")
        if not str(cfg["slurm"].get("partition", "")).strip():
            errors.append("slurm.partition is empty in the config")

    return errors, warnings


def _cmd_shell_env(_args):
    env = get_shell_env(load_config())
    for key, value in env.items():
        print(f"export {key}={shlex.quote(str(value))}")
    return 0


def _cmd_sbatch_args(args):
    for arg in get_sbatch_args(load_config(), args.phase):
        print(arg)
    return 0


def _cmd_validate(args):
    errors, warnings = validate_config(
        load_config(),
        require_inputs=args.require_inputs,
        require_energyplus=args.require_energyplus,
        require_venv=args.require_venv,
        require_slurm=args.require_slurm,
    )

    if not args.quiet:
        if warnings:
            for warning in warnings:
                print(f"WARNING: {warning}")
        if errors:
            for error in errors:
                print(f"ERROR: {error}")
        if not errors:
            print("Validation passed.")

    return 1 if errors else 0


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")

    shell_env = subparsers.add_parser("shell-env", help="Emit shell exports for the resolved config")
    shell_env.set_defaults(func=_cmd_shell_env)

    sbatch_args = subparsers.add_parser("sbatch-args", help="Emit sbatch arguments for a phase")
    sbatch_args.add_argument("phase", choices=PHASE_NAMES)
    sbatch_args.set_defaults(func=_cmd_sbatch_args)

    validate = subparsers.add_parser("validate", help="Validate the configured environment")
    validate.add_argument("--require-inputs", action="store_true")
    validate.add_argument("--require-energyplus", action="store_true")
    validate.add_argument("--require-venv", action="store_true")
    validate.add_argument("--require-slurm", action="store_true")
    validate.add_argument("--quiet", action="store_true")
    validate.set_defaults(func=_cmd_validate)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
