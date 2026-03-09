#!/usr/bin/env bash
# Phase 1 runtime payload. Submit via run_study.sh or sbatch args from eplus_study.config.

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "${SLURM_SUBMIT_DIR:-${ROOT_DIR}}"
eval "$(python3 -m eplus_study.config shell-env)"
source "${EPLUS_VENV_DIR}/bin/activate"

echo "[$(date)] Phase 1: Generating LHS matrix (N_SIM=${N_SIM:-1000})"
python3 -u -m eplus_study.generate_samples
echo "[$(date)] Phase 1 complete"
