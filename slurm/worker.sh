#!/usr/bin/env bash
# Phase 2 runtime payload. Submit via run_study.sh or sbatch args from eplus_study.config.

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "${SLURM_SUBMIT_DIR:-${ROOT_DIR}}"
eval "$(python3 -m eplus_study.config shell-env)"
source "${EPLUS_VENV_DIR}/bin/activate"

echo "[$(date)] Worker batch ${SLURM_ARRAY_TASK_ID} starting"
python3 -u -m eplus_study.simulate_batch
echo "[$(date)] Worker batch ${SLURM_ARRAY_TASK_ID} complete"
