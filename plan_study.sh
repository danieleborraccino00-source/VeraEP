#!/usr/bin/env bash
# Print the auto-batching plan and wall-clock scenarios without submitting jobs.
# Usage: bash plan_study.sh [N_SIM] [BATCH_SIZE|auto] [MAX_CONCURRENT|auto]

set -euo pipefail
cd "$(dirname "$0")"

eval "$(python3 -m eplus_study.config shell-env)"

PLAN_KV=$(PLAN_ONLY=1 PLAN_OUTPUT=kv bash run_study.sh "$@")
while IFS='=' read -r key value; do
    case "${key}" in
        PLAN_*) printf -v "${key}" '%s' "${value}" ;;
    esac
done <<< "${PLAN_KV}"

TIMING_SUMMARY_PATH=${TIMING_SUMMARY_PATH:-${EPLUS_WORKER_TIMING_SUMMARY_PATH}}
EST_SIM_TOTAL_SEC=${PLAN_EST_SIM_SEC}
ESTIMATE_SOURCE="fallback EST_SIM_SEC from run_study.sh"

if [[ -f "${TIMING_SUMMARY_PATH}" ]]; then
    read -r EST_SIM_TOTAL_SEC ESTIMATE_SOURCE < <(
        python3 - "${TIMING_SUMMARY_PATH}" <<'PY'
import csv
import sys

path = sys.argv[1]
with open(path, newline='', encoding='utf-8') as handle:
    row = next(csv.DictReader(handle))
print(row['median_sim_total_sec'], path)
PY
    )
fi

export PLAN_N_SIM PLAN_BATCH_SIZE PLAN_N_BATCHES PLAN_MAX_CONCURRENT_LABEL
export EST_SIM_TOTAL_SEC ESTIMATE_SOURCE

python3 <<'PY'
import math
import os

n_sim = int(os.environ['PLAN_N_SIM'])
batch_size = int(os.environ['PLAN_BATCH_SIZE'])
n_batches = int(os.environ['PLAN_N_BATCHES'])
concurrency_label = os.environ['PLAN_MAX_CONCURRENT_LABEL']
est_sim_total = float(os.environ['EST_SIM_TOTAL_SEC'])
estimate_source = os.environ['ESTIMATE_SOURCE']

batch_runtime = batch_size * est_sim_total

scenario_candidates = [1, 10, 25, 50, 100, 200, 500, 1000]
scenarios = []
for candidate in scenario_candidates:
    if candidate <= n_batches and candidate not in scenarios:
        scenarios.append(candidate)
if n_batches not in scenarios:
    scenarios.append(n_batches)

if concurrency_label.isdigit():
    requested = int(concurrency_label)
    if requested <= n_batches and requested not in scenarios:
        scenarios.append(requested)
    scenarios = sorted(scenarios)

print('=== Study Plan Report ===')
print(f'  Simulations: {n_sim}')
print(f'  Batch size:  {batch_size}')
print(f'  Batches:     {n_batches}')
print(f'  Array spec:  1-{n_batches}' + ('' if concurrency_label == 'scheduler-managed' else f'%{concurrency_label}'))
print(f'  Concurrency: {concurrency_label}')
print(f'  Estimate source: {estimate_source}')
print(f'  Estimated sim wall time: {est_sim_total:.6f} s')
print(f'  Estimated batch wall time: {batch_runtime:.2f} s ({batch_runtime / 60.0:.2f} min)')
print(f'  Best-case lower bound if all batches run at once: {batch_runtime / 60.0:.2f} min')
print('')
print('Worker Wall Scenarios')
for concurrency in scenarios:
    waves = math.ceil(n_batches / concurrency)
    worker_wall = waves * batch_runtime
    print(
        f'  concurrency={concurrency:>4}  waves={waves:>4}  '
        f'worker_wall={worker_wall / 60.0:>8.2f} min  '
        f'worker_wall={worker_wall / 3600.0:>6.2f} h'
    )

print('')
print('Notes')
print('  - These are worker-phase estimates from measured per-simulation wall time.')
print('  - Add phase-1 generation, phase-3 consolidation, scheduler queueing, and fair-share effects on top.')
print('  - If concurrency is scheduler-managed, actual wall time depends on how many array tasks Vera starts concurrently.')
PY