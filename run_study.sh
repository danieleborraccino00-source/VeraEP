#!/usr/bin/env bash
# Orchestrator: submits all phases with SLURM dependencies.
# Usage: bash run_study.sh [N_SIM] [BATCH_SIZE|auto] [MAX_CONCURRENT|auto]
#
#   Phase 1 — generate LHS matrix + zone mapping + cache baseline epJSON
#   Phase 2 — batched workers: generate epJSON + simulate + parse (job array)
#   Phase 3 — consolidate batch outputs into final files (single job)
#
# Default behavior auto-tunes BATCH_SIZE to maximize task-level parallelism while
# respecting SLURM array limits, home inode quota headroom, and worker time limit.
# Set PLAN_ONLY=1 to print the derived launch plan without submitting jobs.

set -euo pipefail
cd "$(dirname "$0")"

eval "$(python3 -m eplus_study.config shell-env)"

STUDY_DIR=${STUDY_DIR:-${EPLUS_STUDY_DIR}}
LOGS_DIR=${LOGS_DIR:-${EPLUS_LOGS_DIR}}
STATE_FILE=${STATE_FILE:-${EPLUS_STATE_FILE}}
BATCH_RESULTS_DIR=${BATCH_RESULTS_DIR:-${EPLUS_BATCH_RESULTS_DIR}}
ZONE_MAPPING_PATH=${ZONE_MAPPING_PATH:-${EPLUS_ZONE_MAPPING_PATH}}
LHS_PARAMETERS_PATH=${LHS_PARAMETERS_PATH:-${EPLUS_LHS_PARAMETERS_PATH}}
STUDY_RESULTS_PATH=${STUDY_RESULTS_PATH:-${EPLUS_STUDY_RESULTS_PATH}}
HOURLY_RESULTS_PATH=${HOURLY_RESULTS_PATH:-${EPLUS_HOURLY_RESULTS_PATH}}
DASHBOARD_PATH=${DASHBOARD_PATH:-${EPLUS_DASHBOARD_PATH}}
DASHBOARD_WAL_PATH=${DASHBOARD_WAL_PATH:-${EPLUS_DASHBOARD_WAL_PATH}}
WORKER_TIMINGS_PATH=${WORKER_TIMINGS_PATH:-${EPLUS_WORKER_TIMINGS_PATH}}
WORKER_BATCH_TIMINGS_PATH=${WORKER_BATCH_TIMINGS_PATH:-${EPLUS_WORKER_BATCH_TIMINGS_PATH}}
WORKER_TIMING_SUMMARY_PATH=${WORKER_TIMING_SUMMARY_PATH:-${EPLUS_WORKER_TIMING_SUMMARY_PATH}}

list_existing_study_outputs() {
    local path
    for path in \
        "${BATCH_RESULTS_DIR}" \
        "${ZONE_MAPPING_PATH}" \
        "${STUDY_RESULTS_PATH}" \
        "${HOURLY_RESULTS_PATH}" \
        "${DASHBOARD_PATH}" \
        "${DASHBOARD_WAL_PATH}" \
        "${LHS_PARAMETERS_PATH}" \
        "${WORKER_TIMINGS_PATH}" \
        "${WORKER_BATCH_TIMINGS_PATH}" \
        "${WORKER_TIMING_SUMMARY_PATH}" \
        "${STATE_FILE}"
    do
        if [[ -e "${path}" ]]; then
            printf '%s\n' "${path}"
        fi
    done
}

confirm_study_reset() {
    local existing_outputs=()
    local path
    local reply

    while IFS= read -r path; do
        [[ -n "${path}" ]] && existing_outputs+=("${path}")
    done < <(list_existing_study_outputs)

    if (( ${#existing_outputs[@]} == 0 )); then
        return 0
    fi

    echo "WARNING: Existing study outputs were found in ${STUDY_DIR}."
    echo "The following paths will be erased before the new study starts:"
    printf '  %s\n' "${existing_outputs[@]}"
    echo ""

    if ! read -r -p "Type y to erase them and proceed, or anything else to stop: " reply; then
        echo "ERROR: Confirmation was not received. Stopping without changes."
        exit 1
    fi

    if [[ "${reply}" != "y" ]]; then
        echo "Stopped. Existing study outputs were left unchanged."
        exit 0
    fi

    rm -rf \
        "${BATCH_RESULTS_DIR}" \
        "${ZONE_MAPPING_PATH}" \
        "${STUDY_RESULTS_PATH}" \
        "${HOURLY_RESULTS_PATH}" \
        "${DASHBOARD_PATH}" \
        "${DASHBOARD_WAL_PATH}" \
        "${LHS_PARAMETERS_PATH}" \
        "${WORKER_TIMINGS_PATH}" \
        "${WORKER_BATCH_TIMINGS_PATH}" \
        "${WORKER_TIMING_SUMMARY_PATH}" \
        "${STATE_FILE}"

    echo "Existing study outputs erased. Proceeding with a fresh run."
}

ceil_div() {
    local num=$1
    local den=$2
    echo $(( (num + den - 1) / den ))
}

detect_max_batches_by_array() {
    local max_array_size
    max_array_size=$(scontrol show config 2>/dev/null | awk -F= '
        /^MaxArraySize/ {
            gsub(/ /, "", $2)
            print $2
            exit
        }
    ')
    if [[ -z "${max_array_size}" ]]; then
        max_array_size=5001
    fi
    echo $(( max_array_size - 1 ))
}

detect_home_file_usage() {
    local quota_output
    quota_output=$(C3SE_quota 2>/dev/null || true)
    if [[ -z "${quota_output}" ]]; then
        return 1
    fi
    local used quota
    used=$(printf '%s\n' "${quota_output}" | awk '
        /^Home:/ { in_home=1; next }
        in_home && /Files used:/ { print $3; exit }
    ')
    quota=$(printf '%s\n' "${quota_output}" | awk '
        /^Home:/ { in_home=1; next }
        in_home && /Files used:/ { print $5; exit }
    ')
    if [[ -z "${used}" || -z "${quota}" ]]; then
        return 1
    fi
    echo "${used} ${quota}"
}

count_existing_log_files() {
    if [[ ! -d "${LOGS_DIR}" ]]; then
        echo 0
        return 0
    fi
    find "${LOGS_DIR}" -type f | wc -l
}

N_SIM=${1:-1000}
REQUESTED_BATCH_SIZE=${2:-auto}
REQUESTED_MAX_CONCURRENT=${3:-auto}

ENABLE_TIMING=${ENABLE_TIMING:-${EPLUS_ENABLE_TIMING}}
PLAN_ONLY=${PLAN_ONLY:-0}
PLAN_OUTPUT=${PLAN_OUTPUT:-human}

EST_SIM_SEC=${EST_SIM_SEC:-${EPLUS_EST_SIM_SEC}}
WORKER_TIME_LIMIT_SEC=${WORKER_TIME_LIMIT_SEC:-${EPLUS_WORKER_TIME_LIMIT_SEC}}
WORKER_TIME_MARGIN_SEC=${WORKER_TIME_MARGIN_SEC:-${EPLUS_WORKER_TIME_MARGIN_SEC}}
HOME_FILE_RESERVE=${HOME_FILE_RESERVE:-${EPLUS_HOME_FILE_RESERVE}}

if (( N_SIM < 1 )); then
    echo "ERROR: N_SIM must be at least 1."
    exit 1
fi

RESULT_FILES_PER_BATCH=2
if (( ENABLE_TIMING != 0 )); then
    RESULT_FILES_PER_BATCH=4
fi
LOG_FILES_PER_BATCH=2
FILES_PER_BATCH=$(( RESULT_FILES_PER_BATCH + LOG_FILES_PER_BATCH ))

MAX_BATCHES_BY_ARRAY=$(detect_max_batches_by_array)
MAX_BATCHES_BY_FILES=${N_SIM}
HOME_FILES_USED="unknown"
HOME_FILES_USED_EFFECTIVE="unknown"
HOME_FILES_QUOTA="unknown"
FREE_FILE_SLOTS="unknown"
RECLAIMABLE_LOG_FILES=0

if read -r HOME_FILES_USED HOME_FILES_QUOTA < <(detect_home_file_usage); then
    RECLAIMABLE_LOG_FILES=$(count_existing_log_files)
    HOME_FILES_USED_EFFECTIVE=$(( HOME_FILES_USED - RECLAIMABLE_LOG_FILES ))
    if (( HOME_FILES_USED_EFFECTIVE < 0 )); then
        HOME_FILES_USED_EFFECTIVE=0
    fi

    FREE_FILE_SLOTS=$(( HOME_FILES_QUOTA - HOME_FILES_USED_EFFECTIVE - HOME_FILE_RESERVE ))
    if (( FREE_FILE_SLOTS < FILES_PER_BATCH )); then
        echo "ERROR: Not enough free file quota headroom for a new study."
        echo "  Home files used: ${HOME_FILES_USED}/${HOME_FILES_QUOTA}"
        if (( RECLAIMABLE_LOG_FILES > 0 )); then
            echo "  Existing logs to be deleted before submission: ${RECLAIMABLE_LOG_FILES}"
        fi
        echo "  Reserve requested: ${HOME_FILE_RESERVE}"
        exit 1
    fi
    MAX_BATCHES_BY_FILES=$(( FREE_FILE_SLOTS / FILES_PER_BATCH ))
    if (( MAX_BATCHES_BY_FILES < 1 )); then
        echo "ERROR: File quota allows zero worker batches with current settings."
        exit 1
    fi
fi

MAX_BATCH_SIZE_BY_TIME=$(( (WORKER_TIME_LIMIT_SEC - WORKER_TIME_MARGIN_SEC) / EST_SIM_SEC ))
if (( MAX_BATCH_SIZE_BY_TIME < 1 )); then
    echo "ERROR: Worker time settings leave no room for even one simulation."
    exit 1
fi

MIN_BATCHES_BY_TIME=$(ceil_div "${N_SIM}" "${MAX_BATCH_SIZE_BY_TIME}")
TARGET_BATCHES=${N_SIM}
if (( TARGET_BATCHES > MAX_BATCHES_BY_ARRAY )); then
    TARGET_BATCHES=${MAX_BATCHES_BY_ARRAY}
fi
if (( TARGET_BATCHES > MAX_BATCHES_BY_FILES )); then
    TARGET_BATCHES=${MAX_BATCHES_BY_FILES}
fi

if (( MIN_BATCHES_BY_TIME > TARGET_BATCHES )); then
    echo "ERROR: Cannot fit ${N_SIM} simulations within current constraints."
    echo "  Minimum batches required by worker timelimit: ${MIN_BATCHES_BY_TIME}"
    echo "  Maximum batches allowed by array limit:      ${MAX_BATCHES_BY_ARRAY}"
    echo "  Maximum batches allowed by file quota:      ${MAX_BATCHES_BY_FILES}"
    echo "Suggestions: reduce N_SIM, disable timing, increase worker timelimit, or free file quota."
    exit 1
fi

if [[ "${REQUESTED_BATCH_SIZE}" == "auto" ]]; then
    N_BATCHES=${TARGET_BATCHES}
    BATCH_SIZE=$(ceil_div "${N_SIM}" "${N_BATCHES}")
    BATCH_MODE="auto"
else
    BATCH_SIZE=${REQUESTED_BATCH_SIZE}
    if (( BATCH_SIZE < 1 )); then
        echo "ERROR: BATCH_SIZE must be at least 1."
        exit 1
    fi
    N_BATCHES=$(ceil_div "${N_SIM}" "${BATCH_SIZE}")
    if (( N_BATCHES > MAX_BATCHES_BY_ARRAY )); then
        echo "ERROR: ${N_BATCHES} batches exceeds SLURM MaxArraySize-derived limit ${MAX_BATCHES_BY_ARRAY}."
        exit 1
    fi
    if (( N_BATCHES > MAX_BATCHES_BY_FILES )); then
        echo "ERROR: ${N_BATCHES} batches exceeds file-quota-derived limit ${MAX_BATCHES_BY_FILES}."
        echo "  Increase BATCH_SIZE, disable timing, or free file quota."
        exit 1
    fi
    if (( BATCH_SIZE > MAX_BATCH_SIZE_BY_TIME )); then
        echo "ERROR: BATCH_SIZE=${BATCH_SIZE} is too large for the current worker time assumption."
        echo "  Maximum recommended batch size: ${MAX_BATCH_SIZE_BY_TIME}"
        exit 1
    fi
    BATCH_MODE="manual"
fi

if [[ "${REQUESTED_MAX_CONCURRENT}" == "auto" ]]; then
    ARRAY_SPEC="1-${N_BATCHES}"
    MAX_CONCURRENT_LABEL="scheduler-managed"
else
    MAX_CONCURRENT=${REQUESTED_MAX_CONCURRENT}
    if [[ "${MAX_CONCURRENT}" == "0" ]]; then
        ARRAY_SPEC="1-${N_BATCHES}"
        MAX_CONCURRENT_LABEL="scheduler-managed"
    else
        if (( MAX_CONCURRENT < 1 )); then
            echo "ERROR: MAX_CONCURRENT must be >= 1, 0, or auto."
            exit 1
        fi
        ARRAY_SPEC="1-${N_BATCHES}%${MAX_CONCURRENT}"
        MAX_CONCURRENT_LABEL=${MAX_CONCURRENT}
    fi
fi

if [[ "${PLAN_OUTPUT}" == "kv" ]]; then
    cat <<EOF
PLAN_N_SIM=${N_SIM}
PLAN_BATCH_MODE=${BATCH_MODE}
PLAN_BATCH_SIZE=${BATCH_SIZE}
PLAN_N_BATCHES=${N_BATCHES}
PLAN_ARRAY_SPEC=${ARRAY_SPEC}
PLAN_MAX_CONCURRENT_LABEL=${MAX_CONCURRENT_LABEL}
PLAN_ENABLE_TIMING=${ENABLE_TIMING}
PLAN_FILES_PER_BATCH=${FILES_PER_BATCH}
PLAN_EST_SIM_SEC=${EST_SIM_SEC}
PLAN_MAX_BATCH_SIZE_BY_TIME=${MAX_BATCH_SIZE_BY_TIME}
PLAN_MAX_BATCHES_BY_ARRAY=${MAX_BATCHES_BY_ARRAY}
PLAN_MAX_BATCHES_BY_FILES=${MAX_BATCHES_BY_FILES}
PLAN_HOME_FILES_USED=${HOME_FILES_USED}
PLAN_HOME_FILES_USED_EFFECTIVE=${HOME_FILES_USED_EFFECTIVE}
PLAN_HOME_FILES_QUOTA=${HOME_FILES_QUOTA}
PLAN_FREE_FILE_SLOTS=${FREE_FILE_SLOTS}
PLAN_HOME_FILE_RESERVE=${HOME_FILE_RESERVE}
PLAN_RECLAIMABLE_LOG_FILES=${RECLAIMABLE_LOG_FILES}
EOF
else
    echo "=== EnergyPlus LHS Study ==="
    echo "  Simulations: $N_SIM"
    echo "  Input path:   epjson"
    echo "  Timing:       $([[ ${ENABLE_TIMING} -ne 0 ]] && echo enabled || echo disabled)"
    echo "  Batch mode:   $BATCH_MODE"
    echo "  Batch size:   $BATCH_SIZE"
    echo "  Batches:      $N_BATCHES"
    echo "  Array spec:   $ARRAY_SPEC"
    echo "  Concurrency:  $MAX_CONCURRENT_LABEL"
    echo "  Files/batch:  $FILES_PER_BATCH"
    echo "  Max batches by array: $MAX_BATCHES_BY_ARRAY"
    if [[ "${HOME_FILES_USED}" != "unknown" ]]; then
        echo "  Home files:   $HOME_FILES_USED / $HOME_FILES_QUOTA"
        if (( RECLAIMABLE_LOG_FILES > 0 )); then
            echo "  Logs to delete before submit: $RECLAIMABLE_LOG_FILES"
            echo "  Effective files for planning: $HOME_FILES_USED_EFFECTIVE / $HOME_FILES_QUOTA"
        fi
        echo "  File reserve: $HOME_FILE_RESERVE"
        echo "  Max batches by files: $MAX_BATCHES_BY_FILES"
    fi
    echo "  Worker time assumption: ~${EST_SIM_SEC}s/sim, max batch size by time ${MAX_BATCH_SIZE_BY_TIME}"
    echo ""
fi

if (( PLAN_ONLY != 0 )); then
    if [[ "${PLAN_OUTPUT}" != "kv" ]]; then
        echo "PLAN_ONLY=1 set; no jobs submitted."
    fi
    exit 0
fi

python3 -m eplus_study.config validate \
    --require-inputs \
    --require-energyplus \
    --require-venv \
    --require-slurm

confirm_study_reset

# Clean old logs to stay within file quota
rm -rf "${LOGS_DIR}"
mkdir -p "${LOGS_DIR}"

mapfile -t GENERATE_SBATCH_ARGS < <(python3 -m eplus_study.config sbatch-args generate)
mapfile -t WORKER_SBATCH_ARGS < <(python3 -m eplus_study.config sbatch-args worker)
mapfile -t CONSOLIDATE_SBATCH_ARGS < <(python3 -m eplus_study.config sbatch-args consolidate)

# Phase 1: Generate LHS matrix
JOB1=$(sbatch "${GENERATE_SBATCH_ARGS[@]}" --parsable --export=ALL,N_SIM=$N_SIM slurm/generate.sh)
echo "Phase 1 (LHS matrix):   Job $JOB1"

# Phase 2: Batched workers — starts after Phase 1
JOB2=$(sbatch --parsable \
    "${WORKER_SBATCH_ARGS[@]}" \
    --dependency=afterok:${JOB1} \
    --array=${ARRAY_SPEC} \
    --export=ALL,BATCH_SIZE=$BATCH_SIZE,ENABLE_TIMING=$ENABLE_TIMING \
    slurm/worker.sh)
echo "Phase 2 (workers):      Job array $JOB2 [1-${N_BATCHES}]"

# Phase 3: Consolidate — starts after all workers finish
JOB3=$(sbatch "${CONSOLIDATE_SBATCH_ARGS[@]}" --parsable --dependency=afterok:${JOB2} slurm/consolidate.sh)
echo "Phase 3 (consolidate):  Job $JOB3"

mkdir -p "${STUDY_DIR}"
cat > "${STATE_FILE}" <<EOF
STATE_VERSION=1
STATE_SUBMITTED_AT=$(date -Is)
STATE_N_SIM=${N_SIM}
STATE_BATCH_MODE=${BATCH_MODE}
STATE_BATCH_SIZE=${BATCH_SIZE}
STATE_N_BATCHES=${N_BATCHES}
STATE_ARRAY_SPEC=${ARRAY_SPEC}
STATE_MAX_CONCURRENT_LABEL=${MAX_CONCURRENT_LABEL}
STATE_ENABLE_TIMING=${ENABLE_TIMING}
STATE_EST_SIM_SEC=${EST_SIM_SEC}
STATE_JOB1=${JOB1}
STATE_JOB2=${JOB2}
STATE_JOB3=${JOB3}
STATE_STUDY_DIR=${STUDY_DIR}
STATE_LOGS_DIR=${LOGS_DIR}
STATE_CONFIG_PATH=${EPLUS_CONFIG_PATH}
EOF

echo ""
echo "Monitor with:  bash monitor.sh --watch 5"
echo "State file:    ${STATE_FILE}"
echo "Results:       ${STUDY_RESULTS_PATH}"
echo "Hourly data:   ${HOURLY_RESULTS_PATH}"
