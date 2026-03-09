#!/usr/bin/env bash
# Live progress monitor for the EnergyPlus LHS study.
# Usage:  bash monitor.sh            (one-shot)
#         bash monitor.sh --watch 5  (refresh every 5s)

set -euo pipefail

cd "$(dirname "$0")"
eval "$(python3 -m eplus_study.config shell-env)"
STUDY=${STUDY:-${EPLUS_STUDY_DIR}}
LOGS_DIR=${LOGS_DIR:-${EPLUS_LOGS_DIR}}
STATE_FILE=${STATE_FILE:-${EPLUS_STATE_FILE}}
BATCH_DIR=${BATCH_DIR:-${EPLUS_BATCH_RESULTS_DIR}}
LHS_PARAMETERS_PATH=${LHS_PARAMETERS_PATH:-${EPLUS_LHS_PARAMETERS_PATH}}
STUDY_RESULTS_PATH=${STUDY_RESULTS_PATH:-${EPLUS_STUDY_RESULTS_PATH}}
HOURLY_RESULTS_PATH=${HOURLY_RESULTS_PATH:-${EPLUS_HOURLY_RESULTS_PATH}}
DASHBOARD_PATH=${DASHBOARD_PATH:-${EPLUS_DASHBOARD_PATH}}
DASHBOARD_WAL_PATH=${DASHBOARD_WAL_PATH:-${EPLUS_DASHBOARD_WAL_PATH}}
BAR_LEN=40
WATCH_MODE=0
INTERVAL=5

usage() {
    cat <<'EOF'
Usage: bash monitor.sh [--watch [seconds]]

Examples:
  bash monitor.sh
  bash monitor.sh --watch
  bash monitor.sh --watch 5
EOF
}

ceil_div() {
    local num=$1
    local den=$2
    echo $(( (num + den - 1) / den ))
}

repeat_char() {
    local count=$1
    local char=$2
    if (( count <= 0 )); then
        return 0
    fi
    printf '%*s' "$count" '' | tr ' ' "$char"
}

draw_bar() {
    local cur=$1
    local tot=$2
    local label=$3
    local pct=0
    local filled=0
    local empty=$BAR_LEN

    if (( tot > 0 )); then
        pct=$(( cur * 100 / tot ))
        filled=$(( cur * BAR_LEN / tot ))
        empty=$(( BAR_LEN - filled ))
    fi

    printf "  %-12s [%s%s] %6d/%d (%3d%%)\n" "$label" \
        "$(repeat_char "$filled" '#')" \
        "$(repeat_char "$empty" '-')" \
        "$cur" "$tot" "$pct"
}

job_state() {
    local job_id=$1
    local state=""

    [[ -n "$job_id" ]] || {
        echo "untracked"
        return 0
    }

    state=$(squeue -h -j "$job_id" -o '%T' 2>/dev/null | head -1 || true)
    if [[ -n "$state" ]]; then
        echo "$state"
        return 0
    fi

    state=$(sacct -n -X -j "$job_id" --format=State 2>/dev/null | awk 'NF {print $1; exit}')
    if [[ -n "$state" ]]; then
        echo "$state"
    else
        echo "unknown"
    fi
}

job_reason() {
    local job_id=$1
    local reason=""

    [[ -n "$job_id" ]] || {
        echo ""
        return 0
    }

    reason=$(squeue -h -j "$job_id" -o '%R' 2>/dev/null | head -1 || true)
    echo "$reason"
}

count_matching_files() {
    local pattern=$1
    if [[ ! -d "$BATCH_DIR" ]]; then
        echo 0
        return 0
    fi
    find "$BATCH_DIR" -maxdepth 1 -type f -name "$pattern" | wc -l
}

parse_args() {
    while (( $# > 0 )); do
        case "$1" in
            --watch)
                WATCH_MODE=1
                if (( $# > 1 )) && [[ "$2" =~ ^[0-9]+$ ]]; then
                    INTERVAL=$2
                    shift
                fi
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                echo "ERROR: Unknown argument '$1'."
                usage
                exit 1
                ;;
        esac
        shift
    done

    if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || (( INTERVAL < 1 )); then
        echo "ERROR: Watch interval must be a positive integer."
        exit 1
    fi
}

load_state() {
    STATE_VERSION=""
    STATE_SUBMITTED_AT=""
    STATE_N_SIM=""
    STATE_BATCH_MODE=""
    STATE_BATCH_SIZE=""
    STATE_N_BATCHES=""
    STATE_ARRAY_SPEC=""
    STATE_MAX_CONCURRENT_LABEL=""
    STATE_ENABLE_TIMING=""
    STATE_EST_SIM_SEC=""
    STATE_JOB1=""
    STATE_JOB2=""
    STATE_JOB3=""
    STATE_LOGS_DIR=""

    if [[ -f "$STATE_FILE" ]]; then
        # shellcheck disable=SC1090
        source "$STATE_FILE"
    fi
}

render_once() {
    load_state

    local total=0
    local final_summary=0
    local final_hourly=0
    local final_dashboard=0
    local dashboard_wal=0
    local n_summary=0
    local n_hourly=0
    local sims_done=0
    local batches_done_label="0"
    local batch_files_line=""
    local batch_size=0
    local n_batches=0
    local phase1_state="untracked"
    local phase2_state="untracked"
    local phase3_state="untracked"
    local phase3_reason=""
    local running=0
    local configuring=0
    local pending=0
    local completing=0
    local active_total=0
    local extra_states=""
    local logs_dir="${LOGS_DIR}"

    if [[ -n "$STATE_LOGS_DIR" ]]; then
        logs_dir=$STATE_LOGS_DIR
    fi

    if [[ -n "$STATE_N_SIM" ]]; then
        total=$STATE_N_SIM
    elif [[ -f "$LHS_PARAMETERS_PATH" ]]; then
        total=$(( $(wc -l < "$LHS_PARAMETERS_PATH") - 1 ))
    elif [[ -f "$STUDY_RESULTS_PATH" ]]; then
        total=$(( $(wc -l < "$STUDY_RESULTS_PATH") - 1 ))
    fi

    if [[ -n "$STATE_BATCH_SIZE" ]]; then
        batch_size=$STATE_BATCH_SIZE
    fi

    if [[ -n "$STATE_N_BATCHES" ]]; then
        n_batches=$STATE_N_BATCHES
    elif (( total > 0 && batch_size > 0 )); then
        n_batches=$(ceil_div "$total" "$batch_size")
    fi

    [[ -f "$STUDY_RESULTS_PATH" ]] && final_summary=1
    [[ -f "$HOURLY_RESULTS_PATH" ]] && final_hourly=1
    [[ -f "$DASHBOARD_PATH" ]] && final_dashboard=1
    [[ -f "$DASHBOARD_WAL_PATH" ]] && dashboard_wal=1

    n_summary=$(count_matching_files 'summary_*.csv')
    n_hourly=$(count_matching_files 'hourly_*.parquet')

    if (( final_summary == 1 && final_hourly == 1 && n_summary == 0 && total > 0 )); then
        sims_done=$total
        if (( n_batches > 0 )); then
            batches_done_label="${n_batches}/${n_batches}"
        else
            batches_done_label="complete"
        fi
        batch_files_line="cleaned up after consolidation"
    elif (( n_summary > 0 )); then
        if (( batch_size == 0 )); then
            local first_summary
            first_summary=$(find "$BATCH_DIR" -maxdepth 1 -type f -name 'summary_*.csv' | sort | head -1 || true)
            if [[ -n "$first_summary" ]]; then
                batch_size=$(( $(wc -l < "$first_summary") - 1 ))
            fi
        fi

        if (( batch_size > 0 )); then
            sims_done=$(( n_summary * batch_size ))
            if (( total > 0 && sims_done > total )); then
                sims_done=$total
            fi
        fi

        if (( n_batches > 0 )); then
            batches_done_label="${n_summary}/${n_batches}"
        else
            batches_done_label="${n_summary}"
        fi
        batch_files_line="${batches_done_label} summaries, ${n_hourly} hourly"
    elif (( n_batches > 0 )); then
        batches_done_label="0/${n_batches}"
        batch_files_line="${batches_done_label} summaries, ${n_hourly} hourly"
    else
        batch_files_line="0 summaries, ${n_hourly} hourly"
    fi

    if [[ -n "$STATE_JOB2" ]]; then
        while read -r count state; do
            [[ -n "$count" && -n "$state" ]] || continue
            case "$state" in
                RUNNING) running=$count ;;
                CONFIGURING) configuring=$count ;;
                PENDING) pending=$count ;;
                COMPLETING) completing=$count ;;
                *)
                    if [[ -n "$extra_states" ]]; then
                        extra_states+=", "
                    fi
                    extra_states+="${state,,} ${count}"
                    ;;
            esac
        done < <(squeue -h -r -j "$STATE_JOB2" -o '%T' 2>/dev/null | sort | uniq -c | awk '{print $1, $2}')
    fi
    active_total=$(( running + configuring + pending + completing ))

    phase1_state=$(job_state "$STATE_JOB1")
    phase3_state=$(job_state "$STATE_JOB3")
    phase3_reason=$(job_reason "$STATE_JOB3")

    if (( sims_done >= total && total > 0 && active_total == 0 )); then
        phase2_state="COMPLETED"
    elif (( active_total > 0 )); then
        if (( running > 0 || configuring > 0 || completing > 0 )); then
            phase2_state="RUNNING"
        else
            phase2_state="PENDING"
        fi
    elif [[ -n "$STATE_JOB2" ]]; then
        phase2_state=$(job_state "$STATE_JOB2")
    elif (( sims_done > 0 )); then
        phase2_state="RUNNING"
    fi

    if [[ "$phase3_state" == "unknown" && $final_summary -eq 1 && $final_hourly -eq 1 ]]; then
        phase3_state="COMPLETED"
    fi
    if [[ "$phase3_state" == "untracked" && $final_summary -eq 1 && $final_hourly -eq 1 ]]; then
        phase3_state="COMPLETED"
    fi
    if [[ "$phase1_state" == "unknown" || "$phase1_state" == "untracked" ]]; then
        if [[ -f "$LHS_PARAMETERS_PATH" || $final_summary -eq 1 || $final_hourly -eq 1 ]]; then
            phase1_state="COMPLETED"
        fi
    fi

    echo "═══════════════════════════════════════════════════════════"
    echo "  EnergyPlus LHS Study   $(date '+%Y-%m-%d %H:%M:%S')"
    echo "═══════════════════════════════════════════════════════════"
    if [[ -n "$STATE_SUBMITTED_AT" ]]; then
        echo "  Submitted:     $STATE_SUBMITTED_AT"
    fi
    echo "  Study path:    $STUDY"
    echo "  Simulations:   $total"
    if (( batch_size > 0 )); then
        echo "  Batch size:    $batch_size"
    fi
    if (( n_batches > 0 )); then
        echo "  Batches:       $n_batches"
    fi
    draw_bar "$sims_done" "$total" "Workers"
    echo "  Batch files:   $batch_files_line"
    echo "  Outputs:       summary=$([[ $final_summary -eq 1 ]] && echo yes || echo no)  hourly=$([[ $final_hourly -eq 1 ]] && echo yes || echo no)  dashboard=$(if [[ $dashboard_wal -eq 1 ]]; then echo writing; elif [[ $final_dashboard -eq 1 ]]; then echo yes; else echo no; fi)"
    echo ""
    echo "  Phase 1:       ${phase1_state}  ${STATE_JOB1:+(job ${STATE_JOB1})}"

    if [[ "$phase2_state" == "RUNNING" || "$phase2_state" == "PENDING" ]]; then
        local worker_detail="done ${sims_done}/${total}"
        if (( running > 0 )); then
            worker_detail+=" | running ${running}"
        fi
        if (( configuring > 0 )); then
            worker_detail+=" | configuring ${configuring}"
        fi
        if (( completing > 0 )); then
            worker_detail+=" | completing ${completing}"
        fi
        if (( pending > 0 )); then
            worker_detail+=" | pending ${pending}"
        fi
        if [[ -n "$extra_states" ]]; then
            worker_detail+=" | ${extra_states}"
        fi
        echo "  Phase 2:       ${phase2_state}  ${STATE_JOB2:+(job ${STATE_JOB2})}  ${worker_detail}"
    else
        echo "  Phase 2:       ${phase2_state}  ${STATE_JOB2:+(job ${STATE_JOB2})}  done ${sims_done}/${total}"
    fi

    if [[ "$phase3_state" == "PENDING" && -n "$phase3_reason" ]]; then
        echo "  Phase 3:       ${phase3_state}  ${STATE_JOB3:+(job ${STATE_JOB3})}  reason: ${phase3_reason}"
    elif [[ "$phase3_state" == "RUNNING" && $dashboard_wal -eq 1 ]]; then
        echo "  Phase 3:       ${phase3_state}  ${STATE_JOB3:+(job ${STATE_JOB3})}  writing dashboard bundle"
    else
        echo "  Phase 3:       ${phase3_state}  ${STATE_JOB3:+(job ${STATE_JOB3})}"
    fi

    if [[ -n "$STATE_JOB1$STATE_JOB2$STATE_JOB3" ]]; then
        echo ""
        echo "  Logs:          ${logs_dir}/generate-${STATE_JOB1}.out"
        echo "                 ${logs_dir}/worker-${STATE_JOB2}_*.out"
        echo "                 ${logs_dir}/consolidate-${STATE_JOB3}.out"
    fi
    echo "═══════════════════════════════════════════════════════════"
}

parse_args "$@"

if (( WATCH_MODE == 0 )); then
    render_once
    exit 0
fi

while true; do
    clear
    render_once
    echo ""
    echo "  Refreshing every ${INTERVAL}s. Press Ctrl-C to stop."
    sleep "$INTERVAL"
done
