#!/usr/bin/env bash
# Prepare a first-run environment from project_config.json and validate it.

set -euo pipefail

cd "$(dirname "$0")"
eval "$(python3 -m eplus_study.config shell-env)"

need_cmd() {
    local cmd=$1
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: Required command not found: $cmd"
        exit 1
    fi
}

download_energyplus() {
    if [[ -d "$EPLUS_EP_INSTALL_DIR" ]]; then
        echo "EnergyPlus already present: $EPLUS_EP_INSTALL_DIR"
        return 0
    fi

    echo "EnergyPlus install not found. Preparing download..."
    if [[ ! -f "$EPLUS_EP_ARCHIVE_PATH" ]]; then
        if command -v wget >/dev/null 2>&1; then
            wget -O "$EPLUS_EP_ARCHIVE_PATH" "$EPLUS_EP_DOWNLOAD_URL"
        elif command -v curl >/dev/null 2>&1; then
            curl -L "$EPLUS_EP_DOWNLOAD_URL" -o "$EPLUS_EP_ARCHIVE_PATH"
        else
            echo "ERROR: Neither wget nor curl is available for downloading EnergyPlus."
            exit 1
        fi
    else
        echo "Using existing EnergyPlus archive: $EPLUS_EP_ARCHIVE_PATH"
    fi

    echo "Extracting EnergyPlus..."
    tar -xzf "$EPLUS_EP_ARCHIVE_PATH"
}

mkdir -p \
    "$EPLUS_STUDY_DIR" \
    "$EPLUS_LOGS_DIR" \
    "$EPLUS_ARCHIVES_DIR" \
    "$(dirname "$EPLUS_INPUT_IDF")" \
    "$(dirname "$EPLUS_INPUT_EPW")"

need_cmd python3
need_cmd tar

if [[ ! -d "$EPLUS_VENV_DIR" ]]; then
    echo "Creating virtual environment: $EPLUS_VENV_DIR"
    python3 -m venv "$EPLUS_VENV_DIR"
else
    echo "Virtual environment already present: $EPLUS_VENV_DIR"
fi

source "$EPLUS_VENV_DIR/bin/activate"

echo "Installing Python requirements..."
python -m pip install -r requirements.txt

download_energyplus

echo "Validating EnergyPlus, venv, and SLURM tooling..."
python3 -m eplus_study.config validate --require-energyplus --require-venv --require-slurm

if [[ ! -f "$EPLUS_INPUT_IDF" || ! -f "$EPLUS_INPUT_EPW" ]]; then
    echo ""
    echo "Environment setup is almost complete, but the study inputs are missing."
    if [[ ! -f "$EPLUS_INPUT_IDF" ]]; then
        echo "  Missing IDF: $EPLUS_INPUT_IDF"
    fi
    if [[ ! -f "$EPLUS_INPUT_EPW" ]]; then
        echo "  Missing EPW: $EPLUS_INPUT_EPW"
    fi
    echo ""
    echo "Add those files, then rerun: bash setup_environment.sh"
    exit 1
fi

echo "Generating or refreshing the cached baseline epJSON..."
python - <<'PY'
from eplus_study.config import load_config
from eplus_study.epjson_parametrics import ensure_baseline_epjson
from eppy.modeleditor import IDF

cfg = load_config()
paths = cfg["resolved_paths"]
IDF.setiddname(paths["idd_file"])
ensure_baseline_epjson(
    paths["idf_path"],
    paths["epjson_cache_path"],
    paths["converter_path"],
)
print(f"Baseline epJSON ready: {paths['epjson_cache_path']}")
PY

echo "Running final validation..."
python3 -m eplus_study.config validate \
    --require-inputs \
    --require-energyplus \
    --require-venv \
    --require-slurm

echo ""
echo "Environment ready. You can start with:"
echo "  PLAN_ONLY=1 bash run_study.sh 5000"
echo "  bash run_study.sh 5000"
echo "  bash monitor.sh --watch 5"