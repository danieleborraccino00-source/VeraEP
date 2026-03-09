#!/usr/bin/env bash
# Destructively remove local study artifacts and local-only inputs from the workspace.

set -euo pipefail

cd "$(dirname "$0")"
eval "$(python3 -m eplus_study.config shell-env)"

ASSUME_YES=0
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage: bash clean_workspace.sh [--dry-run] [--yes]

This script permanently deletes local-only workspace artifacts including:
  - study outputs and batch data
  - archives, logs, virtual environment, and EnergyPlus install
  - local IDF, EPW, and cached epJSON files
  - legacy/ and miscellaneous runtime artifacts
EOF
}

while (( $# > 0 )); do
    case "$1" in
        --dry-run)
            DRY_RUN=1
            ;;
        --yes)
            ASSUME_YES=1
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

clear_dir_preserve_gitkeep() {
    local dir=$1
    [[ -d "$dir" ]] || return 0
    find "$dir" -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} +
}

IDF_DIR=$(dirname "$EPLUS_INPUT_IDF")
EPW_DIR=$(dirname "$EPLUS_INPUT_EPW")
ZONE_MAPPING_PATH=${EPLUS_ZONE_MAPPING_PATH:-${EPLUS_AUTO_MAPPING_PATH}}
DISPLAY_TARGETS=()

[[ -d "$EPLUS_STUDY_DIR" ]] && DISPLAY_TARGETS+=("contents of $EPLUS_STUDY_DIR")
[[ -d "$IDF_DIR" ]] && DISPLAY_TARGETS+=("contents of $IDF_DIR")
[[ -d "$EPW_DIR" ]] && DISPLAY_TARGETS+=("contents of $EPW_DIR")
[[ -e "$EPLUS_LOGS_DIR" ]] && DISPLAY_TARGETS+=("$EPLUS_LOGS_DIR")
[[ -e "$EPLUS_ARCHIVES_DIR" ]] && DISPLAY_TARGETS+=("$EPLUS_ARCHIVES_DIR")
[[ -e "$EPLUS_VENV_DIR" ]] && DISPLAY_TARGETS+=("$EPLUS_VENV_DIR")
[[ -e "$EPLUS_EP_INSTALL_DIR" ]] && DISPLAY_TARGETS+=("$EPLUS_EP_INSTALL_DIR")
[[ -e "$EPLUS_EP_ARCHIVE_PATH" ]] && DISPLAY_TARGETS+=("$EPLUS_EP_ARCHIVE_PATH")
if [[ -e "$ZONE_MAPPING_PATH" && "$ZONE_MAPPING_PATH" != "$EPLUS_STUDY_DIR"/* ]]; then
    DISPLAY_TARGETS+=("$ZONE_MAPPING_PATH")
fi
[[ -e "$EPLUS_REPO_ROOT/readvars.audit" ]] && DISPLAY_TARGETS+=("$EPLUS_REPO_ROOT/readvars.audit")
[[ -e "$EPLUS_REPO_ROOT/legacy" ]] && DISPLAY_TARGETS+=("$EPLUS_REPO_ROOT/legacy")

if find "$EPLUS_REPO_ROOT" \
    -path "$EPLUS_VENV_DIR" -prune -o \
    -path "$EPLUS_EP_INSTALL_DIR" -prune -o \
    -type d -name '__pycache__' -print -quit 2>/dev/null | grep -q .; then
    DISPLAY_TARGETS+=("__pycache__ directories under $EPLUS_REPO_ROOT")
fi

if (( ${#DISPLAY_TARGETS[@]} == 0 )); then
    echo "No local workspace artifacts were found to remove."
    exit 0
fi

echo "WARNING: This will permanently delete local workspace data."
echo "Targets:"
printf '  %s\n' "${DISPLAY_TARGETS[@]}"
echo ""

if (( DRY_RUN != 0 )); then
    echo "Dry run only. Nothing was deleted."
    exit 0
fi

if (( ASSUME_YES == 0 )); then
    if ! read -r -p "Type y to delete these artifacts, or anything else to stop: " reply; then
        echo "ERROR: Confirmation was not received. Stopping without changes."
        exit 1
    fi
    if [[ "$reply" != "y" ]]; then
        echo "Stopped. No files were deleted."
        exit 0
    fi
fi

clear_dir_preserve_gitkeep "$EPLUS_STUDY_DIR"
clear_dir_preserve_gitkeep "$IDF_DIR"
clear_dir_preserve_gitkeep "$EPW_DIR"

rm -rf \
    "$EPLUS_LOGS_DIR" \
    "$EPLUS_ARCHIVES_DIR" \
    "$EPLUS_VENV_DIR" \
    "$EPLUS_EP_INSTALL_DIR" \
    "$EPLUS_REPO_ROOT/legacy" \
    "$EPLUS_REPO_ROOT"/__pycache__

rm -f \
    "$EPLUS_EP_ARCHIVE_PATH" \
    "$EPLUS_INPUT_EPJSON" \
    "$ZONE_MAPPING_PATH" \
    "$EPLUS_REPO_ROOT/readvars.audit"

find "$EPLUS_REPO_ROOT" -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true

echo "Workspace cleanup complete."